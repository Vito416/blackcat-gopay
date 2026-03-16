<?php

declare(strict_types=1);

namespace BlackCat\GoPay;

/**
 * - PSR-3 Logger dependency.
 * - Uses FileCache for OAuth token caching.
 * - Falls back to direct HTTP if official SDK isn't available or fails.
 * - Sanitizes payloads before logging.
 */
use BlackCat\Core\Cache\LockingCacheInterface;
use Psr\Log\LoggerInterface;
use Psr\SimpleCache\CacheInterface;

final class GoPayTokenException extends \RuntimeException {}
final class GoPayHttpException extends \RuntimeException {}
final class GoPayPaymentException extends \RuntimeException {}

final class GoPaySdkWrapper implements PaymentGatewayInterface
{
    /** @var array<string,mixed> */
    private array $cfg;
    private ?object $client = null;
    private CacheInterface $cache;
    private LoggerInterface $logger;
    private string $cacheKey;
    private const PERMANENT_TOKEN_ERRORS = [
        'invalid_client',
        'invalid_grant',
        'unauthorized_client',
        'invalid_request',
        'unsupported_grant_type',
        'invalid_scope',
    ];

    /**
     * @param array<string,mixed> $cfg
     */
    public function __construct(array $cfg, LoggerInterface $logger, CacheInterface $cache)
    {
        $this->cfg = $cfg;
        $this->logger = $logger;
        $this->cache = $cache;
        $this->cacheKey = 'gopay_oauth_token_' . substr(hash('sha256', ($this->cfg['clientId'] ?? '') . '|' . ($this->cfg['gatewayUrl'] ?? '')), 0, 32);

        // basic config validation
        $required = ['gatewayUrl', 'clientId', 'clientSecret', 'goid', 'scope'];
        $missing = [];
        foreach ($required as $k) {
            if (empty($this->cfg[$k])) {
                $missing[] = $k;
            }
        }
        if (!empty($missing)) {
            throw new \InvalidArgumentException('GoPay config missing keys: ' . implode(',', $missing));
        }

        // prefer robust SDK init — wrap in try/catch instead of relying on class_exists
        try {
            if (class_exists(\GoPay\Api::class, true)) {
                $this->client = \GoPay\Api::payments([
                    'goid' => $this->cfg['goid'],
                    'clientId' => $this->cfg['clientId'],
                    'clientSecret' => $this->cfg['clientSecret'],
                    'gatewayUrl' => $this->cfg['gatewayUrl'],
                    'language' => $this->cfg['language'] ?? 'EN',
                    'scope' => $this->cfg['scope'],
                ]);
            }
        } catch (\Throwable $e) {
            $this->client = null;
            $this->logSafe('warning', 'GoPay SDK init failed, falling back to HTTP', ['exception' => $e]);
        }
    }

    /**
     * Safe JSON encode helper (throws on error).
     */
    private function safeJsonEncode(mixed $v): string
    {
        $s = json_encode($v);
        if ($s === false) {
            $msg = json_last_error_msg();
            $ex = new \RuntimeException('JSON encode failed: ' . $msg);
            $this->logSafe('error', 'JSON encode failed', ['phase' => 'json_encode', 'exception' => $ex->getMessage()]);
            throw $ex;
        }
        return $s;
    }

    /**
     * Build HTTP header array from assoc map to avoid fragile numeric indices.
     * @param array<string,mixed> $assoc e.g. ['Authorization'=>'Bearer x', 'Content-Type'=>'application/json']
     * @return array<int,string> ['Key: Value', ...]
     */
    private function buildHeaders(array $assoc): array
    {
        $out = [];
        foreach ($assoc as $k => $v) {
            if ($v === null) continue;
            $key = trim((string)$k);
            if ($key === '') continue;
            if (is_array($v) || is_object($v)) {
                $v = json_encode($v);
            }
            $val = trim((string)$v);
            $out[] = $key . ': ' . $val;
        }
        return $out;
    }

    /**
     * Get OAuth token (cached).
     *
     * @return string
     * @throws \RuntimeException
     */
    public function getToken(): string
    {
        // fast path
        $tokenData = null;
        try {
            $tokenData = $this->cache->get($this->cacheKey);
        } catch (\Throwable $_) {
            $tokenData = null;
        }
        if (is_array($tokenData) && isset($tokenData['token'], $tokenData['expires_at']) && $tokenData['expires_at'] > time()) {
            return (string)$tokenData['token'];
        }

        $lockKey = 'gopay_token_lock_' . substr(hash('sha256', $this->cacheKey), 0, 12);
        $fp = null;
        $lockToken = null;
        $haveCacheLock = false;

        // 1) try cache-provided lock API
        if ($this->cache instanceof LockingCacheInterface) {
            try {
                $lockToken = $this->cache->acquireLock($lockKey, 10);
                $haveCacheLock = $lockToken !== null;
            } catch (\Throwable $_) {
                $haveCacheLock = false;
                $lockToken = null;
            }
        }

        // 2) fallback to file lock if cache lock not available
        $tempLockPath = sys_get_temp_dir() . '/gopay_token_lock_' . substr(hash('sha256', $this->cacheKey), 0, 12);
        if (!$haveCacheLock) {
            $fp = @fopen($tempLockPath, 'c');
            if (is_resource($fp)) {
                $locked = @flock($fp, LOCK_EX);
                if (!$locked) {
                    @fclose($fp);
                    $fp = null;
                }
            }
        }

        try {
            // retry: 3 attempts with exponential backoff for transient failures
            $attempts = 3;
            $backoffMs = 200;
            $lastEx = new GoPayTokenException('Unknown error obtaining token');

            for ($i = 0; $i < $attempts; $i++) {
                // Check cache again (someone else might have populated it while we waited for lock)
                try {
                    $tokenData = $this->cache->get($this->cacheKey);
                    if (is_array($tokenData) && isset($tokenData['token'], $tokenData['expires_at']) && $tokenData['expires_at'] > time()) {
                        return (string)$tokenData['token'];
                    }
                } catch (\Throwable $_) {
                }

                try {
                    // request token
                    $tokenUrl = rtrim((string)$this->cfg['gatewayUrl'], '/') . '/api/oauth2/token';
                    $basic = base64_encode($this->cfg['clientId'] . ':' . $this->cfg['clientSecret']);
                    $body = http_build_query([
                        'grant_type' => 'client_credentials',
                        'scope' => $this->cfg['scope'],
                    ]);

                    $reqId = $this->headerId();
                    $assocHeaders = [
                        'Authorization' => 'Basic ' . $basic,
                        'Content-Type' => 'application/x-www-form-urlencoded',
                        'Accept' => 'application/json',
                        'User-Agent' => 'BlackCat/GoPaySdkWrapper/1.1',
                        'Expect' => '',
                        'X-Request-Id' => $reqId,
                    ];
                    $headers = $this->buildHeaders($assocHeaders);

                    $this->logSafe('info', 'Requesting GoPay OAuth token', [
                        'url' => $tokenUrl,
                        'headers' => $this->sanitizeHeadersForLog($assocHeaders),
                    ]);

                    $resp = $this->doRequest('POST', $tokenUrl, $headers, $body, [
                        'expect_json' => true,
                        'raw' => true,
                        'timeout' => 15,
                    ], $assocHeaders);

                    $httpCode = (int)($resp['http_code'] ?? 0);
                    $decoded = $resp['json'] ?? null;
                    $raw = (string)($resp['body'] ?? '');

                    if ($httpCode >= 200 && $httpCode < 300) {
                        if (!is_array($decoded) || empty($decoded['access_token'])) {
                            throw new GoPayTokenException('Missing access_token in token response');
                        }

                        $token = (string)$decoded['access_token'];
                        $expiresIn = isset($decoded['expires_in']) ? (int)$decoded['expires_in'] : 600;
                        $expiresAt = time() + max(30, $expiresIn - 30); // 30s safety margin

                        // cache token
                        try {
                            $this->cache->set($this->cacheKey, ['token' => $token, 'expires_at' => $expiresAt], max(30, $expiresIn - 20));
                        } catch (\Throwable $_) {
                        }

                        return $token;
                    }

                    // 4xx: treat as permanent error in most cases
                    if ($httpCode >= 400 && $httpCode < 500) {
                        $err = is_array($decoded) ? ($decoded['error'] ?? '') : '';
                        $errDesc = is_array($decoded) ? ($decoded['error_description'] ?? '') : '';

                        if ($err && in_array($err, self::PERMANENT_TOKEN_ERRORS, true)) {
                            $msg = $errDesc ?: json_encode($decoded);
                            $ex = new GoPayTokenException("Permanent token error {$httpCode}: {$err} - {$msg}");
                            $this->logSafe('critical', 'Permanent OAuth token error', [
                                'phase' => 'getToken',
                                'http_code' => $httpCode,
                                'error' => $err,
                                'exception' => $ex->getMessage(),
                            ]);
                            throw $ex;
                        }

                        // If we don't know the error code, still don't brute-force retry too much.
                        // Treat other 4xx as permanent to avoid useless retries.
                        $msg = is_array($decoded) ? ($decoded['error_description'] ?? json_encode($decoded)) : $raw;
                        $ex = new GoPayTokenException("GoPay token endpoint returned HTTP {$httpCode}: {$msg}");
                        $this->logSafe('error', 'GoPay token endpoint returned 4xx', [
                            'phase' => 'getToken',
                            'http_code' => $httpCode,
                            'exception' => $ex->getMessage(),
                        ]);
                        throw $ex;
                    }

                    // For 5xx or unexpected status codes -> throw to outer catch and retry (transient)
                    $msg = is_array($decoded) ? ($decoded['error_description'] ?? json_encode($decoded)) : $raw;
                    throw new GoPayTokenException("GoPay token endpoint returned HTTP {$httpCode}: {$msg}");
                } catch (\Throwable $e) {
                    $lastEx = $e;
                    $this->logSafe('warning', 'getToken attempt failed', ['attempt' => $i + 1, 'exception' => $e]);
                    // exponential backoff for transient failures (but don't sleep after last attempt)
                    if ($i < $attempts - 1) {
                        $backoffMs = min($backoffMs * 2, 2000);
                        usleep(($backoffMs + random_int(0, 250)) * 1000);
                    }
                }
            }

            $ex = $lastEx;
            $this->logSafe('error', 'Failed to obtain GoPay OAuth token after retries', ['phase' => 'getToken', 'exception' => $ex->getMessage()]);
            throw new GoPayTokenException('Failed to obtain GoPay OAuth token: ' . $ex->getMessage());
        } finally {
            // release file lock
            if (isset($fp) && is_resource($fp)) {
                @fflush($fp);
                @flock($fp, LOCK_UN);
                @fclose($fp);
            }
            // release cache lock if used
            if ($lockToken !== null && $this->cache instanceof LockingCacheInterface) {
                try {
                    $this->cache->releaseLock($lockKey, $lockToken);
                } catch (\Throwable $_) {
                }
            }
        }
    }

    /**
     * Create payment, return assoc array (decoded JSON).
     *
     * @param array<string,mixed> $payload
     * @return array<string,mixed>
     * @throws \RuntimeException
     */
    public function createPayment(array $payload): array
    {
        // ensure target.type and goid present
        $goidVal = (string)$this->cfg['goid'];
        if (empty($payload['target'])) {
            $payload['target'] = ['type' => 'ACCOUNT', 'goid' => $goidVal];
        } else {
            $payload['target']['type'] = $payload['target']['type'] ?? 'ACCOUNT';
            $payload['target']['goid'] = (string)($payload['target']['goid'] ?? $goidVal);
        }

        // try SDK first
        if ($this->client !== null && method_exists($this->client, 'createPayment')) {
            try {
                $resp = $this->client->createPayment($payload);
                if (is_object($resp)) {
                    $json = json_encode($resp);
                    if ($json === false) {
                        throw new GoPayPaymentException('Failed to encode createPayment SDK response as JSON');
                    }
                    $resp = json_decode($json, true);
                }
                if (!is_array($resp)) {
                    throw new GoPayPaymentException('Unexpected SDK response type for createPayment');
                }
                return $resp;
            } catch (\Throwable $e) {
                $this->logSafe('warning', 'SDK createPayment failed, falling back to HTTP', ['exception' => $e]);
                // continue to HTTP fallback
            }
        }

        // HTTP fallback
        $token = $this->getToken();
        $url = rtrim($this->cfg['gatewayUrl'], '/') . '/api/payments/payment';
        $body = $this->safeJsonEncode($payload);
        $reqId = $this->headerId();
        $headerAssoc = [
            'Authorization' => 'Bearer ' . $token,
            'Content-Type' => 'application/json',
            'Accept' => 'application/json',
            'User-Agent' => 'BlackCat/GoPaySdkWrapper/1.1',
            // avoid "Expect: 100-continue" delays
            'Expect' => '',
            'X-Request-Id' => $reqId,
        ];
        $headers = $this->buildHeaders($headerAssoc);

        $this->logSafe('info', 'GoPay createPayment payload', ['payload' => $this->sanitizeForLog($payload), 'headers' => $this->sanitizeHeadersForLog($headerAssoc)]);

        // perform request with single retry-on-401
        $resp = $this->doRequest('POST', $url, $headers, $body, ['expect_json' => true, 'raw' => true], $headerAssoc);
        $httpCode = (int)($resp['http_code'] ?? 0);
        $decoded = $resp['json'] ?? null;

        if ($httpCode === 401) {
            // token likely expired; clear cache and retry once
            $this->clearTokenCache();
            $token = $this->getToken();
            $headerAssoc['Authorization'] = 'Bearer ' . $token;
            $headers = $this->buildHeaders($headerAssoc);
            $resp = $this->doRequest('POST', $url, $headers, $body, ['expect_json' => true, 'raw' => true], $headerAssoc);
            $httpCode = (int)($resp['http_code'] ?? 0);
            $decoded = $resp['json'] ?? null;
        }

        if ($httpCode < 200 || $httpCode >= 300) {
            $msg = is_array($decoded) ? json_encode($decoded) : (string)($resp['body'] ?? '');
            throw new GoPayPaymentException('GoPay createPayment failed HTTP ' . $httpCode . ': ' . $msg);
        }

        if (!is_array($decoded)) {
            // if body isn't JSON but success, return raw wrapper
            return ['raw' => $resp['body'] ?? null];
        }

        return $decoded;
    }

    /**
     * Get status for payment id.
     *
     * Return value is a map:
     *  - status => decoded JSON
     *  - from_cache => bool
     *
     * @return array{status: array<string,mixed>, from_cache: bool}
     */
    public function getStatus(string $gatewayPaymentId): array
    {
        $gwId = trim($gatewayPaymentId);
        if ($gwId === '') {
            throw new \InvalidArgumentException('gatewayPaymentId must not be empty');
        }

        $cacheKey = 'gopay_status_' . substr(hash('sha256', $gwId), 0, 32);

        // cache fast-path
        try {
            $cached = $this->cache->get($cacheKey);
            if (is_array($cached)) {
                return ['status' => $cached, 'from_cache' => true];
            }
        } catch (\Throwable $_) {
        }

        // SDK if possible
        if ($this->client !== null && method_exists($this->client, 'paymentStatus')) {
            try {
                $resp = $this->client->paymentStatus((int)$gwId);
                if (is_object($resp)) {
                    $json = json_encode($resp);
                    if ($json === false) {
                        throw new GoPayPaymentException('Failed to encode paymentStatus SDK response as JSON');
                    }
                    $resp = json_decode($json, true);
                }
                if (!is_array($resp)) {
                    throw new GoPayPaymentException('Unexpected SDK response type for paymentStatus');
                }
                // cache response
                try {
                    $this->cache->set($cacheKey, $resp, 3600);
                } catch (\Throwable $_) {
                }
                return ['status' => $resp, 'from_cache' => false];
            } catch (\Throwable $e) {
                $this->logSafe('warning', 'SDK paymentStatus failed, falling back to HTTP', ['exception' => $e]);
            }
        }

        // HTTP fallback
        $token = $this->getToken();
        $url = rtrim($this->cfg['gatewayUrl'], '/') . '/api/payments/payment/' . rawurlencode($gwId);
        $reqId = $this->headerId();
        $headerAssoc = [
            'Authorization' => 'Bearer ' . $token,
            'Accept' => 'application/json',
            'User-Agent' => 'BlackCat/GoPaySdkWrapper/1.1',
            'Expect' => '',
            'X-Request-Id' => $reqId,
        ];
        $headers = $this->buildHeaders($headerAssoc);

        $resp = $this->doRequest('GET', $url, $headers, null, ['expect_json' => true, 'raw' => true], $headerAssoc);
        $httpCode = (int)($resp['http_code'] ?? 0);
        $decoded = $resp['json'] ?? null;

        if ($httpCode === 401) {
            $this->clearTokenCache();
            $token = $this->getToken();
            $headerAssoc['Authorization'] = 'Bearer ' . $token;
            $headers = $this->buildHeaders($headerAssoc);
            $resp = $this->doRequest('GET', $url, $headers, null, ['expect_json' => true, 'raw' => true], $headerAssoc);
            $httpCode = (int)($resp['http_code'] ?? 0);
            $decoded = $resp['json'] ?? null;
        }

        if ($httpCode < 200 || $httpCode >= 300) {
            $msg = is_array($decoded) ? json_encode($decoded) : (string)($resp['body'] ?? '');
            throw new GoPayPaymentException('GoPay getStatus failed HTTP ' . $httpCode . ': ' . $msg);
        }
        $status = is_array($decoded) ? $decoded : ['raw' => $resp['body'] ?? null];
        try {
            $this->cache->set($cacheKey, $status, 3600);
        } catch (\Throwable $_) {
        }
        return ['status' => $status, 'from_cache' => false];
    }

    public function refundPayment(string $gatewayPaymentId, array $args)
    {
        $gwId = trim($gatewayPaymentId);
        if ($gwId === '') {
            throw new \InvalidArgumentException('gatewayPaymentId must not be empty');
        }

        // try SDK if possible
        if ($this->client !== null && method_exists($this->client, 'refundPayment')) {
            try {
                return $this->client->refundPayment((int)$gwId, $args);
            } catch (\Throwable $e) {
                $this->logSafe('warning', 'SDK refundPayment failed, falling back to HTTP', ['exception' => $e]);
            }
        }

        $token = $this->getToken();
        $url = rtrim($this->cfg['gatewayUrl'], '/') . '/api/payments/payment/' . rawurlencode($gwId) . '/refund';
        $body = $this->safeJsonEncode($args);
        $reqId = $this->headerId();
        $headerAssoc = [
            'Authorization' => 'Bearer ' . $token,
            'Content-Type' => 'application/json',
            'Accept' => 'application/json',
            'User-Agent' => 'BlackCat/GoPaySdkWrapper/1.1',
            'Expect' => '',
            'X-Request-Id' => $reqId,
        ];
        $headers = $this->buildHeaders($headerAssoc);

        $resp = $this->doRequest('POST', $url, $headers, $body, ['expect_json' => true, 'raw' => true], $headerAssoc);
        $httpCode = (int)($resp['http_code'] ?? 0);
        $decoded = $resp['json'] ?? null;
        if ($httpCode < 200 || $httpCode >= 300) {
            $msg = is_array($decoded) ? json_encode($decoded) : (string)($resp['body'] ?? '');
            throw new GoPayPaymentException('GoPay refundPayment failed HTTP ' . $httpCode . ': ' . $msg);
        }
        return is_array($decoded) ? $decoded : ['raw' => $resp['body'] ?? null];
    }

    /**
     * Clear cached OAuth token (best-effort).
     */
    private function clearTokenCache(): void
    {
        $lockKey = 'gopay_token_lock_' . substr(hash('sha256', $this->cacheKey), 0, 12);
        $lockToken = null;
        if ($this->cache instanceof LockingCacheInterface) {
            try {
                $lockToken = $this->cache->acquireLock($lockKey, 10);
            } catch (\Throwable $_) {
                $lockToken = null;
            }
        }

        try {
            $this->cache->delete($this->cacheKey);
        } catch (\Throwable $_) {
            try {
                $this->cache->set($this->cacheKey, null, 1);
            } catch (\Throwable $_) {
            }
        }

        // release lock if we acquired it
        if (!empty($lockToken) && $this->cache instanceof LockingCacheInterface) {
            try {
                $this->cache->releaseLock($lockKey, $lockToken);
            } catch (\Throwable $_) {
            }
        }
    }

    /**
     * Central HTTP with retry/backoff.
     *
     * @param array<int,string> $headers
     * @param array<string,mixed> $options
     * @param array<string,mixed>|null $assocHeaders
     * @return array{http_code: int, body: string, json: mixed}
     */
    private function doRequest(string $method, string $url, array $headers = [], ?string $body = null, array $options = [], ?array $assocHeaders = null): array
    {
        $attempts = $options['attempts'] ?? 3;
        $backoffMs = $options['backoff_ms'] ?? 200;
        $expectJson = $options['expect_json'] ?? false;
        $timeout = $options['timeout'] ?? 15;

        $lastEx = null;
        for ($i = 0; $i < $attempts; $i++) {
            $ch = curl_init();
            curl_setopt($ch, CURLOPT_URL, $url);
            curl_setopt($ch, CURLOPT_CUSTOMREQUEST, $method);
            curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
            curl_setopt($ch, CURLOPT_FOLLOWLOCATION, false);
            curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 5);
            curl_setopt($ch, CURLOPT_TIMEOUT, $timeout);
            curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, $options['ssl_verify_peer'] ?? true);
            curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, 2);

            if (!empty($headers)) curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
            if ($body !== null) curl_setopt($ch, CURLOPT_POSTFIELDS, $body);

            $resp = curl_exec($ch);
            $curlErrNo = curl_errno($ch);
            $curlErr = curl_error($ch);
            $info = curl_getinfo($ch);
            $httpCode = (int)$info['http_code'];

            // close the handle
            curl_close($ch);

            if ($resp === false || $curlErrNo !== 0) {
                $lastEx = new GoPayHttpException('CURL error: ' . $curlErr . ' (' . $curlErrNo . ')');
                $this->logSafe('warning', 'HTTP request failed (curl)', ['url' => $url, 'errno' => $curlErrNo, 'info' => $info, 'headers' => $this->sanitizeHeadersForLog($assocHeaders ?? [])]);
                $backoffMs = min($backoffMs * 2, 2000);
                usleep((int)(($backoffMs + random_int(0, 250)) * 1000));
                continue;
            }

            if (!is_string($resp)) {
                $lastEx = new GoPayHttpException('Unexpected CURL response type');
                $this->logSafe('warning', 'HTTP request failed (unexpected curl response type)', ['url' => $url, 'info' => $info, 'headers' => $this->sanitizeHeadersForLog($assocHeaders ?? [])]);
                $backoffMs = min($backoffMs * 2, 2000);
                usleep((int)(($backoffMs + random_int(0, 250)) * 1000));
                continue;
            }

            // decode json when requested/possible
            $decoded = null;
            if ($expectJson) {
                if ($httpCode === 204 || $resp === '') {
                    $decoded = null;
                } else {
                    $decoded = json_decode($resp, true);
                    if (json_last_error() !== JSON_ERROR_NONE) {
                        $sample = substr($resp, 0, 1000);
                        $this->logSafe('error', 'Invalid JSON response from remote', ['body_sample' => $sample, 'http_code' => $httpCode]);
                        throw new GoPayHttpException('Invalid JSON response: ' . json_last_error_msg());
                    }
                }
            }
            return ['http_code' => $httpCode, 'body' => $resp, 'json' => $decoded];
        }

        $ex = new GoPayHttpException('HTTP request failed after retries: ' . ($lastEx ? $lastEx->getMessage() : 'unknown'));
        $this->logSafe('error', 'HTTP request failed after retries', ['phase' => 'doRequest', 'url' => $url, 'exception' => $ex->getMessage()]);
        throw $ex;
    }

    /**
     * Generic sanitizer used by other helpers.
     *
     * Options:
     *  - 'sensitive_keys' => array of lowercased keys to redact
     *  - 'max_string' => int max length before truncation (0 = no truncation)
     *  - 'redact_patterns' => array of regexes; strings matching any pattern are redacted
     *  - 'header_mode' => bool when true, treat $data as header assoc (special header rules)
     *  - 'keep_throwable' => bool when true, keep Throwable objects unchanged
     *
     * Returns sanitized copy (same type: string|array|scalar)
     */
    /**
     * @param array<string,mixed> $opts
     */
    private function sanitize(mixed $data, array $opts = []): mixed
    {
        $defaults = [
            'sensitive_keys' => [
                'account','number','pan','email','phone','phone_number','iban','accountnumber',
                'clientsecret','client_secret','card_number','cardnum','cc_number','ccnum','cvv','cvc',
                'payment_method_token','access_token','refresh_token','clientid','client_id','secret',
                'authorization','auth','password','pwd','token','api_key','apikey'
            ],
            'max_string' => 200,
            'redact_patterns' => [
                '/^(?:[A-Za-z0-9_\\-]{20,})$/',         // long token-like
                '/^(?:[A-Fa-f0-9]{20,})$/',             // long hex
                '/^(?:[A-Za-z0-9+\\/=]{40,})$/'          // base64-like long
            ],
            'header_mode' => false,
            'keep_throwable' => true,
        ];
        $o = array_merge($defaults, $opts);
        // normalize keys to lowercase for comparison
        $sensitiveKeys = array_map('strtolower', $o['sensitive_keys']);
        $maxString = (int)$o['max_string'];
        $redactPatterns = (array)$o['redact_patterns'];
        $headerMode = (bool)$o['header_mode'];
        $keepThrowable = (bool)$o['keep_throwable'];

        $recurse = function ($v, $k = null) use (&$recurse, $sensitiveKeys, $maxString, $redactPatterns, $headerMode, $keepThrowable) {
            // Keep Throwable objects intact (PSR loggers handle them)
            if ($keepThrowable && $v instanceof \Throwable) {
                return $v;
            }

            // Arrays -> recurse
            if (is_array($v)) {
                $out = [];
                foreach ($v as $kk => $vv) {
                    // if header mode and numeric keys -> preserve as-is (rare)
                    $out[$kk] = $recurse($vv, $kk);
                }
                return $out;
            }

            // Strings -> check sensitive keys/patterns and truncate
            if (is_string($v)) {
                // header mode: keys are header names; treat specially
                if ($headerMode && $k !== null) {
                    $lk = strtolower((string)$k);
                    if (in_array($lk, ['authorization','proxy-authorization'], true)) {
                        return '[REDACTED]';
                    }
                    // for other headers keep them but truncate if too long
                    if ($maxString > 0 && strlen($v) > $maxString) {
                        return substr($v, 0, $maxString) . '…';
                    }
                    return $v;
                }

                // redact by key name
                if ($k !== null && in_array(strtolower((string)$k), $sensitiveKeys, true)) {
                    return '[REDACTED]';
                }

                // redact by pattern (token-like strings)
                foreach ($redactPatterns as $pat) {
                    if (preg_match($pat, $v)) {
                        return '[REDACTED]';
                    }
                }

                // truncate
                if ($maxString > 0 && strlen($v) > $maxString) {
                    return substr($v, 0, $maxString) . '…';
                }
                return $v;
            }

            // Scalars: ints, floats, bool, null -> keep
            return $v;
        };

        return $recurse($data, null);
    }

    /**
     * @param array<string,mixed>|string $data
     * @return array<string,mixed>|string
     */
    private function sanitizeForLog(array|string $data): array|string
    {
        // if string, we treat as value (no key context) -> apply basic redaction+truncate
        if (is_string($data)) {
            return $this->sanitize($data, ['max_string' => 1000, 'keep_throwable' => false]);
        }
        return $this->sanitize($data, ['max_string' => 200, 'keep_throwable' => false]);
    }

    /**
     * @param array<string,mixed> $context
     * @return array<string,mixed>
     */
    private function sanitizeContext(array $context): array
    {
        return $this->sanitize($context, ['max_string' => 200, 'keep_throwable' => true]);
    }

    /**
     * @param array<string,mixed> $assoc
     * @return array<string,mixed>
     */
    private function sanitizeHeadersForLog(array $assoc): array
    {
        return $this->sanitize($assoc, ['header_mode' => true, 'max_string' => 200, 'keep_throwable' => false]);
    }

    /**
     * @param array<string,mixed> $context
     */
    private function logSafe(string $level, string $message, array $context = []): void
    {
        try {
            if (!isset($this->logger)) return;

            // normalize aliases
            $map = [
                'warn' => 'warning',
                'err' => 'error',
                'crit' => 'critical',
            ];
            $level = $map[$level] ?? $level;

            $ctx = $this->sanitizeContext($context);

            if (method_exists($this->logger, 'log')) {
                $this->logger->log($level, $message, $ctx);
            } elseif (method_exists($this->logger, $level)) {
                $this->logger->{$level}($message, $ctx);
            } elseif (method_exists($this->logger, 'info')) {
                $this->logger->info($message, $ctx);
            }
        } catch (\Throwable $_) {
            // swallow
        }
    }

    private function headerId(): string
    {
        try {
            return bin2hex(random_bytes(16));
        } catch (\Throwable) {
            return (string)time();
        }
    }
}
