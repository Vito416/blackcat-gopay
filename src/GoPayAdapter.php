<?php

declare(strict_types=1);

namespace BlackCat\GoPay;

use BlackCat\Core\Database;
use BlackCat\Database\Packages\IdempotencyKeys\Repository\IdempotencyKeyRepository;
use BlackCat\Database\Packages\OrderItems\Criteria as OrderItemsCriteria;
use BlackCat\Database\Packages\OrderItems\Repository\OrderItemRepository;
use BlackCat\Database\Packages\Orders\Repository\OrderRepository;
use BlackCat\Database\Packages\PaymentWebhooks\Repository\PaymentWebhookRepository;
use BlackCat\Database\Packages\Payments\Criteria as PaymentsCriteria;
use BlackCat\Database\Packages\Payments\Repository\PaymentRepository;
use Psr\Log\LoggerInterface;
use Psr\SimpleCache\CacheInterface;

final class GoPayAdapter
{
    private readonly OrderRepository $orders;
    private readonly OrderItemRepository $orderItems;
    private readonly PaymentRepository $payments;
    private readonly IdempotencyKeyRepository $idempotency;
    private readonly PaymentWebhookRepository $webhooks;

    private bool $allowCreate = false;

    public function __construct(
        private readonly Database $db,
        private readonly PaymentGatewayInterface $gopayClient,
        private readonly LoggerInterface $logger,
        ?object $mailer = null,
        private readonly string $notificationUrl = '',
        private readonly string $returnUrl = '',
        private readonly ?CacheInterface $cache = null,
    ) {
        unset($mailer);
        $this->orders = new OrderRepository($db);
        $this->orderItems = new OrderItemRepository($db);
        $this->payments = new PaymentRepository($db);
        $this->idempotency = new IdempotencyKeyRepository($db);
        $this->webhooks = new PaymentWebhookRepository($db);
    }

    /**
     * Fetch order items and normalise structure for payment payload.
     *
     * Returns array of items like:
     *  [
     *    ['title' => '...', 'price_snapshot' => 12.34, 'qty' => 1],
     *    ...
     *  ]
     *
     * Non-fatal: returns [] when `order_items` is not installed.
     *
     * @return array<int,array{title:string,price_snapshot:float,qty:int}>
     */
    private function fetchOrderItemsForPayload(int $tenantId, int $orderId): array
    {
        try {
            $crit = OrderItemsCriteria::fromDb($this->db)
                ->where('tenant_id', '=', $tenantId)
                ->where('order_id', '=', $orderId)
                ->orderBy('id', 'ASC')
                ->setPerPage(500)
                ->setPage(1);

            $page = $this->orderItems->paginate($crit);
            $rows = is_array($page['items'] ?? null) ? (array)$page['items'] : [];

            $out = [];
            foreach ($rows as $r) {
                if (!is_array($r)) {
                    continue;
                }

                $title = (string)($r['title_snapshot'] ?? $r['product_ref'] ?? 'item');
                $price = (float)($r['unit_price'] ?? 0.0);
                $qty = (int)($r['quantity'] ?? 1);

                $out[] = [
                    'title' => $title !== '' ? $title : 'item',
                    'price_snapshot' => $price,
                    'qty' => max(1, $qty),
                ];
            }
            return $out;
        } catch (\Throwable $e) {
            $this->logSafe('debug', 'fetchOrderItemsForPayload failed (non-fatal)', ['phase' => 'fetchOrderItemsForPayload', 'order_id' => $orderId, 'exception' => $e]);
            return [];
        }
    }

    /**
     * Create a payment for an existing order.
     *
     * Returns ['payment_id'=>int, 'redirect_url'=>?string, 'gopay'=>array, 'tenant_id'=>int, 'order_id'=>int]
     *
     * @return array<string,mixed>
     */
    public function createPaymentFromOrder(int $orderId, string $idempotencyKey): array
    {
        $idempotencyKey = trim($idempotencyKey);
        if ($idempotencyKey === '') {
            throw new \InvalidArgumentException('idempotencyKey is required and must be non-empty');
        }

        if (!preg_match('/^[A-Za-z0-9._:-]{6,128}$/', $idempotencyKey)) {
            throw new \InvalidArgumentException('idempotencyKey has invalid format');
        }

        $order = $this->orders->getById($orderId, false);
        if (!is_array($order)) {
            throw new \RuntimeException('Order not found: ' . $orderId);
        }

        $tenantId = (int)($order['tenant_id'] ?? 0);
        if ($tenantId <= 0) {
            throw new \RuntimeException('Order missing tenant_id: ' . $orderId);
        }

        if (($order['status'] ?? null) !== 'pending') {
            throw new \RuntimeException('Order not in pending state: ' . $orderId);
        }

        $cached = $this->lookupIdempotency($idempotencyKey, $tenantId);
        if ($cached !== null) {
            $this->logSafe('info', 'Idempotent createPaymentFromOrder hit (lookupIdempotency)', ['order_id' => $orderId, 'tenant_id' => $tenantId]);
            return $cached;
        }

        $idempHash = self::hashIdempotencyKey($tenantId, $idempotencyKey);

        // Reserve idempotency key (prevents concurrent duplicates). If already reserved, wait for completion.
        try {
            $this->idempotency->insert([
                'key_hash' => $idempHash,
                'tenant_id' => $tenantId,
                'order_id' => $orderId,
                'ttl_seconds' => 86400,
            ]);
        } catch (\Throwable $e) {
            if ($this->isUniqueViolation($e)) {
                $maybe = $this->waitForIdempotencyResult($idempotencyKey, $tenantId, 2000);
                if ($maybe !== null) {
                    return $maybe;
                }
                throw new \RuntimeException('idempotency_in_progress', 0, $e);
            }
            throw $e;
        }

        // Build payload
        $amountCents = (int)round(((float)($order['total'] ?? 0.0)) * 100);
        $payload = [
            'amount' => $amountCents,
            'currency' => $order['currency'] ?? 'EUR',
            'order_number' => $order['uuid'] ?? (string)$orderId,
            'callback' => [
                'return_url' => $this->returnUrl,
                'notification_url' => $this->notificationUrl,
            ],
            'order_description' => 'Order ' . ($order['uuid'] ?? (string)$orderId),
        ];

        $items = $this->fetchOrderItemsForPayload($tenantId, $orderId);
        $payload['items'] = array_map(static function (array $it): array {
            return [
                'name' => (string)$it['title'],
                'amount' => (int)round(((float)$it['price_snapshot']) * 100),
                'count' => (int)$it['qty'],
            ];
        }, $items);

        $sumItems = 0;
        foreach ($payload['items'] as $it) {
            $amt = (int)($it['amount'] ?? 0);
            $cnt = max(1, (int)($it['count'] ?? 1));
            if ($amt < 0) {
                throw new \RuntimeException('Invalid item amount in payload');
            }
            $sumItems += $amt * $cnt;
        }
        if ($sumItems !== $payload['amount']) {
            $this->logSafe('warning', 'Payment amount mismatch between items and total', [
                'order_id' => $orderId,
                'tenant_id' => $tenantId,
                'items_sum' => $sumItems,
                'amount' => $payload['amount'],
            ]);
        }

        // 2) Provisional payment row (with order lock) + mark idempotency in-progress (payment_id set).
        $paymentId = $this->db->transaction(function (Database $d) use ($orderId, $tenantId, $idempHash, $order): int {
            $orders = new OrderRepository($d);
            $payments = new PaymentRepository($d);
            $idemp = new IdempotencyKeyRepository($d);

            $locked = $orders->lockById($orderId, 'wait');
            if (!is_array($locked)) {
                throw new \RuntimeException('Order disappeared during processing: ' . $orderId);
            }
            if ((int)($locked['tenant_id'] ?? 0) !== $tenantId) {
                throw new \RuntimeException('Order tenant mismatch during processing: ' . $orderId);
            }
            if (($locked['status'] ?? null) !== 'pending') {
                throw new \RuntimeException('Order no longer pending: ' . $orderId);
            }

            $payments->insert([
                'tenant_id' => $tenantId,
                'order_id' => $orderId,
                'gateway' => 'gopay',
                'transaction_id' => null,
                'status' => 'initiated',
                'amount' => self::safeDecimal($locked['total'] ?? ($order['total'] ?? 0)),
                'currency' => $locked['currency'] ?? ($order['currency'] ?? 'EUR'),
                'details' => null,
            ]);

            $id = $d->lastInsertId();
            $paymentId = (is_string($id) && ctype_digit($id) && (int)$id > 0) ? (int)$id : 0;
            if ($paymentId <= 0) {
                throw new \RuntimeException('Unable to allocate payment id');
            }

            // mark idempotency row as in-progress
            $idemp->updateById($idempHash, [
                'payment_id' => $paymentId,
                'order_id' => $orderId,
            ]);

            return $paymentId;
        });

        // 3) Call GoPay
        $gopayResponse = null;
        try {
            $this->logSafe('info', 'Calling GoPay createPayment', ['order_id' => $orderId, 'tenant_id' => $tenantId, 'payload' => $this->sanitizeForLog($payload)]);
            $gopayResponse = $this->gopayClient->createPayment($payload);
        } catch (\Throwable $e) {
            // Attempt to mark provisioned payment as failed to aid reconciliation
            try {
                $this->payments->updateById($paymentId, [
                    'status' => 'failed',
                    'details' => $this->jsonEncodeSafe(['error' => (string)$e]) ?? '{"error":"encoding_failed"}',
                ]);
            } catch (\Throwable $_) {
            }

            try {
                $this->idempotency->updateById($idempHash, [
                    'gateway_payload' => $this->jsonEncodeSafe(['error' => (string)$e]) ?? null,
                    'redirect_url' => null,
                ]);
            } catch (\Throwable $_) {
            }

            $this->logSafe('error', 'gopay.createPayment failed', ['phase' => 'gopay.createPayment', 'order_id' => $orderId, 'tenant_id' => $tenantId, 'exception' => $e]);
            throw $e;
        }

        // 4) Persist gateway id/details + idempotency result in one short transaction
        $gopayArr = $this->normalizeGopayResponseToArray($gopayResponse);
        $gwId = $this->extractGatewayPaymentId($gopayArr);
        $redirectUrl = $this->extractRedirectUrl($gopayArr);

        $detailsJson = $this->jsonEncodeSafe([
            'note' => 'gopay_payload_cached',
            'cached_at' => (int)time(),
            'gw_id' => $gwId,
        ]) ?? '{}';

        $idempPayloadJson = $this->jsonEncodeSafe([
            'gw_id' => $gwId,
            'redirect_url' => $redirectUrl,
        ]);

        $this->db->transaction(function (Database $d) use ($paymentId, $idempHash, $gwId, $detailsJson, $idempPayloadJson, $redirectUrl): void {
            $payments = new PaymentRepository($d);
            $idemp = new IdempotencyKeyRepository($d);

            $payments->updateById($paymentId, [
                'transaction_id' => $gwId !== '' ? $gwId : null,
                'status' => 'pending',
                'details' => $detailsJson,
            ]);

            $idemp->updateById($idempHash, [
                'gateway_payload' => $idempPayloadJson,
                'redirect_url' => $redirectUrl,
            ]);
        });

        // AFTER COMMIT: best-effort cache write
        $result = [
            'payment_id' => $paymentId,
            'redirect_url' => $redirectUrl,
            'gopay' => $idempPayloadJson !== null ? ($this->jsonDecodeSafe($idempPayloadJson) ?? []) : [],
            'order_id' => $orderId,
            'tenant_id' => $tenantId,
        ];
        try {
            $this->persistIdempotency($idempotencyKey, $result, $paymentId, $tenantId);
        } catch (\Throwable $e) {
            $this->logSafe('warning', 'persistIdempotency after commit failed', ['exception' => $e]);
        }

        return $result;
    }

    /**
     * Handle GoPay notification by gateway transaction id.
     *
     * @return array{action:string}
     */
    public function handleNotify(string $gwId, ?bool $allowCreate = null): array
    {
        $lastError = null;
        $allowCreate = $allowCreate ?? $this->allowCreate;
        unset($allowCreate); // reserved for future behavior

        $gwId = trim((string)$gwId);
        if ($gwId === '') {
            $this->logSafe('warning', 'Notify called without gateway id', ['gwId' => $gwId]);
            throw new \RuntimeException('Webhook missing gateway payment id');
        }

        $status = null;
        $fromCache = false;
        $cacheKey = 'gopay_status_' . substr(hash('sha256', $gwId), 0, 32);

        try {
            $resp = $this->gopayClient->getStatus($gwId);
            if (is_array($resp) && array_key_exists('status', $resp) && is_array($resp['status'])) {
                $status = $resp['status'];
                $fromCache = !empty($resp['from_cache']);
            } else {
                $status = $resp;
                $fromCache = false;
            }
        } catch (\Throwable $e) {
            $lastError = $e;
            $this->logSafe('error', 'gopay.getStatus failed', ['phase' => 'gopay.getStatus', 'gopay_id' => $gwId, 'exception' => $e]);
        }

        $gwState = is_array($status) ? ($status['state'] ?? null) : null;
        $statusEnum = GoPayStatus::tryFrom((string)$gwState);

        // If cached and non-permanent -> delete cache and refresh
        if ($fromCache && $statusEnum !== null && $statusEnum->isNonPermanent()) {
            $this->logSafe('info', 'Cached non-permanent status detected, refreshing from GoPay', ['gopay_id' => $gwId, 'cache_key' => $cacheKey, 'status' => $status]);
            try {
                if (isset($this->cache) && $this->cache instanceof CacheInterface) {
                    $this->cache->delete($cacheKey);
                }
            } catch (\Throwable $e) {
                $lastError = $e;
            }

            try {
                $resp2 = $this->gopayClient->getStatus($gwId);
                if (is_array($resp2) && array_key_exists('status', $resp2) && is_array($resp2['status'])) {
                    $status = $resp2['status'];
                    $fromCache = !empty($resp2['from_cache']);
                } else {
                    $status = $resp2;
                    $fromCache = false;
                }
            } catch (\Throwable $e) {
                $lastError = $e;
                $this->logSafe('error', 'gopay.getStatus.refresh failed', ['phase' => 'gopay.getStatus.refresh', 'gopay_id' => $gwId, 'exception' => $e]);
            }
        }

        $this->logSafe('info', 'GoPay status fetched for notify', ['gopay_id' => $gwId, 'from_cache' => $fromCache, 'status' => $status]);

        // final fallback: if still null, attempt one more time and cache
        if ($status === null) {
            try {
                $status = $this->gopayClient->getStatus($gwId);
            } catch (\Throwable $e) {
                $lastError = $e;
            }
            if (isset($this->cache) && $this->cache instanceof CacheInterface) {
                try {
                    $this->cache->set($cacheKey, $status, 3600);
                } catch (\Throwable $_) {
                }
            }
        }

        $gwState = is_array($status) ? ($status['state'] ?? null) : null;
        $statusEnum = GoPayStatus::tryFrom((string)$gwState);

        $jsonForHash = $this->jsonEncodeSafe($status) ?? '';
        $payloadHash = hash('sha256', $jsonForHash);

        // dedupe check — verify if the webhook already exists in payment_webhooks
        try {
            $exists = $this->webhooks->getByPayloadHash($payloadHash, false);
        } catch (\Throwable $e) {
            $lastError = $e;
            $exists = null;
        }

        if (is_array($exists)) {
            $action = 'done';
            if (!empty($lastError)) {
                $action = 'fail';
            } elseif ($statusEnum?->isNonPermanent() === true) {
                $action = 'delete';
            }
            return ['action' => $action];
        }

        $paymentId = $this->findPaymentIdByGatewayId($gwId);
        $this->persistWebhookRecord($gwId, $payloadHash, $status, $fromCache, $paymentId);

        $action = 'done';
        if ($statusEnum !== null && $statusEnum->isNonPermanent()) {
            $action = 'delete';
        } elseif (!empty($lastError)) {
            $action = 'fail';
        }

        return ['action' => $action];
    }

    /**
     * @return array<string,mixed>
     */
    public function fetchStatus(string $gopayPaymentId): array
    {
        $statusCacheKey = 'gopay_status_' . substr(hash('sha256', $gopayPaymentId), 0, 32);

        if (!isset($this->cache) || !($this->cache instanceof CacheInterface)) {
            return [
                'state' => 'CREATED',
                '_pseudo' => true,
                '_cached' => false,
                '_message' => 'No cache instance available; returning pseudo CREATED.',
            ];
        }

        try {
            $cached = $this->cache->get($statusCacheKey);
        } catch (\Throwable $e) {
            $cached = null;
        }

        if (!is_array($cached)) {
            return [
                'state' => 'CREATED',
                '_pseudo' => true,
                '_cached' => false,
                '_message' => 'No cached gateway status available or invalid format.',
            ];
        }

        $state = $cached['state'] ?? ($cached['status']['state'] ?? 'CREATED');
        $out = $cached;
        $out['_cached'] = true;
        $out['state'] = (string)$state;
        return $out;
    }

    /**
     * @return array<string,mixed>
     */
    public function refundPayment(string $gopayPaymentId, float $amount): array
    {
        $amt = (int)round($amount * 100);
        $resp = $this->gopayClient->refundPayment($gopayPaymentId, ['amount' => $amt]);
        return is_array($resp) ? $resp : ['raw' => $resp];
    }

    /**
     * Lookup idempotency key (Cache first, DB fallback).
     *
     * Returns: ['payment_id','redirect_url','gopay','order_id','tenant_id'] or null if miss.
     *
     * @return array<string,mixed>|null
     */
    public function lookupIdempotency(string $idempotencyKey, ?int $tenantId = null): ?array
    {
        $idempotencyKey = trim((string)$idempotencyKey);
        if ($idempotencyKey === '') {
            throw new \InvalidArgumentException('lookupIdempotency: non-empty idempotencyKey required');
        }
        if (!preg_match('/^[A-Za-z0-9._:-]{6,128}$/', $idempotencyKey)) {
            $this->logSafe('warning', 'lookupIdempotency: invalid key format', ['key' => $idempotencyKey]);
            return null;
        }

        $tenantId = $tenantId !== null && $tenantId > 0 ? $tenantId : null;
        $hashes = $tenantId !== null
            ? [self::hashIdempotencyKey($tenantId, $idempotencyKey), hash('sha256', $idempotencyKey)]
            : [hash('sha256', $idempotencyKey)];

        foreach ($hashes as $hash) {
            $cacheKey = $this->makeCacheKey($hash, $tenantId);

            $cached = $this->cacheGetSafe($cacheKey);
            if (is_array($cached) && isset($cached['payment_id'])) {
                return $cached;
            }

            try {
                $row = $this->idempotency->getById($hash, false);
            } catch (\Throwable $e) {
                $this->logSafe('error', 'lookupIdempotency: DB read failed', ['exception' => $e]);
                continue;
            }

            if (!is_array($row)) {
                continue;
            }

            if (!$this->isNotExpired($row['created_at'] ?? null, (int)($row['ttl_seconds'] ?? 86400))) {
                continue;
            }

            $pid = isset($row['payment_id']) ? (int)$row['payment_id'] : 0;
            if ($pid <= 0) {
                continue;
            }

            $gopay = null;
            if (isset($row['gateway_payload']) && $row['gateway_payload'] !== null && $row['gateway_payload'] !== '') {
                $gopay = is_string($row['gateway_payload'])
                    ? $this->jsonDecodeSafe($row['gateway_payload'])
                    : $row['gateway_payload'];
            }

            $out = [
                'payment_id' => $pid,
                'redirect_url' => $row['redirect_url'] ?? null,
                'gopay' => $gopay,
                'order_id' => isset($row['order_id']) ? (int)$row['order_id'] : null,
                'tenant_id' => isset($row['tenant_id']) ? (int)$row['tenant_id'] : $tenantId,
            ];

            $ttl = 86400;
            $remaining = $this->remainingTtlSeconds($row['created_at'] ?? null, (int)($row['ttl_seconds'] ?? 86400));
            if ($remaining !== null) {
                $ttl = max(60, $remaining);
            }
            $this->cacheSetSafe($cacheKey, $out, $ttl);
            return $out;
        }

        return null;
    }

    /**
     * Persist idempotency: write DB (best-effort) and cache (best-effort).
     *
     * @param array<string,mixed> $payload
     */
    public function persistIdempotency(string $idempotencyKey, array $payload, int $paymentId, ?int $tenantId = null): void
    {
        $tenantId = $tenantId !== null && $tenantId > 0 ? $tenantId : (int)($payload['tenant_id'] ?? 0);
        if ($tenantId <= 0) {
            $this->logSafe('warning', 'persistIdempotency skipped: missing tenant_id', ['payment_id' => $paymentId]);
            return;
        }

        $idempotencyKey = trim((string)$idempotencyKey);
        if ($idempotencyKey === '') {
            throw new \InvalidArgumentException('persistIdempotency: non-empty idempotencyKey required');
        }

        $hash = self::hashIdempotencyKey($tenantId, $idempotencyKey);
        $redirectUrl = $payload['gw_url'] ?? $payload['payment_redirect'] ?? $payload['redirect_url'] ?? null;
        $orderId = isset($payload['order_id']) ? (int)$payload['order_id'] : null;

        $gatewayPayload = $payload['gopay'] ?? $payload;
        $gatewayPayloadJson = $this->jsonEncodeSafe($gatewayPayload);

        try {
            $this->idempotency->upsertByKeys(
                [
                    'key_hash' => $hash,
                    'tenant_id' => $tenantId,
                    'payment_id' => $paymentId,
                    'order_id' => $orderId,
                    'gateway_payload' => $gatewayPayloadJson,
                    'redirect_url' => is_string($redirectUrl) ? $redirectUrl : null,
                    'ttl_seconds' => 86400,
                ],
                ['key_hash' => $hash],
                ['tenant_id', 'payment_id', 'order_id', 'gateway_payload', 'redirect_url', 'ttl_seconds']
            );
        } catch (\Throwable $e) {
            $this->logSafe('warning', 'persistIdempotency failed', ['exception' => $e]);
        }

        $this->cacheSetSafe(
            $this->makeCacheKey($hash, $tenantId),
            [
                'payment_id' => $paymentId,
                'order_id' => $orderId,
                'redirect_url' => $redirectUrl,
                'gopay' => $gatewayPayload,
                'tenant_id' => $tenantId,
            ],
            86400
        );
    }

    /**
     * @return array<string,mixed>|null
     */
    private function waitForIdempotencyResult(string $idempotencyKey, int $tenantId, int $timeoutMs): ?array
    {
        $timeoutMs = max(0, $timeoutMs);
        $until = microtime(true) + ($timeoutMs / 1000.0);
        do {
            $row = $this->lookupIdempotency($idempotencyKey, $tenantId);
            if ($row !== null) {
                return $row;
            }
            usleep(100_000);
        } while (microtime(true) < $until);
        return null;
    }

    private function isUniqueViolation(\Throwable $e): bool
    {
        if ($e instanceof \PDOException) {
            $code = (string)$e->getCode();
            if (in_array($code, ['23000', '23505'], true)) {
                return true;
            }
        }
        $msg = strtolower($e->getMessage());
        return str_contains($msg, 'duplicate') || str_contains($msg, 'unique constraint') || str_contains($msg, 'unique violation');
    }

    private function persistWebhookRecord(string $gwId, string $payloadHash, mixed $status, bool $fromCache, ?int $paymentId = null): void
    {
        $payloadJson = $this->jsonEncodeSafe($status);
        try {
            $this->webhooks->insert([
                'payment_id' => $paymentId && $paymentId > 0 ? $paymentId : null,
                'gateway_event_id' => $gwId,
                'payload_hash' => $payloadHash,
                'payload' => $payloadJson,
                'from_cache' => $fromCache ? 1 : 0,
            ]);
        } catch (\Throwable $_) {
            // silent
        }
    }

    /**
     * @return array<string,mixed>
     */
    private function normalizeGopayResponseToArray(mixed $resp): array
    {
        if (is_array($resp)) return $resp;
        if (is_object($resp)) {
            $json = $this->jsonEncodeSafe($resp);
            if (!is_string($json) || $json === '') {
                return [];
            }
            $decoded = $this->jsonDecodeSafe($json);
            return is_array($decoded) ? $decoded : [];
        }
        return ['value' => $resp];
    }

    private function jsonEncodeSafe(mixed $v): ?string
    {
        try {
            return json_encode($v, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE);
        } catch (\JsonException $e) {
            $this->logSafe('error', 'jsonEncodeSafe failed', ['exception' => (string)$e]);
            return null;
        }
    }

    private function jsonDecodeSafe(string $s): mixed
    {
        try {
            return json_decode($s, true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            $this->logSafe('warning', 'jsonDecodeSafe failed', ['exception' => (string)$e]);
            return null;
        }
    }

    /**
     * @param array<string,mixed> $context
     */
    private function logSafe(string $level, string $message, array $context = []): void
    {
        try {
            if (!isset($this->logger)) return;

            $map = ['warn' => 'warning', 'err' => 'error', 'crit' => 'critical'];
            $level = $map[$level] ?? $level;

            if (method_exists($this->logger, 'log')) {
                $this->logger->log($level, $message, $context);
            } elseif (method_exists($this->logger, $level)) {
                $this->logger->{$level}($message, $context);
            } elseif (method_exists($this->logger, 'info')) {
                $this->logger->info($message, $context);
            }
        } catch (\Throwable $_) {
            // swallow
        }
    }

    private function cacheGetSafe(string $key): mixed
    {
        if (empty($this->cache) || !method_exists($this->cache, 'get')) return null;
        try {
            return $this->cache->get($key);
        } catch (\Throwable $_) {
            return null;
        }
    }

    private function cacheSetSafe(string $key, mixed $value, int $ttl): void
    {
        if (empty($this->cache) || !method_exists($this->cache, 'set')) return;
        try {
            $this->cache->set($key, $value, $ttl);
        } catch (\Throwable $_) {
        }
    }

    private function makeCacheKey(string $idempHash, ?int $tenantId): string
    {
        $tid = $tenantId !== null && $tenantId > 0 ? (string)$tenantId : 'no-tenant';
        return 'gopay_idemp_' . md5($tid . '|' . ($this->notificationUrl ?? '') . '|' . ($this->returnUrl ?? '') . '|' . $idempHash);
    }

    /**
     * @param array<string,mixed> $gopayResponse
     */
    private function extractGatewayPaymentId(array $gopayResponse): string
    {
        $candidates = [];
        if (isset($gopayResponse['id']) && $gopayResponse['id'] !== '') $candidates[] = $gopayResponse['id'];
        if (isset($gopayResponse['paymentId']) && $gopayResponse['paymentId'] !== '') $candidates[] = $gopayResponse['paymentId'];

        if (isset($gopayResponse['payment']) && is_array($gopayResponse['payment']) && !empty($gopayResponse['payment']['id'])) {
            $candidates[] = $gopayResponse['payment']['id'];
        }
        if (isset($gopayResponse['data']) && is_array($gopayResponse['data']) && !empty($gopayResponse['data']['id'])) {
            $candidates[] = $gopayResponse['data']['id'];
        }

        foreach ($candidates as $c) {
            if (!empty($c)) return (string)$c;
        }
        return '';
    }

    /**
     * @param array<string,mixed> $gopayResponse
     */
    private function extractRedirectUrl(array $gopayResponse): ?string
    {
        if (isset($gopayResponse[0]) && is_array($gopayResponse[0]) && !empty($gopayResponse[0]['gw_url'])) {
            return (string)$gopayResponse[0]['gw_url'];
        }
        if (!empty($gopayResponse['gw_url'])) return (string)$gopayResponse['gw_url'];
        if (!empty($gopayResponse['payment_redirect'])) return (string)$gopayResponse['payment_redirect'];
        if (!empty($gopayResponse['redirect_url'])) return (string)$gopayResponse['redirect_url'];
        if (isset($gopayResponse['links']) && is_array($gopayResponse['links']) && !empty($gopayResponse['links']['redirect'])) return (string)$gopayResponse['links']['redirect'];
        return null;
    }

    private function findPaymentIdByGatewayId(string $gwId): ?int
    {
        try {
            $crit = PaymentsCriteria::fromDb($this->db)
                ->where('transaction_id', '=', $gwId)
                ->where('gateway', '=', 'gopay')
                ->orderBy('id', 'DESC')
                ->setPerPage(1)
                ->setPage(1);

            $page = $this->payments->paginate($crit);
            $items = is_array($page['items'] ?? null) ? (array)$page['items'] : [];
            $row = $items[0] ?? null;
            if (is_array($row) && isset($row['id'])) {
                $id = (int)$row['id'];
                return $id > 0 ? $id : null;
            }
        } catch (\Throwable $_) {
        }
        return null;
    }

    /**
     * @param array<string,mixed> $a
     * @return array<string,mixed>
     */
    private function sanitizeForLog(array $a): array
    {
        unset($a['card_number'], $a['cvv'], $a['payment_method_token']);
        return $a;
    }

    private function isNotExpired(mixed $createdAt, int $ttlSeconds): bool
    {
        return $this->remainingTtlSeconds($createdAt, $ttlSeconds) !== null;
    }

    private function remainingTtlSeconds(mixed $createdAt, int $ttlSeconds): ?int
    {
        $ttlSeconds = max(1, $ttlSeconds);
        if ($createdAt instanceof \DateTimeInterface) {
            $ts = $createdAt->getTimestamp();
        } elseif (is_string($createdAt) && $createdAt !== '') {
            $ts = strtotime($createdAt);
            if ($ts === false) {
                return null;
            }
        } else {
            return null;
        }

        $expires = $ts + $ttlSeconds;
        $left = $expires - time();
        return $left > 0 ? $left : null;
    }

    private static function hashIdempotencyKey(int $tenantId, string $idempotencyKey): string
    {
        return hash('sha256', $tenantId . ':' . $idempotencyKey);
    }

    private static function safeDecimal(mixed $v): string
    {
        return number_format((float)$v, 2, '.', '');
    }
}
