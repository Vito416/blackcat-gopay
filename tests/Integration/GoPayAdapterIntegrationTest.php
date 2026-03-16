<?php
declare(strict_types=1);

namespace BlackCat\GoPay\Tests\Integration;

use BlackCat\Core\Database;
use BlackCat\Database\Packages\IdempotencyKeys\IdempotencyKeysModule;
use BlackCat\Database\Packages\Orders\OrdersModule;
use BlackCat\Database\Packages\Orders\Repository\OrderRepository;
use BlackCat\Database\Packages\PaymentWebhooks\PaymentWebhooksModule;
use BlackCat\Database\Packages\PaymentWebhooks\Repository\PaymentWebhookRepository;
use BlackCat\Database\Packages\Payments\PaymentsModule;
use BlackCat\Database\Packages\Tenants\Repository\TenantRepository;
use BlackCat\Database\Packages\Tenants\TenantsModule;
use BlackCat\Database\Packages\Users\UsersModule;
use BlackCat\GoPay\GoPayAdapter;
use BlackCat\GoPay\PaymentGatewayInterface;
use PHPUnit\Framework\Attributes\PreserveGlobalState;
use PHPUnit\Framework\Attributes\RunClassInSeparateProcess;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use Psr\SimpleCache\CacheInterface;

/**
 * Requires a real DB (MySQL/Postgres); fails if a DB is not reachable.
 */
#[RunClassInSeparateProcess]
#[PreserveGlobalState(false)]
final class GoPayAdapterIntegrationTest extends TestCase
{
    public function testCreatePaymentIsIdempotentAndWritesWebhookDedupe(): void
    {
        $db = $this->initDbOrFail();
        $dialect = $db->dialect();

        if ($dialect->isPg()) {
            try {
                $db->exec('CREATE EXTENSION IF NOT EXISTS pgcrypto;');
            } catch (\Throwable) {
                // best-effort
            }
        }

        (new TenantsModule())->install($db, $dialect);
        (new UsersModule())->install($db, $dialect);
        (new OrdersModule())->install($db, $dialect);
        (new PaymentsModule())->install($db, $dialect);
        (new PaymentWebhooksModule())->install($db, $dialect);
        (new IdempotencyKeysModule())->install($db, $dialect);

        $this->wipeTables($db, [
            'payment_webhooks',
            'idempotency_keys',
            'payments',
            'order_items',
            'orders',
            'users',
            'tenants',
        ]);

        $tenantId = $this->createTenant($db);
        $orderId = $this->createOrder($db, $tenantId);

        $gateway = new class implements PaymentGatewayInterface {
            public function createPayment(array $payload)
            {
                return ['id' => 'GW_TEST_1', 'gw_url' => 'https://example.test/redirect'];
            }
            public function getStatus(string $gatewayPaymentId)
            {
                unset($gatewayPaymentId);
                return ['status' => ['state' => 'PAID'], 'from_cache' => false];
            }
            public function refundPayment(string $gatewayPaymentId, array $args)
            {
                return ['ok' => true, 'id' => $gatewayPaymentId, 'args' => $args];
            }
        };

        $cache = new class implements CacheInterface {
            /** @var array<string,mixed> */
            private array $data = [];
            public function get(string $key, mixed $default = null): mixed { return $this->data[$key] ?? $default; }
            public function set(string $key, mixed $value, null|int|\DateInterval $ttl = null): bool { $this->data[$key] = $value; return true; }
            public function delete(string $key): bool { unset($this->data[$key]); return true; }
            public function clear(): bool { $this->data = []; return true; }
            public function getMultiple(iterable $keys, mixed $default = null): iterable { foreach ($keys as $k) { yield $k => $this->get((string)$k, $default); } }
            /** @param iterable<string,mixed> $values */
            public function setMultiple(iterable $values, null|int|\DateInterval $ttl = null): bool { foreach ($values as $k => $v) { $this->set((string)$k, $v, $ttl); } return true; }
            public function deleteMultiple(iterable $keys): bool { foreach ($keys as $k) { $this->delete((string)$k); } return true; }
            public function has(string $key): bool { return array_key_exists($key, $this->data); }
        };

        $adapter = new GoPayAdapter(
            $db,
            $gateway,
            new NullLogger(),
            mailer: null,
            notificationUrl: 'https://example.test/notify',
            returnUrl: 'https://example.test/return',
            cache: $cache
        );

        $idemp = 'idemp_test_123456';
        $r1 = $adapter->createPaymentFromOrder($orderId, $idemp);
        self::assertSame($tenantId, (int)($r1['tenant_id'] ?? 0));
        self::assertSame($orderId, (int)($r1['order_id'] ?? 0));
        self::assertGreaterThan(0, (int)($r1['payment_id'] ?? 0));
        self::assertSame('https://example.test/redirect', (string)($r1['redirect_url'] ?? ''));

        $r2 = $adapter->createPaymentFromOrder($orderId, $idemp);
        self::assertSame((int)$r1['payment_id'], (int)($r2['payment_id'] ?? 0));

        $notify1 = $adapter->handleNotify('GW_TEST_1');
        self::assertSame('done', $notify1['action']);
        $notify2 = $adapter->handleNotify('GW_TEST_1');
        self::assertSame('done', $notify2['action']);

        $webhooks = new PaymentWebhookRepository($db);
        $json = json_encode(['state' => 'PAID'], JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        self::assertIsString($json);
        $row = $webhooks->getByPayloadHash(hash('sha256', $json), false);
        self::assertIsArray($row);
    }

    private function initDbOrFail(): Database
    {
        $dsn = (string)(getenv('DB_DSN') ?: '');
        if ($dsn === '') {
            self::fail('Integration tests require a real DB. Set DB_DSN/DB_USER/DB_PASSWORD (use a disposable test database).');
        }

        Database::init([
            'dsn' => $dsn,
            'user' => getenv('DB_USER') ?: null,
            'pass' => getenv('DB_PASSWORD') ?: null,
        ]);

        return Database::getInstance();
    }

    private function createTenant(Database $db): int
    {
        $repo = new TenantRepository($db);
        $repo->insert([
            'name' => 'Test Tenant',
            'slug' => 'test',
            'status' => 'active',
        ]);

        $id = $db->lastInsertId();
        if (is_string($id) && ctype_digit($id) && (int)$id > 0) {
            return (int)$id;
        }
        return 0;
    }

    private function createOrder(Database $db, int $tenantId): int
    {
        $repo = new OrderRepository($db);
        $repo->insert([
            'tenant_id' => $tenantId,
            'uuid' => $this->uuidV4(),
            'currency' => 'EUR',
            'subtotal' => '10.00',
            'discount_total' => '0.00',
            'tax_total' => '0.00',
            'total' => '10.00',
            'status' => 'pending',
        ]);

        $id = $db->lastInsertId();
        if (is_string($id) && ctype_digit($id) && (int)$id > 0) {
            return (int)$id;
        }
        return 0;
    }

    private function uuidV4(): string
    {
        $b = random_bytes(16);
        $b[6] = chr((ord($b[6]) & 0x0f) | 0x40);
        $b[8] = chr((ord($b[8]) & 0x3f) | 0x80);
        $hex = bin2hex($b);
        return substr($hex, 0, 8) . '-' . substr($hex, 8, 4) . '-' . substr($hex, 12, 4) . '-' . substr($hex, 16, 4) . '-' . substr($hex, 20, 12);
    }

    /**
     * @param list<string> $tables
     */
    private function wipeTables(Database $db, array $tables): void
    {
        foreach ($tables as $table) {
            $table = trim($table);
            if ($table === '') {
                continue;
            }
            try {
                $db->exec('DELETE FROM ' . $table);
            } catch (\Throwable) {
            }
        }
    }
}
