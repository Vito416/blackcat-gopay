# BlackCat GoPay

GoPay platební modul pro ekosystém BlackCat:
- integrace přes `blackcat-database` (`orders`, `payments`, `idempotency_keys`, `payment_webhooks`),
- idempotentní create-payment write-path (bez raw SQL),
- volitelná cache (PSR-16) pro idempotency a gateway status.

## Instalace

```bash
composer require blackcatacademy/blackcat-gopay
```

## Použití

- Vytvoř `Database` (`blackcat-core`) a nainstaluj DB moduly: `tenants`, `users`, `orders`, `payments`, `idempotency-keys`, `payment-webhooks`.
- Vytvoř klienta `GoPaySdkWrapper` (nebo vlastní implementaci `PaymentGatewayInterface`).
- Použij `GoPayAdapter::createPaymentFromOrder($orderId, $idempotencyKey)`.

