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

## Licensing

This repository is an official component of the Blackcat Covered System. It is licensed under `BFNL-1.0`, and repository separation inside `BLACKCAT_MESH_NEXUS` exists for maintenance, safety, auditability, delivery, and architectural clarity. It does not by itself create a separate unavoidable founder-fee or steward/development-fee event for the same ordinary covered deployment.

Canonical licensing bundle:
- BFNL 1.0: https://github.com/Vito416/blackcat-darkmesh-ao/blob/main/docs/BFNL-1.0.md
- Founder Fee Policy: https://github.com/Vito416/blackcat-darkmesh-ao/blob/main/docs/FEE_POLICY.md
- Covered-System Notice: https://github.com/Vito416/blackcat-darkmesh-ao/blob/main/docs/LICENSING_SYSTEM_NOTICE.md
