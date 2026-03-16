<?php

declare(strict_types=1);

namespace BlackCat\GoPay;

interface PaymentGatewayInterface
{
    /**
     * Create payment using gateway payload. Return whatever underlying SDK returns (object/array).
     * @param array<string,mixed> $payload
     * @return mixed
     */
    public function createPayment(array $payload);

    /**
     * Retrieve status for gateway payment id.
     * @param string $gatewayPaymentId
     * @return mixed
     */
    public function getStatus(string $gatewayPaymentId);

    /**
     * Refund payment. $args is delegated to underlying SDK (amount in smallest unit etc.).
     * @param string $gatewayPaymentId
     * @param array<string,mixed> $args
     * @return mixed
     */
    public function refundPayment(string $gatewayPaymentId, array $args);
}
