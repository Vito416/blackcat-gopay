<?php

declare(strict_types=1);

namespace BlackCat\GoPay;

enum GoPayStatus: string
{
    case CREATED = 'CREATED';
    case PAYMENT_METHOD_CHOSEN = 'PAYMENT_METHOD_CHOSEN';
    case PAID = 'PAID';
    case AUTHORIZED = 'AUTHORIZED';
    case CANCELED = 'CANCELED';
    case TIMEOUTED = 'TIMEOUTED';
    case REFUNDED = 'REFUNDED';
    case PARTIALLY_REFUNDED = 'PARTIALLY_REFUNDED';

    public function isNonPermanent(): bool
    {
        return in_array($this, [self::CREATED, self::PAYMENT_METHOD_CHOSEN], true);
    }
}

