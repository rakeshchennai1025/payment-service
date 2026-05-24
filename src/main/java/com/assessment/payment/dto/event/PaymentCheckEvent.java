package com.assessment.payment.dto.event;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

public record PaymentCheckEvent(
        Integer transaction_id,
        Integer source_account,
        BigDecimal transaction_amount
) {
}
