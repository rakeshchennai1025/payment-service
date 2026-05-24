package com.assessment.payment.dto.request;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

public record PaymentRequest(
        //Integer transaction_id,
        @NotNull(message = "Source account is required")
        Integer source_account,
        @Positive(message = "Transaction amount must be greater than zero")
        BigDecimal transaction_amount
) {
}
