package com.assessment.payment.dto.response;



import com.assessment.payment.model.enums.PaymentStatus;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record PaymentResponse(
        Integer transaction_id,
        Integer source_account,
        BigDecimal transaction_amount,
        LocalDateTime transaction_date,
        PaymentStatus transaction_status
) {
}
