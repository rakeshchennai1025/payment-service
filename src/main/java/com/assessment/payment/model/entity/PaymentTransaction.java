package com.assessment.payment.model.entity;

import com.assessment.payment.model.enums.PaymentStatus;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Data
@AllArgsConstructor
@NoArgsConstructor
public class PaymentTransaction {
    @Id
    @Column(name = "transaction_id", nullable = false, length = 100)
    private Integer transactionId;

    @Column(name = "source_account", nullable = false, length = 50)
    private Integer sourceAccount;

    @Column(name = "transaction_amount", nullable = false, precision = 15, scale = 2)
    private BigDecimal transactionAmount;

    @Column(name = "transaction_date", nullable = false)
    private LocalDateTime transactionDate;

    @Enumerated(EnumType.STRING)
    @Column(name = "transaction_status", nullable = false, length = 50)
    private PaymentStatus transactionStatus;

}
