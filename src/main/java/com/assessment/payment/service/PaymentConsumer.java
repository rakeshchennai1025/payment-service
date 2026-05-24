package com.assessment.payment.service;

import com.assessment.payment.dto.event.PaymentCheckEvent;
import com.assessment.payment.dto.request.PaymentRequest;
import com.assessment.payment.dto.response.PaymentResponse;
import com.assessment.payment.model.entity.Account;
import com.assessment.payment.model.entity.PaymentTransaction;
import com.assessment.payment.model.enums.PaymentStatus;
import com.assessment.payment.repo.AccountRepository;
import com.assessment.payment.repo.PaymentRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.internals.Acknowledgements;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;

@Service
@Slf4j
public class PaymentConsumer {
    private final PaymentRepository paymentRepository;
    private final AccountRepository accountRepository;
    private  String topicName="Kf_Payment_Check";
    public PaymentConsumer(PaymentRepository paymentRepository, AccountRepository accountRepository) {
        this.paymentRepository = paymentRepository;
        this.accountRepository = accountRepository;
    }

    @KafkaListener(topics = "Kf_Payment_Check", groupId = "${spring.kafka.consumer.group-id}")
    public void validateAndProcessPayment(PaymentCheckEvent event, Acknowledgment ack) {
        log.info("Async Validator picked up Transaction ID: {}", event.transaction_id());

        PaymentStatus finalStatus;

        // Business Rule Validation Engine: Look-up source account table
        Optional<Account> sourceAccountOpt = accountRepository.findByAccountNumber(event.source_account());

        if (sourceAccountOpt.isEmpty()) {
            log.warn("Validation failure: Account {} not found.", event.source_account());
            finalStatus = PaymentStatus.FAILED_ACCOUNT_NOT_FOUND;
        } else {
            Account account = sourceAccountOpt.get();
            // Check if account balance can cover the transaction amount
            if (account.getBalance().compareTo(event.transaction_amount()) >= 0) {
                log.info("Validation success: Sufficient funds available in account {}", event.source_account());

                // 1. Reduce the account balance
                BigDecimal newBalance = account.getBalance().subtract(event.transaction_amount());
                account.setBalance(newBalance);

                // 2. Persist the updated account entity back to H2 database
                accountRepository.save(account);

                log.info("Successfully deducted {} from account {}. New balance: {}",
                        event.transaction_amount(), event.source_account(), newBalance);

                finalStatus = PaymentStatus.PROCESSED;
            } else {
                log.warn("Validation failure: Insufficient funds in account {}. Available: {}, Requested: {}",
                        event.source_account(), account.getBalance(), event.transaction_amount());
                finalStatus = PaymentStatus.FAILED_INSUFFICIENT_FUNDS;
            }
        }

        // Commit final state changes back to repository store
        /*PaymentResponse finalResponse = new PaymentResponse(
                event.transaction_id(), event.source_account(),
                event.transaction_amount(), LocalDateTime.now(), finalStatus);*/

        PaymentTransaction paymentTransaction= new PaymentTransaction(                event.transaction_id(), event.source_account(),
                event.transaction_amount(), LocalDateTime.now(), finalStatus);
        paymentRepository.save(paymentTransaction);
        ack.acknowledge();
        log.info("Transaction workflow finalized for ID: {} with state: {}", event.transaction_id(), finalStatus);
    }
}
