package com.assessment.payment.controller;


import com.assessment.payment.dto.request.PaymentRequest;
import com.assessment.payment.dto.response.PaymentResponse;
import com.assessment.payment.repo.PaymentRepository;
import com.assessment.payment.service.PaymentProducer;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/payment")
@Slf4j
public class PaymentController {

    private final PaymentProducer producer;
    private final PaymentRepository repository;

    public PaymentController(PaymentProducer producer, PaymentRepository repository) {
        this.producer = producer;
        this.repository = repository;
    }
/*
    @GetMapping
    public String getStatus(){
        return "Server is up";
    }
*/
    @PostMapping
    public ResponseEntity<PaymentResponse> submitPayment(@Valid @RequestBody PaymentRequest request) {
        //String txnId = (request.transaction_id() == null || request.transaction_id().isBlank())
        //        ? UUID.randomUUID().toString() : request.transaction_id();

        Integer txnId= Math.abs(UUID.randomUUID().hashCode());
        PaymentRequest normalizedRequest = new PaymentRequest(
                 request.source_account(), request.transaction_amount());

        log.info("REST received payment execution request for ID: {}", txnId);

        // Track initial PENDING state internally before handing off to messaging layer
        PaymentResponse initialReceipt = producer.initializePaymentState(txnId, normalizedRequest);

        // Asynchronously forward to Kafka Cluster
        producer.sendPaymentEvent(txnId, normalizedRequest);

        return ResponseEntity.status(HttpStatus.ACCEPTED).body(initialReceipt);
    }
}
