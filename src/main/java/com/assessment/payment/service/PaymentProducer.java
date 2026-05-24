package com.assessment.payment.service;

import com.assessment.payment.dto.event.PaymentCheckEvent;
import com.assessment.payment.dto.request.PaymentRequest;
import com.assessment.payment.dto.response.PaymentResponse;
import com.assessment.payment.model.entity.PaymentTransaction;
import com.assessment.payment.model.enums.PaymentStatus;
import com.assessment.payment.repo.PaymentRepository;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
@Slf4j
public class PaymentProducer {

    private final KafkaTemplate<String, PaymentCheckEvent> kafkaTemplate;
    private final PaymentRepository repository;
    //@Value("${app.kafka.topic-name}")
    private  String topicName="Kf_Payment_Check";

    public PaymentProducer(KafkaTemplate<String, PaymentCheckEvent> kafkaTemplate,
                           PaymentRepository repository
                          ) {
        this.kafkaTemplate = kafkaTemplate;
        this.repository = repository;
        //this.topicName = topicName;
    }

    public PaymentResponse initializePaymentState(Integer transactionId, PaymentRequest request) {
        PaymentTransaction pendingPayment;
        pendingPayment = new PaymentTransaction(transactionId, request.source_account(), request.transaction_amount(),LocalDateTime.now(), PaymentStatus.PENDING);
        repository.save(pendingPayment);
        PaymentResponse paymentResponse;

        paymentResponse = new PaymentResponse(transactionId, request.source_account(), request.transaction_amount(),
                LocalDateTime.now(), PaymentStatus.PENDING);
        return paymentResponse;
    }

    public void sendPaymentEvent(Integer transactionId, PaymentRequest request) {
        log.info("Routing payment event payload downstream to topic: {}", topicName);

        // source_account used as routing key ensures sequence integrity per account
        PaymentCheckEvent paymentCheckEvent= new PaymentCheckEvent(transactionId,request.source_account(),request.transaction_amount());
        kafkaTemplate.send(topicName, String.valueOf(paymentCheckEvent.transaction_id()), paymentCheckEvent)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Kafka delivery failure notice: {}", ex.getMessage());
                    } else {
                        log.info("Payment event acknowledged on partition: {}", result.getRecordMetadata().partition());
                    }
                });
    }

}
