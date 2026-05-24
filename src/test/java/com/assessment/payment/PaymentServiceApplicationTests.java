package com.assessment.payment;

import com.assessment.payment.dto.request.PaymentRequest;
import com.assessment.payment.dto.response.PaymentResponse;
import com.assessment.payment.model.enums.PaymentStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT) // Mandatory for TestRestTemplate
@EmbeddedKafka(partitions = 1, topics = { "payment.transactions" })
@DirtiesContext
public class PaymentServiceApplicationTests {

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void testSuccessfulPaymentWorkflow() {
        String id = UUID.randomUUID().toString();
        // ACC123 has 1000.00 mock balance, request for 150.00 should clear
        PaymentRequest payload = new PaymentRequest( 123, new BigDecimal(150.00));

        ResponseEntity<PaymentResponse> postResponse = restTemplate.postForEntity("/api/v1/payments", payload, PaymentResponse.class);
        assertThat(postResponse.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            ResponseEntity<PaymentResponse> queryResponse = restTemplate.getForEntity("/api/v1/payments/" + id, PaymentResponse.class);
            assertThat(queryResponse.getBody().transaction_status()).isEqualTo(PaymentStatus.PROCESSED);
        });
    }

    @Test
    void testInsufficientFundsPaymentWorkflow() {
        String id = UUID.randomUUID().toString();
        // ACC456 only has 50.00 mock balance, requesting 500.00 should trigger a validation rejection
        PaymentRequest payload = new PaymentRequest( 456, new BigDecimal(500.00));

        restTemplate.postForEntity("/payment", payload, PaymentResponse.class);

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            ResponseEntity<PaymentResponse> queryResponse = restTemplate.getForEntity("/api/v1/payments/" + id, PaymentResponse.class);
            assertThat(queryResponse.getBody().transaction_status()).isEqualTo(PaymentStatus.FAILED_INSUFFICIENT_FUNDS);
        });
    }
}
