package com.assessment.payment.config;

import com.assessment.payment.model.entity.Account;
import com.assessment.payment.repo.AccountRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.math.BigDecimal;

@Configuration
@Slf4j
public class PaymentValidatorConfig {
    @Bean
    CommandLineRunner seedDatabase(AccountRepository accountRepository) {
        return args -> {
            log.info("Pre-seeding mock accounts into H2 Database...");

            accountRepository.save(new Account(123, new BigDecimal("1000.00")));
            accountRepository.save(new Account(456, new BigDecimal("500.00")));
            accountRepository.save(new Account(234, new BigDecimal("0.00")));
            accountRepository.save(new Account(789, new BigDecimal("200.00")));
            accountRepository.save(new Account(567, new BigDecimal("50.00")));


            log.info("Database pre-seeding complete. Accounts ready for lookup validation.");
        };
    }
}
