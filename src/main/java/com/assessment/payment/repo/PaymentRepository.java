package com.assessment.payment.repo;

import com.assessment.payment.model.entity.PaymentTransaction;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface PaymentRepository extends JpaRepository<PaymentTransaction, Integer> {
}
