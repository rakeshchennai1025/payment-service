# Payment Service - Spring Boot REST API

## Overview

`payment-service` is a Spring Boot REST API project developed for handling payment processing operations.

The project demonstrates:
- REST API development using Spring Boot
- CRUD operations for payments
- H2 database integration
- Maven-based project structure
- Layered architecture
- Kafka integration through a separate event-driven project

---

## GitHub Repository

Main Repository:

https://github.com/rakeshchennai1025/payment-service

Related Kafka Project:

https://github.com/rakeshchennai1025/Kf_Payment_Check

---

# Technologies Used

| Technology | Purpose |
|---|---|
| Java 17 | Programming Language |
| Spring Boot | Application Framework |
| Spring Web | REST API Development |
| Spring Data JPA | Database Interaction |
| H2 Database | Lightweight Embedded Database |
| Maven | Build & Dependency Management |
| Kafka | Event-Driven Messaging (Separate Project) |

---

# Project Structure

```text
payment-service
 ├── src/
 │    ├── main/
 │    │    ├── java/
 │    │    └── resources/
 │    └── test/
 ├── pom.xml
 ├── mvnw
 ├── README.md
```

---

# Features Implemented

- Payment creation API
- Payment retrieval API
- Payment status update
- H2 database persistence
- RESTful service architecture
- Maven project setup
- Spring Boot configuration
- YAML configuration support

---

# API Endpoints

## Create Payment

### Request

```http
POST /payments
```

### Sample Request Body

```json
{
  "payerName": "Rakesh",
  "amount": 1000,
  "status": "SUCCESS"
}
```

### Sample Response

```json
{
  "paymentId": 1,
  "payerName": "Rakesh",
  "amount": 1000,
  "status": "SUCCESS"
}
```

---

## Get Payment By ID

### Request

```http
GET /payments/{id}
```

### Example

```http
GET /payments/1
```

---

## Update Payment Status

### Request

```http
PUT /payments/{id}
```

### Sample Request Body

```json
{
  "status": "COMPLETED"
}
```

---

# Database Configuration

## H2 Console

```text
http://localhost:8080/h2-console
```

## Example Configuration

| Property | Value |
|---|---|
| JDBC URL | url: jdbc:h2:mem:payment |
| Username | payment |
| Password |  |

---

# How To Run The Project

## Clone Repository

```bash
git clone https://github.com/rakeshchennai1025/payment-service.git
```

---

## Navigate To Project

```bash
cd payment-service
```

---

## Build The Project

```bash
mvn clean install
```

---

## Run Spring Boot Application

```bash
mvn spring-boot:run
```

---

# Kafka Integration Project

Kafka-related payment validation and asynchronous processing logic are implemented in a separate repository:

https://github.com/rakeshchennai1025/Kf_Payment_Check

## Kafka Project Highlights

- Kafka Producer implementation
- Kafka Consumer implementation
- Payment validation/check processing
- Event-driven architecture
- Spring Boot + Kafka integration
- Asynchronous messaging workflow

---

# Future Enhancements

- JWT Authentication
- Swagger/OpenAPI Documentation
- Docker Containerization
- Unit Testing & Integration Testing
- Microservices Communication
- API Gateway Integration

---

# Git Commands Used For Daily Development

```bash
cd /d/Rakesh/SpringWorks/payment

git status

git add .

git commit -m "Updated payment logic"

git push
```

---

# Author

## Rakesh K.M

Backend development learning repository focusing on:
- Spring Boot
- Kafka
- REST APIs
- Database integration
- Enterprise backend architecture
