# Payment Service - Spring Boot REST API + Kafka

## Overview
Here is a comprehensive, production-ready README.md file designed for your GitHub repositories. 
It bridges your payment-service and a simulated account-service using the exact architectural constraints, REST APIs, H2 database, and Kafka transactional event pattern you requested.

## Design Pattern
This Spring Boot project primarily uses the Transactional Outbox / Dual-Write Pattern to reliably save payment records to the H2 database while simultaneously publishing transaction events to Apache Kafka. 
It also utilizes the Publisher-Subscriber (Pub-Sub) Pattern via Kafka to asynchronously broadcast those payment events, ensuring loose coupling and high throughput for downstream services

`payment-service` is a Spring Boot REST API project developed for handling payment processing operations.

The project demonstrates:
- REST API development using Spring Boot
- CRUD operations for payments
- H2 database integration
- Maven-based project structure
- Layered architecture
- Kafka integration through a separate event-driven project


## Payment & Account Processing Service
This project comprises microservices (payment-service and Kf_Payment_Check) built using Spring Boot. 
It demonstrates a synchronous REST-driven integration combined with an asynchronous event-driven pattern using Apache Kafka to post payments, 
fetch payment statuses, and stream transaction updates.

## Architecture Overview
The system is designed around a hybrid synchronous/asynchronous choreography. 
It ensures that while the initial validation and persistence happen in near-real-time via REST, 
downstream systems (like auditing, notifications, or analytics) are fed asynchronously via Kafka.


				  +-----------------------------------+
                  |        Client / Postman           |
                  +-----------------+-----------------+
                                    |
                       (1) POST / GET Payment REST
                                    v
                  +-----------------+-----------------+
                  |         payment-service           |
                  |  (Spring Boot / Embedded H2 DB)   |
                  +--------+-----------------+--------+
                           |                 |
  (2) Synchronous REST API |                 | (3) Asynchronous Event
  Balance Validation       |                 |     "payment-events" topic
                           v                 v
+--------------------------+----+   +--------+------------------------+
|        account-service         |   |          Apache Kafka           |
| (Simulated Remote Endpoint)    |   |     (Kf_Payment_Check Consumer) |
+--------------------------------+   +--------------------------------+

## Architectural Workflow
Initiate Payment: A client sends a POST /payment request to the payment-service.

Account Verification: The payment-service fires a synchronous REST call to the account-service to check balance availability and account status.

Local Persistence: Upon successful validation, the payment is saved into the local H2 In-Memory Database with a status of PENDING or SUCCESS.

Event Streaming: Immediately after the database transaction commits, a payment transaction event is produced to the Kafka topic (payment-events).

Downstream Consumption: The Kf_Payment_Check module consumes these events to execute transaction verification rules or post-processing logs.


## GitHub Repository

Main Repository:

https://github.com/rakeshchennai1025/payment-service

Related Kafka Project:

https://github.com/rakeshchennai1025/Kf_Payment_Check

---

# Tech Stack

| Technology | Purpose |
|---|---|
| Java 17 | Programming Language |
| Spring Boot 3.x (Spring Web, Spring Data JPA) |
| Spring Web | REST API Development |
| H2 Database (In-Memory database for local development) | Lightweight Embedded Database |
| Maven | Build & Dependency Management |
| Apache Kafka (Distributed event streaming platform) |

---

# Project Structure
				
```text					
payment/
├── README.md
├── pom.xml
├── .mvn/
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com/
│   │   │       └── assessment/
│   │   │           └── payment/
│   │   │               ├── config/
│   │   │               │   └── PaymentValidatorConfig.java			# Configuration class used to define payment validation-related beans and application configurations.
│   │   │               ├── controller/
│   │   │               │   └── PaymentController.java				# REST controller exposing APIs for creating, retrieving, and updating payment transactions.
│   │   │               ├── dto/
│   │   │               │   ├── event/
│   │   │               │   │   └── PaymentCheckEvent.java			# DTO representing Kafka/event-driven payment validation or payment check event details.
│   │   │               │   ├── request/
│   │   │               │   │   └── PaymentRequest.java				# Request DTO used to receive payment input data from client/API requests.
│   │   │               │   └── response/
│   │   │               │       └── PaymentResponse.java			# Response DTO used to send payment transaction details back to API clients.
│   │   │               ├── model/
│   │   │               │   ├── entity/
│   │   │               │   │   ├── Account.java					# Entity class representing account information stored in the database.
│   │   │               │   │   └── PaymentTransaction.java			# Entity class representing payment transaction details persisted in the database.
│   │   │               │   └── enums/
│   │   │               │       └── PaymentStatus.java				# Enum defining possible payment transaction statuses such as SUCCESS, FAILED, or PENDING.
│   │   │               ├── repo/
│   │   │               │   ├── AccountRepository.java				# JPA repository interface used for performing database operations on Account entities.
│   │   │               │   └── PaymentRepository.java				# JPA repository interface used for CRUD and database operations on PaymentTransaction entities.
│   │   │               ├── service/
│   │   │               	└── PaymentApplication.java				# Main Spring Boot application class used to bootstrap and start the payment service application.
│   │   └── resources/
│   │       ├── static/
│   │       ├── templates/
│   │       └── application.yaml									# App configurations (Port, H2 DB, Kafka Brokers)
│   └── test/
│       └── java/
│           └── com/
│               └── assessment/
│                   └── payment										# Unit & Integration Tests (JUnit/Mockito)
						├── PaymentApplicationTests.java
						└── PaymentServiceApplicationTests.java
```

## Design Decisions & Trade-offs
1. Hybrid REST + Kafka Architecture
Decision: Use synchronous REST for account validation, but asynchronous Kafka events for payment completion streaming.

Trade-off: Making a synchronous call to the account-service blocks the payment thread during network latency. 
However, this ensures strong consistency—preventing a payment from being generated if the account does not exist or has insufficient funds. 
Downstream processes use Kafka for high-throughput eventual consistency.

2. Embedded H2 Database
Decision: Using H2 in mem mode for local service execution.

Trade-off: Fast setup and zero-dependency configuration for development. 
However, data is non-persistent and will clear whenever the Spring Boot application restarts.

3. Kafka Producer Acknowledgments (acks=all)
Decision: Configured the Kafka producer with acks=all (or acks=1 based on critical balance).

Trade-off: It slightly increases latency per payment post since the broker waits for replica acknowledgments, 
but it completely eliminates the risk of missing transaction events (guaranteeing at least-once delivery).


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
POST /payment
```

### Sample Request Body

```json
{
    "source_account": 456,
    "transaction_amount": 10
}
```

### Sample Response

```json
{
    "transaction_id": 1770297312,
    "source_account": 456,
    "transaction_amount": 10,
    "transaction_date": "2026-05-26T15:00:25.8591835",
    "transaction_status": "PENDING"
}
```

---

## Get Payment By ID

### Request

```http
GET /payment/{id}
```

### Example

```http
GET /payment/1770297312
```

### Sample Response

```json
{
    "transactionId": 1770297312,
    "sourceAccount": 456,
    "transactionAmount": 10.00,
    "transactionDate": "2026-05-26T15:00:26.850545",
    "transactionStatus": "PROCESSED"
}
```


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



# Apache Kafka Multi-Broker Setup (Windows)
This guide explains how to set up a local Apache Kafka cluster on Windows using:
- 1 ZooKeeper instance
- 2 Kafka Brokers
- 1 Kafka Topic
- 4 Partitions
- Replication Factor = 2

# Pre-Requisites
Kafka Installation Path:

```text
D:\Rakesh\kafka_2.13-3.9.2
```


# One-Time Broker Configuration
Navigate to:
```text
D:\Rakesh\kafka_2.13-3.9.2\config
```

Copy:
```text
server.properties
```

Rename the copied file to:
```text
server2.properties
```

Update the following properties inside `server2.properties`:
```properties
broker.id=1
listeners=PLAINTEXT://:9093
log.dirs=/tmp/kafka-logs2
```


# Step 1 - Start ZooKeeper
Open Command Prompt as Administrator.
Run:
```dos
d:
cd D:\Rakesh\kafka_2.13-3.9.2\bin\windows
zookeeper-server-start.bat ../../config/zookeeper.properties
```

# Step 2 - Start Kafka Broker 1
Open a new Command Prompt as Administrator.
Run:
```dos
d:
cd D:\Rakesh\kafka_2.13-3.9.2\bin\windows
kafka-server-start.bat ../../config/server.properties
```

Broker 1 runs on:
```text
localhost:9092
```

# Step 3 - Start Kafka Broker 2
Open another new Command Prompt as Administrator.
Run:
```dos
d:
cd D:\Rakesh\kafka_2.13-3.9.2\bin\windows
kafka-server-start.bat ../../config/server2.properties
```

Broker 2 runs on:
```text
localhost:9093
```

# Step 4 - Create Kafka Topic
Create topic:
```text
Kf_Payment_Check
```
with:
- 4 partitions
- replication factor = 2
Run:
```dos
d:
cd D:\Rakesh\kafka_2.13-3.9.2\bin\windows

kafka-topics.bat --create --topic Kf_Payment_Check --bootstrap-server localhost:9092 --partitions 4 --replication-factor 2
```


# Verification Commands

## List Available Topics

```dos
kafka-topics.bat --list --bootstrap-server localhost:9092
```


## Describe Topic Details

Verify:
- partitions
- replication
- broker assignments

```dos
kafka-topics.bat --describe --topic Kf_Payment_Check --bootstrap-server localhost:9092
```


# Troubleshooting & Hard Reset
If Kafka enters inconsistent state, lock issues, or startup failures:

## Stop All Kafka & ZooKeeper Processes
```dos
taskkill /F /IM java.exe
```

## Delete Temporary Kafka Log Directories
Delete the following folders manually:
```text
D:\tmp\kafka-logs
D:\tmp\kafka-logs2
D:\tmp\zookeeper
```
(Delete only if configured inside `zookeeper.properties`)

---

# Kafka Cluster Summary
| Component | Port |
|---|---|
| ZooKeeper | 2181 |
| Kafka Broker 1 | 9092 |
| Kafka Broker 2 | 9093 |
---

# Topic Configuration
| Property | Value |
|---|---|
| Topic Name | Kf_Payment_Check |
| Partitions | 4 |
| Replication Factor | 2 |
---

# Notes
- Open separate Command Prompts for ZooKeeper and each Broker.
- Always start ZooKeeper before Kafka Brokers.
- Start Broker 1 before Broker 2.
- Ensure ports 9092 and 9093 are available.
- Use replication factor 2 only when both brokers are running.




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

