# Redis Stream Priority Scheduler

A small Python project that listens to a Redis Stream and schedules tasks into PostgreSQL based on a priority/execute_at schedule. After successful processing, the project sends a notification email.

Key behaviours:
- Read messages from a Redis stream (consumer group).
- Determine if a message is due (execute_at/time) and sort by priority.
- Write processed rows to PostgreSQL.
- Send a success email and acknowledge/delete the Redis message.
- Retry failed messages using Redis pending/xautoclaim semantics.

# Redis Stream to PostgreSQL Data Processing System

## Overview
This project implements an event-driven data processing pipeline using **Redis Streams** and **PostgreSQL**. Producers publish messages to Redis Streams, while Python consumers validate, schedule, retry, and insert the data into PostgreSQL.

## Architecture
Producer → Redis Stream → Consumer Group → Python Consumer → Validation → Priority/execute_at Check → PostgreSQL → Email → ACK

## Features
- Redis Streams
- Consumer Groups
- PostgreSQL integration
- Retry mechanism
- Dead Letter Queue (DLQ)
- Priority-based processing
- Scheduled execution (`execute_at`)
- Email notifications
- ACK handling
- Pending message recovery

## Streams
- `student_stream`
- `employee_stream`

Each stream has its own consumer group for independent processing.

## Workflow
1. Producer publishes a message.
2. Redis stores it in a stream.
3. Consumer reads using XREADGROUP.
4. Message is validated.
5. Priority and execute_at are checked.
6. Data is inserted into PostgreSQL.
7. Success email is sent.
8. Message is acknowledged (XACK).

## Retry & DLQ
If processing fails:
- Retry up to the configured limit.
- On repeated failure, move the message to the Dead Letter Queue (DLQ).

## Technologies
- Python
- Redis
- PostgreSQL
- psycopg2
- redis-py

## Conclusion
The project provides a scalable, reliable, asynchronous processing pipeline using Redis Streams and PostgreSQL.
