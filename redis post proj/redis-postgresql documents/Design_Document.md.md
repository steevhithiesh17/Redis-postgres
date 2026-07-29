**RELIABLE EVENT-DRIVEN**

**DATA PROCESSING SYSTEM**

Design Document

**Technology Stack**

Redis Streams

Python

PostgreSQL

SMTP Email Service

Version: 1.0

Table of Contents

1\. Introduction

The Reliable Event-Driven Data Processing System is designed to process
data asynchronously using Redis Streams and store the processed data in
PostgreSQL.

The system separates the producer of data from the database processing
system. Instead of directly inserting data into PostgreSQL, the producer
first publishes the data to a Redis Stream. A Python consumer then reads
the message, validates it, checks its execution time and priority, and
processes it into PostgreSQL.

The system is designed to provide:

-   Asynchronous processing

-   Reliable message delivery

-   Scheduled execution

-   Priority-based processing

-   Automatic retry handling

-   Failure recovery

-   Dead Letter Queue processing

-   Email notifications

-   Message acknowledgement

The primary goal is to prevent data loss and ensure that failed
operations can be retried or manually recovered.

2\. Problem Statement

In a traditional synchronous system, the producer directly communicates
with PostgreSQL. This creates several problems:

-   If PostgreSQL is temporarily unavailable, the operation fails
    immediately.

-   The producer becomes dependent on the database.

-   Failed operations may result in data loss.

-   There is no built-in retry mechanism.

-   High traffic can overload the database.

-   There is no structured failure recovery process.

To solve these problems, Redis Streams is introduced as an intermediate
message-processing layer. This architecture decouples the producer from
PostgreSQL and allows reliable asynchronous processing.

![](media/8a9672ade55f88de1a58fc9d7083ad6de2164a3a.png){width="5.0in"
height="3.2291666666666665in"}

*Figure 1: Synchronous architecture vs. event-driven architecture*

3\. System Objectives

The main objectives of the system are:

1.  To create an asynchronous data processing architecture.

2.  To use Redis Streams as a message queue.

3.  To use Python as the processing and integration layer.

4.  To store successfully processed data in PostgreSQL.

5.  To support scheduled processing using execute_at.

6.  To support message priorities.

7.  To implement automatic retries.

8.  To prevent message loss.

9.  To maintain failed messages in a Dead Letter Queue.

10. To send email notifications for success and failure.

11. To support manual reprocessing of failed messages.

12. To improve system reliability and scalability.

4\. High-Level System Architecture

The diagram below shows the complete lifecycle of a message, from
creation by the Producer through validation, scheduling, priority
checks, PostgreSQL processing, and the success or failure branches,
including retries and Dead Letter Queue handling.

![](media/10fd4c3f5d28727118a3eff60219deb05bf62266.png){width="3.125in"
height="6.895833333333333in"}

*Figure 2: High-level system architecture*

5\. System Components

5.1 Producer

The Producer is responsible for creating and publishing messages. The
Producer does not directly communicate with PostgreSQL; instead it
publishes to the Redis Stream.

Example message:

+-----------------------------------------------------------------------+
| {                                                                     |
|                                                                       |
| \"student_id\": \"STU001\",                                           |
|                                                                       |
| \"name\": \"John\",                                                   |
|                                                                       |
| \"age\": \"22\",                                                      |
|                                                                       |
| \"department\": \"Data Science\",                                     |
|                                                                       |
| \"priority\": \"HIGH\",                                               |
|                                                                       |
| \"execute_at\": \"2026-07-21 12:00:00\"                               |
|                                                                       |
| }                                                                     |
+-----------------------------------------------------------------------+

The Producer sends this message to the Redis Stream.

5.2 Redis Stream

Redis Stream acts as the message queue (example: student_stream). The
stream stores messages until they are processed by the Python Consumer.
Redis Stream provides:

-   Message ordering

-   Message IDs

-   Consumer Groups

-   Pending message tracking

-   Message acknowledgement

-   Failure recovery

The producer publishes messages using the XADD command.

5.3 Python Consumer

The Python Consumer is the main processing component. It:

13. Reads messages from Redis.

14. Validates messages.

15. Checks execute_at.

16. Checks priority.

17. Sends data to PostgreSQL.

18. Handles retries.

19. Sends email notifications.

20. Acknowledges successful messages.

21. Sends failed messages to the DLQ.

The Consumer uses Redis Consumer Groups for reliable processing.

5.4 PostgreSQL

PostgreSQL is the final persistent database. After successful
processing, the Python Consumer inserts or updates the data in
PostgreSQL. PostgreSQL acts as the system of record.

5.5 Email Notification Service

The email service sends notifications on both successful processing and
on final failure after the retry limit is reached:

-   **Success:** Message Processed → Email Sent

-   **Failure:** Maximum Retries Reached → Failure Email Sent

5.6 Dead Letter Queue

The Dead Letter Queue stores messages that cannot be processed
successfully after the configured retry limit.

![](media/515977b853da9fc20587669e9a1f96fa14f17846.png){width="2.6979166666666665in"
height="7.083333333333333in"}

*Figure 3: Dead Letter Queue escalation path*

The DLQ allows developers to:

-   Review the failed message.

-   Identify the root cause.

-   Correct the issue.

-   Reprocess the message.

6\. End-to-End Data Flow

The table below summarizes each step of processing a message from
creation to database write.

  -----------------------------------------------------------------------
  **Step**       **Description**                    **Example**
  -------------- ---------------------------------- ---------------------
  1\. Producer   The producer creates the student   Student ID: STU001,
  creates data   record to be processed.            Name: John, Dept:
                                                    Data Science,
                                                    Priority: HIGH

  2\. Message    The producer publishes the message 1753123456789-0
  added to Redis to student_stream using XADD;      
                 Redis assigns a unique message ID. 

  3\. Consumer   The Python Consumer reads the      Redis Stream →
  reads the      message through a Redis Consumer   Consumer Group →
  message        Group.                             Python Consumer

  4\. Message    The Consumer checks that required  student_id, name,
  validation     fields and values are present and  priority, execute_at
                 valid.                             

  5\. Execute    The system compares the current    Current 10:00 AM,
  time check     time to execute_at; the message    execute_at 12:00 PM →
                 waits if the time has not been     Wait
                 reached.                           

  6\. Priority   Messages are ordered according to  Message A = HIGH,
  check          HIGH, MEDIUM, or LOW priority.     Message B = LOW

  7\. PostgreSQL The Consumer performs an INSERT or Python → PostgreSQL →
  processing     UPDATE in PostgreSQL.              Data Stored
  -----------------------------------------------------------------------

![](media/25c8dae1d223a416207d06da890f55a8d2ed9924.png){width="4.166666666666667in"
height="6.1875in"}

*Figure 4: End-to-end message processing flow*

7\. Success Flow

If PostgreSQL successfully processes the message, the Consumer sends a
confirmation email, acknowledges the message with XACK, and removes it
from the stream with XDEL.

![](media/e62274af5f56056207c2035286b87e0689dd5d43.png){width="2.7708333333333335in"
height="7.083333333333333in"}

*Figure 5: Success flow*

The message is considered successfully processed.

8\. Failure Flow

If PostgreSQL processing fails, the system retries the operation. If the
third attempt also fails, the message is moved to the Dead Letter Queue
and a failure email is sent.

![](media/d61448f2982f74eb41b41407f6a2c2b2130535c0.png){width="5.0in"
height="5.166666666666667in"}

*Figure 6: Failure and retry escalation to the Dead Letter Queue*

9\. Retry Design

The system uses a maximum retry count:

  -----------------------------------------------------------------------
  MAX_RETRIES = 3

  -----------------------------------------------------------------------

Each attempt either completes successfully or falls through to the next
attempt. After the third failed attempt, the message is routed to the
Dead Letter Queue (see Figure 6).

10\. Dead Letter Queue Design

The Dead Letter Queue record contains:

-   Original message

-   Message ID

-   Error reason

-   Retry count

-   Failure timestamp

Example:

+-----------------------------------------------------------------------+
| {                                                                     |
|                                                                       |
| \"original_message_id\": \"1753123456789-0\",                         |
|                                                                       |
| \"student_id\": \"STU001\",                                           |
|                                                                       |
| \"retry_count\": \"3\",                                               |
|                                                                       |
| \"error\": \"PostgreSQL connection failed\",                          |
|                                                                       |
| \"failed_at\": \"2026-07-21 12:30:00\"                                |
|                                                                       |
| }                                                                     |
+-----------------------------------------------------------------------+

11\. Message Acknowledgement

The system uses Redis acknowledgement. A message should be acknowledged
only after successful processing. If the Consumer crashes before XACK,
the message remains pending rather than being lost.

![](media/b3315f107927552270ab32cdb4e72dc3315d70d3.png){width="5.208333333333333in"
height="4.0in"}

*Figure 7: Message acknowledgement --- normal path vs. consumer crash
path*

12\. Failure Scenarios

  -----------------------------------------------------------------------
  **Failure**                         **System Response**
  ----------------------------------- -----------------------------------
  PostgreSQL unavailable              Retry processing

  Database timeout                    Retry processing

  Invalid message                     Move to DLQ

  Python Consumer crashes             Message remains pending

  Redis connection fails              Reconnect

  Email fails                         Log failure and continue according
                                      to policy

  Three retries fail                  Move to DLQ
  -----------------------------------------------------------------------

13\. Security Design

The system must follow these security practices:

-   Do not hardcode passwords.

-   Store credentials in environment variables.

-   Do not commit .env files to GitHub.

-   Use secure database credentials.

-   Use SMTP App Passwords where required.

-   Restrict Redis access in production.

-   Restrict PostgreSQL access in production.

-   Do not expose passwords in logs.

-   Use TLS for production communication where supported.

Example (.env):

+-----------------------------------------------------------------------+
| POSTGRES_PASSWORD=\*\*\*\*\*\*\*\*                                    |
|                                                                       |
| SMTP_PASSWORD=\*\*\*\*\*\*\*\*                                        |
+-----------------------------------------------------------------------+

14\. Scalability

The system can be scaled by running multiple Consumers that belong to
the same Consumer Group, allowing parallel processing, higher
throughput, and better availability.

![](media/c44c7d6d7a17b278d22c1ceb6b2cc861559278e3.png){width="5.208333333333333in"
height="2.15625in"}

*Figure 8: Horizontal scaling with multiple consumers in one Consumer
Group*

15\. Future Enhancements

Future improvements may include:

-   Redis Sentinel

-   Redis Cluster

-   PostgreSQL replication

-   Prometheus monitoring

-   Grafana dashboards

-   Distributed tracing

-   API-based producers

-   Web-based DLQ management

-   Advanced priority scheduling

-   Exponential backoff

-   Automatic dead-letter reprocessing

-   Docker Compose deployment

16\. Conclusion

The Reliable Event-Driven Data Processing System provides a robust
architecture for processing data between producers and PostgreSQL.

The use of Redis Streams provides asynchronous communication and
reliable message handling. Python acts as the processing layer, while
PostgreSQL provides permanent structured storage.

The retry mechanism and Dead Letter Queue ensure that temporary and
permanent failures are handled systematically.

The final architecture is summarized below:

![](media/45d32784dd0d7dfef235141d29a7741495d25a06.png){width="5.208333333333333in"
height="5.229166666666667in"}

*Figure 9: Final system architecture summary*
