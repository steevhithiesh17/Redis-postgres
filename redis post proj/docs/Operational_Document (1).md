**OPERATIONAL DOCUMENT**

**Redis Streams-Based Asynchronous Data Processing and Qdrant Semantic
Search System**

*Version 1.0*

Prepared for: Project Guide / Engineering Team

July 2026

**Table of Contents**

1\. Document Purpose

2\. System Overview

3\. Complete System Architecture

4\. Components and Responsibilities

5\. Redis Stream Structure

6\. Redis Consumer Groups

7\. Complete Document Processing Operation

8\. Complete Validation and Failure Flow

9\. Retry Mechanism

10\. Dead Letter Queue

11\. Email Notification Operations

12\. Qdrant Vector Database Operations

13\. Semantic Search Operation

14\. Search Result Output

15\. Docker Deployment

16\. Service Health Checks

17\. Operational Monitoring

18\. Troubleshooting Guide

19\. Complete Operational Flowchart

20\. Complete Search Flowchart

21\. Operational Runbook

22\. Operational Best Practices

23\. Final End-to-End System Summary

**1. Document Purpose**

This document describes the operational behavior of a distributed,
asynchronous data processing and semantic search system built on Redis
Streams and the Qdrant vector database. It is intended for project
guides, managers, developers, and technical operators who need to
understand how the system runs, how it is monitored, and how it recovers
from failure.

**Core Components**

-   Redis Streams as the message-processing layer

-   Redis Consumer Groups for reliable message consumption

-   Python consumers for processing business logic

-   A Sentence Transformer embedding model for converting text into
    vectors

-   Qdrant as the vector database

-   Redis Streams for issuing search commands

-   Qdrant similarity search for semantic search

-   Email notifications for success and permanent failure

-   Retry logic with exponential backoff and Dead Letter Queues for
    failure handling

-   Docker containers for Redis and Qdrant deployment

-   SSH-based deployment on a remote production server

**This Document Focuses On**

-   Starting the system

-   Processing data end-to-end

-   Monitoring running services

-   Validation of incoming messages

-   Scheduling via execute_at

-   Priority handling (HIGH / MEDIUM / LOW)

-   Retry handling and exponential backoff

-   Dead Letter Queue (DLQ) handling

-   Semantic search operations

-   Failure recovery

-   Service health checks

-   Day-to-day operational commands

**2. System Overview**

The system implements two major, loosely coupled workflows: a
data-ingestion and vector-storage pipeline, and a semantic-search
pipeline. Both pipelines share the same Redis and Qdrant infrastructure
but operate through independent streams and consumer groups so that a
slowdown in one workflow does not block the other.

Data ingestion is the process of accepting a raw record (a document,
student, or employee entry), validating it, and durably storing it.
Vector storage is the specific step within ingestion where the record\'s
text is converted into a numerical embedding and persisted in Qdrant
alongside its metadata. Semantic search is a separate, read-only
workflow that converts a query into the same embedding space and
retrieves the most similar previously stored vectors, rather than
performing exact keyword matching.

**A. Data Ingestion and Vector Storage --- Flow**

  -----------------------------------------------------------------------
  **Producer**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Redis Stream**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Redis Consumer Group**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Python Consumer**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Validation**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Priority Check**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **execute_at Check**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Embedding Generation**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Qdrant Vector Storage**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Success Email**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Redis XACK**

  -----------------------------------------------------------------------

*Figure 2A --- End-to-end path of a message from creation to
acknowledgement.*

**B. Semantic Search --- Flow**

  -----------------------------------------------------------------------
  **Search Query**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **search_stream**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Search Consumer**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Query Embedding**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Qdrant Similarity Search**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Top-K Results**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Result Stream / Terminal Output**

  -----------------------------------------------------------------------

*Figure 2B --- Read-only semantic search path, independent of the
ingestion pipeline.*

**3. Complete System Architecture**

The diagram below shows the full path a message takes from creation
through to storage or failure handling.

**Complete System Architecture Diagram**

  ----------------- ----------------- ----------------- -----------------
  **Document        **Student         **Employee        **Search
  Producer**        Producer**        Producer**        Producer**

  ----------------- ----------------- ----------------- -----------------

**↓**

  --------------------- -------------------- --------------------- -------------------
  **document_stream**   **student_stream**   **employee_stream**   **search_stream**

  --------------------- -------------------- --------------------- -------------------

**↓**

  -------------------- ------------------- -------------------- ------------------
  **document_group**   **student_group**   **employee_group**   **search_group**

  -------------------- ------------------- -------------------- ------------------

**↓**

  ----------------------------------- -----------------------------------
  **consumer.py**                     **search_consumer.py**

  ----------------------------------- -----------------------------------

**↓**

  -----------------------------------------------------------------------
  **Validation**

  -----------------------------------------------------------------------

**↓**

+-----------------------------------+-----------------------------------+
| **VALID**                         | **INVALID**                       |
|                                   |                                   |
| **↓**                             | **↓**                             |
|                                   |                                   |
|   ------------------------------  |   ------------------------------  |
|   **Priority Check**              |   **Retry Handler**               |
|                                   |                                   |
|   ------------------------------  |   ------------------------------  |
|                                   |                                   |
| **↓**                             | **↓**                             |
|                                   |                                   |
|   ------------------------------  |   ------------------------------  |
|   **execute_at Check**            |   **Retry Limit Check**           |
|                                   |                                   |
|   ------------------------------  |   ------------------------------  |
|                                   |                                   |
| **↓**                             | **↓** *exceeded*                  |
|                                   |                                   |
|   ------------------------------  |   ------------------------------  |
|   **Embedding Generation**        |   **DLQ**                         |
|                                   |                                   |
|   ------------------------------  |   ------------------------------  |
|                                   |                                   |
| **↓**                             | **↓**                             |
|                                   |                                   |
|   ------------------------------  |   ------------------------------  |
|   **Qdrant Vector Storage**       |   **Developer Review**            |
|                                   |                                   |
|   ------------------------------  |   ------------------------------  |
|                                   |                                   |
| **↓**                             |                                   |
|                                   |                                   |
|   ------------------------------  |                                   |
|   **Success Email**               |                                   |
|                                   |                                   |
|   ------------------------------  |                                   |
|                                   |                                   |
| **↓**                             |                                   |
|                                   |                                   |
|   ------------------------------  |                                   |
|   **Redis XACK**                  |                                   |
|                                   |                                   |
|   ------------------------------  |                                   |
+-----------------------------------+-----------------------------------+

*Figure 3 --- Producers, streams, consumer groups, and consumers feed a
shared validation stage that branches into the success path or the
retry/DLQ path.*

Every producer writes into a dedicated Redis Stream. Each stream is read
by its own consumer group, so document, student, employee, and search
traffic are isolated from one another. A single Python consumer process
(consumer.py) can be launched per stream type, and search_consumer.py
handles only search commands.

**4. Components and Responsibilities**

  -----------------------------------------------------------------------
  **Component**            **Responsibility**
  ------------------------ ----------------------------------------------
  Producer                 Creates and publishes messages

  Redis Streams            Stores asynchronous messages

  Redis Consumer Groups    Distributes messages reliably

  Python Consumer          Processes messages

  Validation Module        Validates required fields

  Priority Logic           Handles HIGH, MEDIUM, LOW priority

  Scheduling Logic         Handles execute_at

  Embedding Service        Converts text into vectors

  Embedding Model          Generates numerical representation

  Qdrant                   Stores and searches vectors

  Retry Handler            Handles temporary failures

  DLQ Handler              Stores permanently failed messages

  Email Service            Sends success/failure notifications

  Search Consumer          Processes search queries

  Docker                   Runs infrastructure services
  -----------------------------------------------------------------------

**5. Redis Stream Structure**

**document_stream**

Used for document ingestion. Each entry carries the following fields:

+-----------------------------------------------------------------------+
| document_id                                                           |
|                                                                       |
| title                                                                 |
|                                                                       |
| text                                                                  |
|                                                                       |
| priority                                                              |
|                                                                       |
| retry_count                                                           |
|                                                                       |
| execute_at (optional)                                                 |
+-----------------------------------------------------------------------+

Example command:

+-----------------------------------------------------------------------+
| XADD document_stream \* \\                                            |
|                                                                       |
| document_id DOC3001 \\                                                |
|                                                                       |
| title \"Redis Streams\" \\                                            |
|                                                                       |
| text \"Redis Streams allow applications to send and process messages  |
| asynchronously.\" \\                                                  |
|                                                                       |
| priority HIGH \\                                                      |
|                                                                       |
| retry_count 0                                                         |
+-----------------------------------------------------------------------+

**student_stream**

Used for student-related data processing. The student consumer processes
student messages independently of the document and employee pipelines,
using its own consumer group and retry/DLQ state.

**employee_stream**

Used for employee-related data processing. Employee messages are
processed independently using a separate consumer group, so a backlog in
the employee stream does not affect document or student processing.

**search_stream**

Used to submit semantic search commands.

+-----------------------------------------------------------------------+
| XADD search_stream \* \\                                              |
|                                                                       |
| command search \\                                                     |
|                                                                       |
| query \"What are Redis Streams?\" \\                                  |
|                                                                       |
| top_k 5                                                               |
+-----------------------------------------------------------------------+

The search consumer reads this command and performs a semantic
similarity search against the vectors stored in Qdrant.

**6. Redis Consumer Groups**

+-----------------------------------------------------------------------+
| document_group                                                        |
|                                                                       |
| student_group                                                         |
|                                                                       |
| employee_group                                                        |
|                                                                       |
| search_group                                                          |
+-----------------------------------------------------------------------+

Consumer groups are used so that multiple consumer instances can share
the work of a single stream while Redis guarantees that each message is
delivered to only one consumer within the group. This provides
at-least-once delivery: a message is only considered fully handled once
it has been explicitly acknowledged.

**Key Mechanics**

-   XREADGROUP reads new (or previously undelivered) entries on behalf
    of a named consumer within a group.

-   Redis tracks a Pending Entries List (PEL) of messages that have been
    delivered but not yet acknowledged.

-   XACK removes a message from the PEL once processing has completed
    successfully.

-   Acknowledgements matter because they are what allows Redis to know a
    message is safe to forget; without them, a message will remain
    pending indefinitely.

-   If a consumer crashes before acknowledging a message, the message
    stays in the PEL and can be claimed and reprocessed by another
    consumer (for example using XCLAIM or XAUTOCLAIM) after its idle
    time threshold is exceeded.

**Operational Commands**

+-----------------------------------------------------------------------+
| XINFO GROUPS document_stream                                          |
|                                                                       |
| XINFO GROUPS student_stream                                           |
|                                                                       |
| XINFO GROUPS employee_stream                                          |
|                                                                       |
| XINFO GROUPS search_stream                                            |
+-----------------------------------------------------------------------+

**Pending Message Monitoring**

+-----------------------------------------------------------------------+
| XPENDING document_stream document_group                               |
|                                                                       |
| XPENDING employee_stream employee_group                               |
|                                                                       |
| XPENDING student_stream student_group                                 |
|                                                                       |
| XPENDING search_stream search_group                                   |
+-----------------------------------------------------------------------+

**Consumer Monitoring**

  -----------------------------------------------------------------------
  XINFO CONSUMERS document_stream document_group

  -----------------------------------------------------------------------

**7. Complete Document Processing Operation**

**Step 1: Data Creation**

The producer creates a record with the following fields:

+-----------------------------------------------------------------------+
| document_id                                                           |
|                                                                       |
| title                                                                 |
|                                                                       |
| text                                                                  |
|                                                                       |
| priority                                                              |
|                                                                       |
| execute_at                                                            |
|                                                                       |
| retry_count                                                           |
+-----------------------------------------------------------------------+

**Step 2: Message Publication**

The producer sends the message to document_stream.

**Step 3: Consumer Group**

The message is read by document_group.

**Step 4: Python Consumer**

The Python consumer uses XREADGROUP to read the message.

**Step 5: Validation**

The consumer validates document_id, title, text, priority, and
execute_at. document_id, title, and text are mandatory; priority and
execute_at are optional and fall back to sensible defaults when absent.

**Step 6: Priority Handling**

Messages carry one of three priority levels: HIGH, MEDIUM, or LOW.
Priority determines the order in which otherwise-ready messages are
processed, with HIGH priority messages processed ahead of MEDIUM and LOW
priority messages.

**Step 7: Scheduling**

If a message carries an execute_at timestamp, the consumer compares it
to the current time:

+-----------------------------------------------------------------------+
| Current Time \< execute_at → processing is deferred                   |
|                                                                       |
| Current Time ≥ execute_at → message can be processed                  |
+-----------------------------------------------------------------------+

**Step 8: Embedding Generation**

  -----------------------------------------------------------------------
  **Text**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Embedding Model**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Vector**

  -----------------------------------------------------------------------

Example:

  -----------------------------------------------------------------------
  **\"Redis Streams are asynchronous\"**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **\[0.12, -0.44, 0.78, \...\]**

  -----------------------------------------------------------------------

Semantically similar text produces vectors that are close together in
the embedding space, which is what makes similarity search possible.

**Step 9: Qdrant Storage**

Each record is stored as a Qdrant point containing:

+-----------------------------------------------------------------------+
| Point ID                                                              |
|                                                                       |
| Vector                                                                |
|                                                                       |
| Payload                                                               |
+-----------------------------------------------------------------------+

Example payload:

+-----------------------------------------------------------------------+
| document_id                                                           |
|                                                                       |
| title                                                                 |
|                                                                       |
| text                                                                  |
|                                                                       |
| priority                                                              |
+-----------------------------------------------------------------------+

**Step 10: Success**

After successful insertion into Qdrant, the consumer sends a success
email and then acknowledges the message:

  -----------------------------------------------------------------------
  **Send Success Email**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Redis XACK**

  -----------------------------------------------------------------------

XACK confirms that the message has been fully and successfully processed
and can be removed from the pending list.

**8. Complete Validation and Failure Flow**

**Validation and Failure Flowchart**

  -----------------------------------------------------------------------
  **Message Received**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Validate Message**

  -----------------------------------------------------------------------

**↓**

+-----------------------------------+-----------------------------------+
| **VALID**                         | **INVALID**                       |
|                                   |                                   |
| **↓**                             | **↓**                             |
|                                   |                                   |
|   ------------------------------  |   ------------------------------  |
|   **Continue Processing**         |   **Retry**                       |
|                                   |                                   |
|   ------------------------------  |   ------------------------------  |
|                                   |                                   |
|                                   | **↓**                             |
|                                   |                                   |
|                                   |   ------------------------------  |
|                                   |   **retry_count + 1**             |
|                                   |                                   |
|                                   |   ------------------------------  |
|                                   |                                   |
|                                   | **↓**                             |
|                                   |                                   |
|                                   |   ------------------------------  |
|                                   |   **Retry Limit?**                |
|                                   |                                   |
|                                   |   ------------------------------  |
+-----------------------------------+-----------------------------------+

+-----------------------------------+-----------------------------------+
| **NO**                            | **YES**                           |
|                                   |                                   |
| **↓**                             | **↓**                             |
|                                   |                                   |
|   ------------------------------  |   ------------------------------  |
|   **Retry Again**                 |   **DLQ**                         |
|                                   |                                   |
|   ------------------------------  |   ------------------------------  |
|                                   |                                   |
|                                   | **↓**                             |
|                                   |                                   |
|                                   |   ------------------------------  |
|                                   |   **Failure Email**               |
|                                   |                                   |
|                                   |   ------------------------------  |
|                                   |                                   |
|                                   | **↓**                             |
|                                   |                                   |
|                                   |   ------------------------------  |
|                                   |   **Developer Review**            |
|                                   |                                   |
|                                   |   ------------------------------  |
+-----------------------------------+-----------------------------------+

*Figure 8 --- Valid messages proceed to processing; invalid messages
retry until the limit is reached, then move to the DLQ.*

Examples of invalid data include:

-   Missing document_id

-   Missing title

-   Missing text

-   Invalid priority value

-   Invalid execute_at format

**9. Retry Mechanism**

Failed messages are retried using exponential backoff:

+-----------------------------------------------------------------------+
| Attempt 1 → Wait 5 seconds                                            |
|                                                                       |
| Attempt 2 → Wait 10 seconds                                           |
|                                                                       |
| Attempt 3 → Wait 20 seconds                                           |
+-----------------------------------------------------------------------+

**Retry Flowchart**

  -----------------------------------------------------------------------
  **Failure**

  -----------------------------------------------------------------------

**↓**

+-----------------------------------------------------------------------+
| **Retry 1**                                                           |
|                                                                       |
| *wait 5 seconds*                                                      |
+-----------------------------------------------------------------------+

**↓**

+-----------------------------------------------------------------------+
| **Retry 2**                                                           |
|                                                                       |
| *wait 10 seconds*                                                     |
+-----------------------------------------------------------------------+

**↓**

+-----------------------------------------------------------------------+
| **Retry 3**                                                           |
|                                                                       |
| *wait 20 seconds*                                                     |
+-----------------------------------------------------------------------+

**↓**

  -----------------------------------------------------------------------
  **Still Failed?**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **DLQ**

  -----------------------------------------------------------------------

*Figure 9 --- Exponential backoff spaces retries further apart on each
attempt, giving transient problems time to clear.*

Retries are useful specifically for temporary failures, such as:

-   Temporary Qdrant connection failure

-   Temporary Redis issue

-   Network failure

-   Embedding model failure

**10. Dead Letter Queue**

A message is moved to the Dead Letter Queue (DLQ) once it has exhausted
its maximum number of retry attempts:

**Dead Letter Queue Flowchart**

  -----------------------------------------------------------------------
  **Original Message**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Retry 1**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Retry 2**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Retry 3**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Permanent Failure**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **DLQ**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Developer Review**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Fix Problem**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Reprocess Message**

  -----------------------------------------------------------------------

*Figure 10 --- DLQ lifecycle from repeated failure through developer
remediation and reprocessing.*

**Why the DLQ Exists**

The DLQ prevents a permanently broken message from blocking the stream
or looping through retries forever, while still preserving the message
so nothing is silently lost.

**What Is Stored**

The DLQ retains the original message payload, the final error, and the
retry history, so that a developer can diagnose the failure without
needing to reproduce it from scratch.

**Review and Reprocessing**

Developers review DLQ entries, fix the underlying problem (a data issue,
a code defect, or an infrastructure issue), and then resubmit the
corrected message back into the originating stream for normal
processing.

**11. Email Notification Operations**

**Success Email**

Sent after embedding generation and successful Qdrant storage:

  -----------------------------------------------------------------------
  **Embedding Generated**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Qdrant Storage Successful**

  -----------------------------------------------------------------------

**Failure Email**

Sent after the maximum number of retry attempts has been reached:

  -----------------------------------------------------------------------
  **Maximum Retry Attempts Reached**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Message Moved to DLQ**

  -----------------------------------------------------------------------

Email notifications give operators visibility into both successful
processing and permanent failures without needing to continuously watch
logs.

**12. Qdrant Vector Database Operations**

All document vectors are stored in a single collection:

  -----------------------------------------------------------------------
  Collection: documents

  -----------------------------------------------------------------------

  -----------------------------------------------------------------------
  **Text**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Embedding**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Vector**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Qdrant Point**

  -----------------------------------------------------------------------

A Qdrant point contains:

+-----------------------------------------------------------------------+
| id                                                                    |
|                                                                       |
| vector                                                                |
|                                                                       |
| payload                                                               |
+-----------------------------------------------------------------------+

The payload stores the original metadata alongside the vector, such as:

+-----------------------------------------------------------------------+
| document_id                                                           |
|                                                                       |
| title                                                                 |
|                                                                       |
| text                                                                  |
|                                                                       |
| priority                                                              |
+-----------------------------------------------------------------------+

Collection health can be checked with:

  -----------------------------------------------------------------------
  GET /collections/documents

  -----------------------------------------------------------------------

This response also reports the current point count, which operators can
use to confirm that ingestion is progressing as expected.

**13. Semantic Search Operation**

**Semantic Search Flowchart**

  -----------------------------------------------------------------------
  **User Query**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **search_stream**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **search_consumer.py**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Validate Search Command**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Query Embedding**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Qdrant query_points()**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Similarity Search**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Top-K Results**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Display / Publish Results**

  -----------------------------------------------------------------------

*Figure 13 --- A search command is validated, embedded, and matched
against stored vectors to produce ranked results.*

Example request:

+-----------------------------------------------------------------------+
| Query: \"What are Redis Streams?\"                                    |
|                                                                       |
| top_k: 5                                                              |
+-----------------------------------------------------------------------+

  -----------------------------------------------------------------------
  **Query**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Embedding**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Vector Search**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Similarity Score**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Top-K Documents**

  -----------------------------------------------------------------------

The system returns the most semantically similar documents rather than
only exact keyword matches, so a query can retrieve relevant results
even when it does not share exact wording with the stored text.

Example result:

+-----------------------------------------------------------------------+
| Result 1                                                              |
|                                                                       |
| ID: DOC3001                                                           |
|                                                                       |
| Score: 0.89                                                           |
|                                                                       |
| Payload:                                                              |
|                                                                       |
| { \"title\": \"Redis Streams\",                                       |
|                                                                       |
| \"text\": \"Redis Streams allow applications\...\" }                  |
+-----------------------------------------------------------------------+

**14. Search Result Output**

Search results are made available in two ways:

-   Printed in the search consumer terminal

-   Published to the configured result stream

Example terminal output:

+-----------------------------------------------------------------------+
| ========== SEARCH RESULTS ==========                                  |
|                                                                       |
| Query: \"What are Redis Streams?\"                                    |
|                                                                       |
| Results Found: 2                                                      |
|                                                                       |
| Result 1: ID: DOC3001 Score: 0.89 Payload: {\...}                     |
|                                                                       |
| Result 2: ID: DOC3002 Score: 0.81 Payload: {\...}                     |
|                                                                       |
| =====================================                                 |
+-----------------------------------------------------------------------+

The result stream carries the following structure:

+-----------------------------------------------------------------------+
| query                                                                 |
|                                                                       |
| result_count                                                          |
|                                                                       |
| results                                                               |
+-----------------------------------------------------------------------+

**15. Docker Deployment**

The infrastructure is deployed as two Docker containers on the remote
SSH server:

  -----------------------------------------------------------------------
  **Docker**

  -----------------------------------------------------------------------

+-----------------------------------+-----------------------------------+
| **Redis Container**               | **Qdrant Container**              |
|                                   |                                   |
| **↓**                             | **↓**                             |
|                                   |                                   |
|   ------------------------------  |   ------------------------------  |
|   **Redis 6.2**                   |   **Qdrant Vector Database**      |
|                                   |                                   |
|   ------------------------------  |   ------------------------------  |
+-----------------------------------+-----------------------------------+

Operational commands:

+-----------------------------------------------------------------------+
| sudo docker ps                                                        |
|                                                                       |
| sudo docker ps -a                                                     |
|                                                                       |
| sudo docker logs redis                                                |
|                                                                       |
| sudo docker logs qdrant                                               |
|                                                                       |
| sudo docker restart redis                                             |
|                                                                       |
| sudo docker restart qdrant                                            |
+-----------------------------------------------------------------------+

Redis and Qdrant run as long-lived Docker containers on the remote
server, and are managed independently of the Python consumer processes.

**16. Service Health Checks**

**Redis**

  -----------------------------------------------------------------------
  redis-cli ping

  -----------------------------------------------------------------------

Expected response:

  -----------------------------------------------------------------------
  PONG

  -----------------------------------------------------------------------

**Qdrant**

  -----------------------------------------------------------------------
  curl http://localhost:6333

  -----------------------------------------------------------------------

Collection check:

  -----------------------------------------------------------------------
  curl http://localhost:6333/collections/documents

  -----------------------------------------------------------------------

**Docker**

  -----------------------------------------------------------------------
  sudo docker ps

  -----------------------------------------------------------------------

**Python Consumers**

+-----------------------------------------------------------------------+
| python3 consumer.py document                                          |
|                                                                       |
| python3 consumer.py student                                           |
|                                                                       |
| python3 consumer.py employee                                          |
|                                                                       |
| python3 search_consumer.py                                            |
+-----------------------------------------------------------------------+

All of these services must be running simultaneously for the system to
be fully operational.

**17. Operational Monitoring**

**Redis**

+-----------------------------------------------------------------------+
| XINFO STREAM                                                          |
|                                                                       |
| XINFO GROUPS                                                          |
|                                                                       |
| XINFO CONSUMERS                                                       |
|                                                                       |
| XPENDING                                                              |
|                                                                       |
| XLEN                                                                  |
|                                                                       |
| XRANGE                                                                |
+-----------------------------------------------------------------------+

**Qdrant**

Operators should monitor:

-   Collection availability

-   Point count

-   API response times

-   Vector storage growth

**Python Consumers**

Consumer logs should be monitored at the INFO, WARNING, and ERROR
levels. Important log lines to watch for include:

+-----------------------------------------------------------------------+
| Received message                                                      |
|                                                                       |
| Embedding model loaded                                                |
|                                                                       |
| Embedding generated                                                   |
|                                                                       |
| Qdrant request successful                                             |
|                                                                       |
| Search results returned                                               |
|                                                                       |
| Retrying message                                                      |
|                                                                       |
| Message moved to DLQ                                                  |
|                                                                       |
| Email sent                                                            |
|                                                                       |
| Message acknowledged                                                  |
+-----------------------------------------------------------------------+

**18. Troubleshooting Guide**

  ------------------------------------------------------------------------
  **Problem**           **Possible Cause**         **Solution**
  --------------------- -------------------------- -----------------------
  Redis connection      Redis container stopped    Check docker ps
  failed                                           

  Qdrant connection     Qdrant container stopped   Restart Qdrant
  failed                                           

  Message not processed Consumer not running       Start Python consumer

  Message retrying      Validation or processing   Check logs
                        failure                    

  Message in DLQ        Maximum retries reached    Review DLQ

  No search result      No matching vectors        Check Qdrant collection

  Search command not    Search consumer not        Start
  processed             running                    search_consumer.py

  Qdrant dashboard not  Port forwarding missing    Create SSH tunnel
  opening                                          

  Redis web UI not      RedisInsight not running   Check RedisInsight and
  opening               or tunnel missing          port 8001

  XADD command not      Redis command entered in   Enter redis-cli first
  found                 Linux terminal             

  docker command        User lacks Docker          Use sudo docker
  permission denied     permission                 
  ------------------------------------------------------------------------

**19. Complete Operational Flowchart**

**Complete Operational Flowchart**

  -----------------------------------------------------------------------
  **START**

  -----------------------------------------------------------------------

**↓**

+-----------------------------------------------------------------------+
| **Create Data**                                                       |
|                                                                       |
| *Document / Student / Employee*                                       |
+-----------------------------------------------------------------------+

**↓**

  -----------------------------------------------------------------------
  **Redis Stream**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Consumer Group**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Python Consumer**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Validate Message**

  -----------------------------------------------------------------------

**↓**

+-----------------------------------+-----------------------------------+
| **VALID**                         | **INVALID**                       |
|                                   |                                   |
| **↓**                             | **↓**                             |
|                                   |                                   |
|   ------------------------------  |   ------------------------------  |
|   **Priority Check**              |   **Retry**                       |
|                                   |                                   |
|   ------------------------------  |   ------------------------------  |
|                                   |                                   |
| **↓**                             | **↓**                             |
|                                   |                                   |
|   ------------------------------  |   ------------------------------  |
|   **execute_at Check**            |   **Retry Limit**                 |
|                                   |                                   |
|   ------------------------------  |   ------------------------------  |
|                                   |                                   |
| **↓**                             | **↓** *no → retry again*          |
|                                   |                                   |
|   ------------------------------  |   ------------------------------  |
|   **Generate Embedding**          |   **DLQ**                         |
|                                   |                                   |
|   ------------------------------  |   ------------------------------  |
|                                   |                                   |
| **↓**                             | **↓**                             |
|                                   |                                   |
|   ------------------------------  |   ------------------------------  |
|   **Qdrant**                      |   **Failure Email**               |
|                                   |                                   |
|   ------------------------------  |   ------------------------------  |
|                                   |                                   |
| **↓**                             | **↓**                             |
|                                   |                                   |
|   ------------------------------  |   ------------------------------  |
|   **Success Email**               |   **Developer Review**            |
|                                   |                                   |
|   ------------------------------  |   ------------------------------  |
|                                   |                                   |
| **↓**                             |                                   |
|                                   |                                   |
|   ------------------------------  |                                   |
|   **XACK**                        |                                   |
|                                   |                                   |
|   ------------------------------  |                                   |
+-----------------------------------+-----------------------------------+

**↓**

  -----------------------------------------------------------------------
  **END**

  -----------------------------------------------------------------------

*Figure 19 --- The full operational path from data creation through the
validation branch to a terminal state.*

**20. Complete Search Flowchart**

**Complete Search Flowchart**

  -----------------------------------------------------------------------
  **SEARCH REQUEST**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **User Query**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **search_stream**

  -----------------------------------------------------------------------

**↓**

+-----------------------------------------------------------------------+
| **Redis Consumer Group**                                              |
|                                                                       |
| *search_group*                                                        |
+-----------------------------------------------------------------------+

**↓**

  -----------------------------------------------------------------------
  **search_consumer.py**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Validate Command**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Query Embedding**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Qdrant**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Semantic Similarity**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Top-K Results**

  -----------------------------------------------------------------------

**↓**

+-----------------------------------+-----------------------------------+
| **Terminal Output**               | **Result Stream**                 |
|                                   |                                   |
| **↓**                             | **↓**                             |
|                                   |                                   |
|   ------------------------------  |   ------------------------------  |
|   **Displayed to Operator**       |   **Published for Consumers**     |
|                                   |                                   |
|   ------------------------------  |   ------------------------------  |
+-----------------------------------+-----------------------------------+

*Figure 20 --- A search request flows through validation and embedding
to Qdrant, and results are delivered two ways.*

**21. Operational Runbook**

**Start Infrastructure**

  -----------------------------------------------------------------------
  sudo docker ps

  -----------------------------------------------------------------------

If Redis is stopped:

  -----------------------------------------------------------------------
  sudo docker start redis

  -----------------------------------------------------------------------

If Qdrant is stopped:

  -----------------------------------------------------------------------
  sudo docker start qdrant

  -----------------------------------------------------------------------

**Start Python Environment**

+-----------------------------------------------------------------------+
| cd \~/IITM                                                            |
|                                                                       |
| source venv/bin/activate                                              |
+-----------------------------------------------------------------------+

**Start Consumers**

Document:

  -----------------------------------------------------------------------
  python3 consumer.py document

  -----------------------------------------------------------------------

Student:

  -----------------------------------------------------------------------
  python3 consumer.py student

  -----------------------------------------------------------------------

Employee:

  -----------------------------------------------------------------------
  python3 consumer.py employee

  -----------------------------------------------------------------------

Search:

  -----------------------------------------------------------------------
  python3 search_consumer.py

  -----------------------------------------------------------------------

**Test Document Processing**

+-----------------------------------------------------------------------+
| XADD document_stream \* \\                                            |
|                                                                       |
| document_id DOC3001 \\                                                |
|                                                                       |
| title \"Redis Streams\" \\                                            |
|                                                                       |
| text \"Redis Streams allow applications to send and process messages  |
| asynchronously.\" \\                                                  |
|                                                                       |
| priority HIGH \\                                                      |
|                                                                       |
| retry_count 0                                                         |
+-----------------------------------------------------------------------+

**Test Search**

+-----------------------------------------------------------------------+
| XADD search_stream \* \\                                              |
|                                                                       |
| command search \\                                                     |
|                                                                       |
| query \"What are Redis Streams?\" \\                                  |
|                                                                       |
| top_k 5                                                               |
+-----------------------------------------------------------------------+

**Check Results**

  -----------------------------------------------------------------------
  XRANGE \<RESULT_STREAM\> - +

  -----------------------------------------------------------------------

**22. Operational Best Practices**

-   Always monitor consumer logs

-   Monitor pending Redis messages

-   Monitor DLQ messages

-   Use retry limits

-   Use exponential backoff

-   Validate messages before processing

-   Monitor Qdrant collection health

-   Keep Redis and Qdrant containers running

-   Use Docker restart policies

-   Do not delete unprocessed Redis messages manually

-   Use XACK only after successful processing

-   Keep the search consumer running for search operations

-   Back up important data

-   Monitor disk usage

-   Monitor memory usage

-   Monitor embedding model availability

**23. Final End-to-End System Summary**

**Data Ingestion Path**

  -----------------------------------------------------------------------
  **Producer**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Redis Streams**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Consumer Groups**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Python Consumers**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Validation**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Priority + Scheduling**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Retry / DLQ on Failure**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Embedding Generation**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Qdrant Vector Storage**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Success Email**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Redis XACK**

  -----------------------------------------------------------------------

*Figure 23A --- Complete data-ingestion path, start to finish.*

**Search Path**

  -----------------------------------------------------------------------
  **User Query**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **search_stream**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Search Consumer**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Query Embedding**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Qdrant Semantic Search**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Top-K Similar Documents**

  -----------------------------------------------------------------------

**↓**

  -----------------------------------------------------------------------
  **Terminal / Result Stream**

  -----------------------------------------------------------------------

*Figure 23B --- Complete semantic-search path, start to finish.*

Taken together, the system provides:

-   Asynchronous processing

-   Reliable message delivery

-   Consumer group-based processing

-   Validation

-   Priority handling

-   Scheduled execution

-   Retry and exponential backoff

-   Dead Letter Queue handling

-   Email notifications

-   Vector embeddings

-   Semantic search

-   Qdrant vector storage

-   Docker-based deployment

-   Operational monitoring

-   Failure recovery

This document reflects the full operational structure of the system as
implemented, including document, student, and employee processing, retry
and DLQ handling, email notifications, embedding generation, semantic
search, and Docker/SSH-based server deployment.
