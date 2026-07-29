**SYSTEM DESIGN DOCUMENT**

**Redis Streams-Based Asynchronous Data Processing and Qdrant Semantic
Search System**

*Version 1.0*

Prepared for: Project Guide / Architecture Review / Engineering Team

July 2026

Table of Contents

1\. Design Document Purpose

This document defines the technical design of an asynchronous data
processing and semantic search platform. Unlike the companion
Operational Document, which describes how to run and monitor the system,
this document explains how the system is architected internally, the
reasoning behind each design decision, and how its components interact.

The system is designed around the following processing pipeline:

+-----------------------------------------------------------------------+
| Redis Streams                                                         |
|                                                                       |
| \|                                                                    |
|                                                                       |
| v                                                                     |
|                                                                       |
| Redis Consumer Groups                                                 |
|                                                                       |
| \|                                                                    |
|                                                                       |
| v                                                                     |
|                                                                       |
| Python Processing Services                                            |
|                                                                       |
| \|                                                                    |
|                                                                       |
| v                                                                     |
|                                                                       |
| Embedding Model                                                       |
|                                                                       |
| \|                                                                    |
|                                                                       |
| v                                                                     |
|                                                                       |
| Qdrant Vector Database                                                |
+-----------------------------------------------------------------------+

The design supports the following capabilities:

-   Document ingestion

-   Student data processing

-   Employee data processing

-   Asynchronous message processing

-   Message validation

-   Priority-based processing

-   Scheduled processing

-   Retry handling with exponential backoff

-   Dead Letter Queue processing

-   Email notifications

-   Vector embedding generation

-   Qdrant vector storage

-   Semantic similarity search

-   Docker-based deployment

2\. Design Goals

  -----------------------------------------------------------------------
  **Goal**                 **Description**
  ------------------------ ----------------------------------------------
  Asynchronous Processing  Producers and consumers operate independently

  Reliability              Messages are not lost during normal processing
                           failures

  Scalability              Consumers can be scaled independently

  Fault Tolerance          Temporary failures are retried

  Recoverability           Permanently failed messages are moved to DLQ

  Semantic Search          Text is converted into embeddings and searched
                           semantically

  Modularity               Each major responsibility is separated into
                           modules

  Observability            Logs, pending messages, and service health can
                           be monitored

  Deployment Flexibility   Infrastructure runs using Docker

  Extensibility            New streams and consumers can be added later
  -----------------------------------------------------------------------

3\. High-Level System Architecture

The system is organized as a strict top-to-bottom pipeline. Producers
publish onto Redis Streams; Redis Consumer Groups distribute stream
entries reliably to Python consumers; a processing layer applies
validation, priority, and scheduling rules; the embedding service
converts text into vectors; and Qdrant persists and indexes those
vectors for later retrieval.

![](media/112db92dedb55be51249392e496e9e299aef5ba0.png){width="4.791666666666667in"
height="6.739583333333333in"}

*Figure 3.1 - High-level layered system architecture*

**Layer responsibilities:**

-   Producers --- create and publish typed messages (document, student,
    employee, search) onto their dedicated stream.

-   Redis Streams --- durably buffer messages until a consumer group
    reads and acknowledges them.

-   Redis Consumer Groups --- guarantee that each stream entry is
    delivered to exactly one consumer within the group, with tracked
    pending state.

-   Python Consumers --- host the business logic that turns a raw stream
    entry into a fully processed record or search result.

-   Processing Layer --- applies validation, priority ordering,
    execute_at scheduling, retry accounting, and DLQ routing.

-   Embedding Service --- converts validated text into a fixed-length
    numerical vector using a Sentence Transformer model.

-   Qdrant --- stores each vector alongside its metadata payload and
    serves similarity search queries.

4\. Detailed Component Architecture

  -----------------------------------------------------------------------
  **Component**       **Responsibility**           **Main Interface**
  ------------------- ---------------------------- ----------------------
  Producer            Publishes messages           XADD

  Redis Stream        Buffers messages             Stream API

  Consumer Group      Reliable distribution        XREADGROUP

  Python Consumer     Processes messages           Python service

  Validation Module   Validates fields             Validation functions

  Retry Handler       Controls retries             Retry functions

  DLQ Handler         Handles permanent failures   DLQ publisher

  Email Service       Sends notifications          Email function

  Embedding Service   Creates vectors              embed_text()

  Qdrant Service      Stores vectors               Upsert API

  Search Service      Searches vectors             Query API

  Search Consumer     Processes search requests    Search stream
  -----------------------------------------------------------------------

5\. Module-Level Design

The project is organized into focused, single-responsibility modules:

+-----------------------------------------------------------------------+
| IITM/                                                                 |
|                                                                       |
| \|                                                                    |
|                                                                       |
| +\-- consumer.py                                                      |
|                                                                       |
| +\-- search_consumer.py                                               |
|                                                                       |
| +\-- producer.py                                                      |
|                                                                       |
| \|                                                                    |
|                                                                       |
| +\-- config.py                                                        |
|                                                                       |
| +\-- redis_client.py                                                  |
|                                                                       |
| \|                                                                    |
|                                                                       |
| +\-- validation.py                                                    |
|                                                                       |
| +\-- retry_handler.py                                                 |
|                                                                       |
| +\-- dlq_handler.py                                                   |
|                                                                       |
| +\-- email_service.py                                                 |
|                                                                       |
| \|                                                                    |
|                                                                       |
| +\-- embedding_service.py                                             |
|                                                                       |
| +\-- qdrant_service.py                                                |
|                                                                       |
| +\-- qdrant_helper.py                                                 |
|                                                                       |
| +\-- search_service.py                                                |
|                                                                       |
| \|                                                                    |
|                                                                       |
| +\-- dlq_review.py                                                    |
|                                                                       |
| +\-- requirements.txt                                                 |
|                                                                       |
| +\-- docker-compose.yml                                               |
|                                                                       |
| \`\-- README.md                                                       |
+-----------------------------------------------------------------------+

  ------------------------------------------------------------------------
  **File**               **Responsibility**
  ---------------------- -------------------------------------------------
  consumer.py            Entry point for document, student, and employee
                         stream consumers

  search_consumer.py     Entry point for the search-stream consumer

  producer.py            Publishes sample or production messages onto the
                         streams

  config.py              Centralized configuration (hosts, ports,
                         thresholds, model name)

  redis_client.py        Shared Redis connection and stream/consumer-group
                         helpers

  validation.py          Field-level validation rules for every message
                         type

  retry_handler.py       Retry-count tracking and exponential backoff
                         scheduling

  dlq_handler.py         Publishes permanently failed messages to the DLQ
                         stream

  email_service.py       Sends success and failure notification emails

  embedding_service.py   Wraps the Sentence Transformer model
                         (embed_text())

  qdrant_service.py      Upsert and collection-management calls to Qdrant

  qdrant_helper.py       Point-ID generation and payload-shaping utilities

  search_service.py      Query embedding and Qdrant similarity search
                         calls

  dlq_review.py          Operator utility for inspecting and reprocessing
                         DLQ entries
  ------------------------------------------------------------------------

6\. Producer Design

Each producer follows the same lifecycle: receive data, assemble a
message payload, attach required fields, and publish to the correct
Redis Stream.

+-----------------------------------------------------------------------+
| Producer                                                              |
|                                                                       |
| \|                                                                    |
|                                                                       |
| +\-- Document Data                                                    |
|                                                                       |
| +\-- Student Data                                                     |
|                                                                       |
| +\-- Employee Data                                                    |
|                                                                       |
| \`\-- Search Command                                                  |
|                                                                       |
| \|                                                                    |
|                                                                       |
| v                                                                     |
|                                                                       |
| XADD                                                                  |
|                                                                       |
| \|                                                                    |
|                                                                       |
| v                                                                     |
|                                                                       |
| Redis Stream                                                          |
+-----------------------------------------------------------------------+

Message schemas

  -----------------------------------------------------------------------
  **Message Type**    **Fields**
  ------------------- ---------------------------------------------------
  document_stream     document_id, title, text, priority, retry_count,
                      execute_at

  student_stream      student_id, name, age, department, priority,
                      execute_at

  employee_stream     employee_id, name, department, role, priority,
                      execute_at

  search_stream       command, query, top_k
  -----------------------------------------------------------------------

Each producer publishes only to its corresponding stream, keeping the
three data domains and the search-command channel fully independent of
one another.

7\. Redis Stream Design

+-----------------------------------------------------------------------+
| document_stream -\> document_group -\> document consumer              |
|                                                                       |
| student_stream -\> student_group -\> student consumer                 |
|                                                                       |
| employee_stream -\> employee_group -\> employee consumer              |
|                                                                       |
| search_stream -\> search_group -\> search consumer                    |
+-----------------------------------------------------------------------+

Separate streams are used for each data domain rather than a single
shared stream, for the following reasons:

-   Isolation --- a spike or fault in one domain cannot block another

-   Independent processing --- each stream has its own consumer logic

-   Independent scaling --- consumers can be added per stream as needed

-   Different validation rules --- document, student, and employee
    records have distinct required fields

-   Independent failure handling --- retry counters and DLQs are scoped
    per stream

-   Better monitoring --- XLEN, XPENDING, and XINFO are meaningful per
    stream rather than mixed together

8\. Consumer Group Design

![](media/220c453f9040efc4e481f2418930549288116d1b.png){width="5.0in"
height="3.28125in"}

*Figure 8.1 - Redis Consumer Group delivery model*

**Key mechanics:**

-   XREADGROUP delivers new (or previously undelivered) entries to a
    named consumer within the group.

-   Each consumer has its own identity, so Redis can track which
    consumer currently owns which message.

-   The Pending Entries List (PEL) records every message that has been
    delivered but not yet acknowledged.

-   XACK removes a message from the PEL once it has been fully and
    successfully processed.

-   Message ownership can be reassigned (via XCLAIM/XAUTOCLAIM) if the
    owning consumer fails before acknowledging.

-   A message is acknowledged only after processing succeeds --- partial
    processing never results in an XACK.

9\. Document Processing Design

![](media/b726340d0db2271dc5f2bbfb77c606beb1ae512e.png){width="4.697916666666667in"
height="7.291666666666667in"}

*Figure 9.1 - Detailed document processing and failure-handling flow*

Each stage of the flow has a distinct responsibility:

-   Validate --- confirms the message is well-formed before any business
    logic runs.

-   Priority --- reorders otherwise-ready messages so HIGH priority work
    is handled first.

-   Scheduling --- defers messages whose execute_at time has not yet
    arrived.

-   Embedding --- converts the document text into a vector
    representation.

-   Qdrant --- persists the vector and payload.

-   Email --- notifies stakeholders of success or, on the failure path,
    of permanent failure.

-   XACK --- confirms to Redis that the message is fully handled.

10\. Validation Design

The validation layer runs before any business logic and checks:

+-----------------------------------------------------------------------+
| Required Fields                                                       |
|                                                                       |
| \|                                                                    |
|                                                                       |
| +\-- document_id                                                      |
|                                                                       |
| +\-- title                                                            |
|                                                                       |
| \`\-- text                                                            |
|                                                                       |
| Additional Fields                                                     |
|                                                                       |
| \|                                                                    |
|                                                                       |
| +\-- priority                                                         |
|                                                                       |
| +\-- execute_at                                                       |
|                                                                       |
| \`\-- retry_count                                                     |
+-----------------------------------------------------------------------+

Validation covers:

-   Required field validation --- document_id, title, and text must be
    present

-   Type validation --- fields must match their expected data type

-   Empty value validation --- blank strings are rejected the same as
    missing fields

-   Priority validation --- priority must be one of HIGH, MEDIUM, or LOW

-   Date/time validation --- execute_at, when present, must be a
    parseable timestamp

+-----------------------------------------------------------------------+
| Invalid Message                                                       |
|                                                                       |
| \|                                                                    |
|                                                                       |
| v                                                                     |
|                                                                       |
| Validation Error                                                      |
|                                                                       |
| \|                                                                    |
|                                                                       |
| v                                                                     |
|                                                                       |
| Retry Handler                                                         |
+-----------------------------------------------------------------------+

11\. Priority Design

![](media/ed1a82ba6b3f4427179374f79df7cb3f0b1e604a.png){width="4.375in"
height="2.03125in"}

*Figure 11.1 - Priority tiers*

Priority is carried as a plain string field on every ingestion message
and is read by the consumer immediately after validation. It does not
change whether a message is processed, only the order in which ready
messages are handled; the model can be extended later with additional
tiers or numeric weights without changing the message schema.

  -----------------------------------------------------------------------
  **Priority**    **Meaning**
  --------------- -------------------------------------------------------
  HIGH            Process urgently

  MEDIUM          Normal processing

  LOW             Lower urgency
  -----------------------------------------------------------------------

12\. Scheduling Design

![](media/33deb3df8c3ef5d808b99e34f88b17df36395ad2.png){width="4.375in"
height="2.5in"}

*Figure 12.1 - execute_at scheduling decision*

+-----------------------------------------------------------------------+
| Current Time \< execute_at -\> message is not ready (deferred)        |
|                                                                       |
| Current Time \>= execute_at -\> message can be processed              |
+-----------------------------------------------------------------------+

All execute_at comparisons are performed in UTC to avoid ambiguity
across producer and consumer time zones.

13\. Embedding Architecture

![](media/bb9e3c9e2c887b884f90cd45a9fb142c0bd6e16d.png){width="4.375in"
height="3.5520833333333335in"}

*Figure 13.1 - Text-to-vector embedding pipeline*

Text is passed through a Sentence Transformer embedding model, which
produces a fixed-length numerical vector capturing the semantic meaning
of the text rather than its literal wording. Semantically similar text
--- even with different vocabulary --- produces vectors that are close
together in the embedding space, which is what makes similarity search
possible. Qdrant is used as the vector store because it is purpose-built
for high-dimensional similarity search with metadata filtering.

14\. Qdrant Data Model

![](media/83107dbe0bc8290d0117b3da19a7ef9c001f8968.png){width="4.791666666666667in"
height="3.1458333333333335in"}

*Figure 14.1 - Qdrant point structure*

The vector is used purely for similarity search; the payload stores the
original metadata needed to interpret and display a match.

+-----------------------------------------------------------------------+
| Collection: documents                                                 |
|                                                                       |
| Vector                                                                |
|                                                                       |
| \|                                                                    |
|                                                                       |
| +\-- Dimension: based on the embedding model                          |
|                                                                       |
| \`\-- Distance: similarity metric (e.g. cosine)                       |
|                                                                       |
| Payload                                                               |
|                                                                       |
| \|                                                                    |
|                                                                       |
| +\-- document_id                                                      |
|                                                                       |
| +\-- title                                                            |
|                                                                       |
| +\-- text                                                             |
|                                                                       |
| \`\-- metadata                                                        |
+-----------------------------------------------------------------------+

The vector dimension configured for the collection must match the output
dimension of the selected embedding model; changing the embedding model
requires migrating or recreating the collection.

15\. Qdrant Upsert Design

![](media/a2f2c03eadaae65ea879e969cdf403cab88d65c4.png){width="4.166666666666667in"
height="3.6458333333333335in"}

*Figure 15.1 - Upsert flow from text to stored point*

Point IDs are generated deterministically from the source record\'s
identifier (for example, derived from document_id) rather than randomly.
This makes the upsert operation idempotent: reprocessing the same
document --- whether due to a retry, a redelivered message, or a manual
reprocessing step --- overwrites the existing point instead of creating
a duplicate vector record.

16\. Search Architecture

![](media/5027704ccca04a2da887442e6a348c67fedc23f1.png){width="4.583333333333333in"
height="6.0625in"}

*Figure 16.1 - Semantic search pipeline*

Example search command:

+-----------------------------------------------------------------------+
| command = search                                                      |
|                                                                       |
| query = \"What are Redis Streams?\"                                   |
|                                                                       |
| top_k = 5                                                             |
+-----------------------------------------------------------------------+

Processing stages:

-   Query validation --- confirms the command, query text, and top_k are
    well-formed

-   Query embedding --- converts the query text into the same vector
    space as stored documents

-   Vector search --- Qdrant compares the query vector against stored
    vectors

-   Similarity score --- each candidate result carries a numeric
    similarity score

-   Top-K selection --- only the K most similar results are returned

-   Result formatting --- results are shaped into the standard result
    structure before being returned

17\. Search Result Design

The result structure is intentionally minimal:

+-----------------------------------------------------------------------+
| query                                                                 |
|                                                                       |
| result_count                                                          |
|                                                                       |
| results                                                               |
+-----------------------------------------------------------------------+

Example:

+-----------------------------------------------------------------------+
| {                                                                     |
|                                                                       |
| \"query\": \"What are Redis Streams?\",                               |
|                                                                       |
| \"result_count\": \"2\",                                              |
|                                                                       |
| \"results\": \[\...\]                                                 |
|                                                                       |
| }                                                                     |
+-----------------------------------------------------------------------+

+-----------------------------------------------------------------------+
| Qdrant Results                                                        |
|                                                                       |
| \|                                                                    |
|                                                                       |
| v                                                                     |
|                                                                       |
| Format Results                                                        |
|                                                                       |
| \|                                                                    |
|                                                                       |
| v                                                                     |
|                                                                       |
| Publish to Result Stream                                              |
|                                                                       |
| \|                                                                    |
|                                                                       |
| v                                                                     |
|                                                                       |
| Display Results                                                       |
+-----------------------------------------------------------------------+

18\. Retry and Failure Design

![](media/49ddea53a1de436bbda54f8261f560a68112b9af.png){width="4.375in"
height="3.5520833333333335in"}

*Figure 18.1 - Retry and failure decision flow*

![](media/6e6474bbd841f16bd273a6a50500c760da0bc5ec.png){width="5.208333333333333in"
height="1.5625in"}

*Figure 18.2 - Exponential backoff between retry attempts*

The system does not retry continuously, because a tight retry loop
against a struggling dependency (Redis, Qdrant, or the embedding model)
can worsen an outage rather than help it recover. Exponential backoff
spaces each retry further apart, giving the dependency time to recover
before the next attempt.

19\. Dead Letter Queue Design

![](media/def233c836b74d2e0f05f82abc6ec6ead089f921.png){width="4.583333333333333in"
height="4.177083333333333in"}

*Figure 19.1 - Dead Letter Queue lifecycle*

The following information is preserved for every DLQ entry so that a
developer can diagnose the failure without reproducing it from scratch:

-   Original message

-   Original message ID

-   Error reason

-   Retry count

-   Timestamp

-   Failure details

20\. Email Notification Design

![](media/b409c80042fdcb41c3420b29abcd98677a5f7dcd.png){width="5.416666666666667in"
height="3.6666666666666665in"}

*Figure 20.1 - Success and failure notification paths*

Notification logic is implemented as a separate service rather than
being embedded directly inside the validation, retry, or Qdrant modules.
This separation means the core processing logic does not need to know
how notifications are delivered, and the notification channel (email
today) can be replaced or extended (for example, with Slack or webhook
notifications) without touching the processing pipeline.

21\. End-to-End Sequence Diagrams

Ingestion Sequence

![](media/46114b73590453f59ef7c189f63bd20048a7dada.png){width="5.833333333333333in"
height="3.21875in"}

*Figure 21.1 - Ingestion sequence: Producer, Redis, Consumer, Embedding,
Qdrant*

Search Sequence

![](media/0d368a881e4cc6fe5efb730679762a22187459c6.png){width="5.833333333333333in"
height="2.9166666666666665in"}

*Figure 21.2 - Search sequence: User, Redis, Search Consumer, Embedding,
Qdrant*

22\. Data Flow Design

![](media/91cbc471ee9e1d0f70d7263e233910a901ccc968.png){width="4.375in"
height="4.5in"}

*Figure 22.1 - End-to-end data flow from raw input to search results*

23\. Deployment Architecture

![](media/ba8e16647e03c80598fc05c9f160ccb562185e04.png){width="4.791666666666667in"
height="3.9479166666666665in"}

*Figure 23.1 - Deployment topology on the remote server*

-   Redis runs as a Docker container and owns all stream state.

-   Qdrant runs as a separate Docker container and owns all vector
    storage.

-   Python consumers run in the application environment (a virtual
    environment) directly on the host, outside of Docker.

-   SSH is used for remote access to the server for deployment and
    operational tasks.

-   Port forwarding (SSH tunneling) can expose the Qdrant dashboard and
    RedisInsight locally without opening those ports publicly.

24\. Docker Design

![](media/b7caaa60d65df56a999926d03fe5e05155888eba.png){width="5.833333333333333in"
height="2.5729166666666665in"}

*Figure 24.1 - Container responsibilities and ports*

+-----------------------------------------------------------------------+
| Redis -\> Port 6379                                                   |
|                                                                       |
| Qdrant -\> Port 6333 (HTTP)                                           |
|                                                                       |
| Qdrant gRPC -\> Port 6334                                             |
+-----------------------------------------------------------------------+

Each container owns a single responsibility --- Redis owns stream and
queue state, Qdrant owns vector collections --- and the two never
communicate directly with one another; all coordination happens through
the Python consumer layer.

25\. Reliability Design

![](media/2bd912cc24ddb2cb19e602d0d058aec69184054f.png){width="4.791666666666667in"
height="3.3854166666666665in"}

*Figure 25.1 - Reliability path from delivery to acknowledgement or DLQ*

Reliability is built from a small number of composable mechanisms rather
than a single feature:

-   Consumer groups guarantee at-least-once delivery of every stream
    entry.

-   Pending message tracking makes it possible to detect and recover
    work left behind by a crashed consumer.

-   Acknowledgement (XACK) is the single source of truth for whether a
    message is fully processed.

-   Retry with backoff absorbs transient failures without operator
    intervention.

-   The DLQ guarantees that a permanently failing message is preserved
    and surfaced rather than silently dropped or retried forever.

26\. Scalability Design

![](media/b1487cb1afdcca38515e9be4a872766dd2912a42.png){width="6.458333333333333in"
height="3.5729166666666665in"}

*Figure 26.1 - Current single-consumer design versus a future
multi-consumer design*

In the current design, each stream is served by a single consumer within
its consumer group. Because Redis Consumer Groups already support
multiple named consumers per group, the system can scale horizontally by
starting additional consumer processes against the same group --- Redis
will automatically distribute new stream entries across all active
consumers. Different streams can also be scaled independently of one
another, since each has its own group and consumer population.

27\. Security Design

-   Redis authentication (requirepass or ACLs) to prevent
    unauthenticated access to stream data

-   Qdrant access control (API keys) for its HTTP and gRPC interfaces

-   SSH key-based authentication for all remote server access, with
    password authentication disabled

-   Secure environment variables for connection strings, API keys, and
    email credentials

-   No hardcoded passwords or secrets anywhere in source code

-   Protected email credentials, loaded from environment variables or a
    secrets manager

-   Firewall configuration restricting inbound access to only required
    ports

-   Restricted ports --- Redis and Qdrant ports are not exposed
    publicly, only reachable via SSH tunnel or internal network

-   TLS for production deployments, particularly for any externally
    reachable endpoint

Secrets are always stored outside source code, using environment
variables or a dedicated secrets manager, and are never committed to
version control.

28\. Observability Design

Logging is structured around three severity levels:

+-----------------------------------------------------------------------+
| INFO                                                                  |
|                                                                       |
| WARNING                                                               |
|                                                                       |
| ERROR                                                                 |
+-----------------------------------------------------------------------+

Key events emitted at these levels include:

+-----------------------------------------------------------------------+
| Message Received                                                      |
|                                                                       |
| Validation Failed                                                     |
|                                                                       |
| Retry Started                                                         |
|                                                                       |
| Retry Completed                                                       |
|                                                                       |
| Embedding Generated                                                   |
|                                                                       |
| Qdrant Upsert Successful                                              |
|                                                                       |
| Search Started                                                        |
|                                                                       |
| Search Completed                                                      |
|                                                                       |
| DLQ Published                                                         |
|                                                                       |
| Email Sent                                                            |
|                                                                       |
| Message Acknowledged                                                  |
+-----------------------------------------------------------------------+

These logs support four operational needs:

-   Debugging individual message failures

-   Monitoring overall system health

-   Investigating incidents after the fact

-   Analyzing throughput and latency over time

29\. Design Decisions

  -----------------------------------------------------------------------
  **Decision**          **Reason**
  --------------------- -------------------------------------------------
  Redis Streams         Asynchronous message processing

  Consumer Groups       Reliable distributed consumption

  Python                Flexible processing and ML integration

  Embeddings            Semantic representation of text

  Qdrant                Vector similarity search

  Docker                Consistent infrastructure deployment

  Retry                 Temporary failure recovery

  DLQ                   Permanent failure isolation

  Separate Streams      Independent processing domains

  Search Stream         Asynchronous search request processing
  -----------------------------------------------------------------------

30\. Future Improvements

-   Multiple consumers per group for higher throughput

-   Priority queues instead of a single priority field

-   Redis Sentinel or Redis Cluster for high availability

-   Qdrant clustering for larger collections

-   A REST API for producers instead of direct stream access

-   A web UI for search

-   Authentication and authorization for administrative operations

-   HTTPS for all externally reachable endpoints

-   Metrics using Prometheus

-   Dashboards using Grafana

-   Automatic DLQ reprocessing

-   A dedicated scheduler for execute_at handling at scale

-   Horizontal scaling across multiple hosts

-   Kubernetes-based deployment

-   Centralized logging

-   Advanced ranking and filtering in search

31\. Final Architecture Summary

Data ingestion:

+-----------------------------------------------------------------------+
| Producer                                                              |
|                                                                       |
| \|                                                                    |
|                                                                       |
| v                                                                     |
|                                                                       |
| Redis Stream                                                          |
|                                                                       |
| \|                                                                    |
|                                                                       |
| v                                                                     |
|                                                                       |
| Consumer Group                                                        |
|                                                                       |
| \|                                                                    |
|                                                                       |
| v                                                                     |
|                                                                       |
| Python Consumer                                                       |
|                                                                       |
| \|                                                                    |
|                                                                       |
| v                                                                     |
|                                                                       |
| Validation                                                            |
|                                                                       |
| \|                                                                    |
|                                                                       |
| v                                                                     |
|                                                                       |
| Priority + Scheduling                                                 |
|                                                                       |
| \|                                                                    |
|                                                                       |
| v                                                                     |
|                                                                       |
| Retry / DLQ                                                           |
|                                                                       |
| \|                                                                    |
|                                                                       |
| v                                                                     |
|                                                                       |
| Embedding Model                                                       |
|                                                                       |
| \|                                                                    |
|                                                                       |
| v                                                                     |
|                                                                       |
| Qdrant                                                                |
|                                                                       |
| \|                                                                    |
|                                                                       |
| v                                                                     |
|                                                                       |
| Email                                                                 |
|                                                                       |
| \|                                                                    |
|                                                                       |
| v                                                                     |
|                                                                       |
| XACK                                                                  |
+-----------------------------------------------------------------------+

Semantic search:

+-----------------------------------------------------------------------+
| Search Query                                                          |
|                                                                       |
| \|                                                                    |
|                                                                       |
| v                                                                     |
|                                                                       |
| search_stream                                                         |
|                                                                       |
| \|                                                                    |
|                                                                       |
| v                                                                     |
|                                                                       |
| Search Consumer                                                       |
|                                                                       |
| \|                                                                    |
|                                                                       |
| v                                                                     |
|                                                                       |
| Query Embedding                                                       |
|                                                                       |
| \|                                                                    |
|                                                                       |
| v                                                                     |
|                                                                       |
| Qdrant Similarity Search                                              |
|                                                                       |
| \|                                                                    |
|                                                                       |
| v                                                                     |
|                                                                       |
| Top-K Results                                                         |
|                                                                       |
| \|                                                                    |
|                                                                       |
| v                                                                     |
|                                                                       |
| Result Stream / Terminal                                              |
+-----------------------------------------------------------------------+

The architecture cleanly separates six concerns, each owned by a
distinct layer:

+-----------------------------------------------------------------------+
| Message Transport                                                     |
|                                                                       |
| \|                                                                    |
|                                                                       |
| v                                                                     |
|                                                                       |
| Business Processing                                                   |
|                                                                       |
| \|                                                                    |
|                                                                       |
| v                                                                     |
|                                                                       |
| Embedding Generation                                                  |
|                                                                       |
| \|                                                                    |
|                                                                       |
| v                                                                     |
|                                                                       |
| Vector Storage                                                        |
|                                                                       |
| \|                                                                    |
|                                                                       |
| v                                                                     |
|                                                                       |
| Semantic Search                                                       |
|                                                                       |
| \|                                                                    |
|                                                                       |
| v                                                                     |
|                                                                       |
| Failure Recovery                                                      |
+-----------------------------------------------------------------------+

This document is the architectural counterpart to the Operational
Document: where the Operational Document explains how to run, monitor,
and troubleshoot the system, this document explains how the system is
designed and why each component exists, how it interacts with the rest
of the system, what data it consumes and produces, and how it behaves
under failure.
