**TECHNICAL DOCUMENTATION**

*Redis Streams-Based Asynchronous Data Processing and Qdrant Semantic
Search System*

Implementation-Level Technical Reference

1\. Technical Document Overview

This project is an asynchronous, event-driven processing system built on
Redis Streams for message transport and Qdrant for vector storage and
semantic search. Documents are published to a stream, consumed by a
Python worker, validated, scheduled, embedded into vectors, stored in
Qdrant, and confirmed by email --- with automatic retry and Dead Letter
Queue (DLQ) handling on failure. A parallel path lets users submit
natural-language search commands that are embedded and matched against
stored vectors.

1.1 Document Processing Flow

> Producer (producer.py)
>
> ↓ XADD
>
> Redis Stream (document_stream / student_stream / employee_stream)
>
> ↓ XREADGROUP
>
> Redis Consumer Group (document_group / student_group / employee_group)
>
> ↓
>
> Python Consumer (consumer.py)
>
> ↓
>
> Validation (validation.py)
>
> ↓
>
> Priority Sort + execute_at Scheduling Check
>
> ↓
>
> Embedding Generation (embedding_service.py)
>
> ↓
>
> Qdrant Vector Upsert (qdrant_service.py / qdrant_helper.py)
>
> ↓
>
> Email Notification (email_service.py)
>
> ↓
>
> Redis XACK

1.2 Semantic Search Flow

> Search Query (producer.py search)
>
> ↓ XADD command_stream
>
> Search Consumer (search_consumer.py)
>
> ↓
>
> Validate Search Command (validation.py)
>
> ↓
>
> Query Embedding (embedding_service.py)
>
> ↓
>
> Qdrant Similarity Search (search_service.py)
>
> ↓
>
> Top-K Results
>
> ↓
>
> Result Stream (result_stream)

1.3 Technologies Used

  -----------------------------------------------------------------------
  **Technology**          **Purpose**
  ----------------------- -----------------------------------------------
  Python 3                Implements all producer, consumer, and service
                          modules

  Redis Streams           Durable, ordered, asynchronous message queues

  Redis Consumer Groups   At-least-once, load-balanced message
                          consumption with XACK/XPENDING/XCLAIM

  Qdrant                  Vector database used for storage and
                          cosine-similarity search

  Sentence Transformers   Generates 384-dimensional text embeddings
                          (all-MiniLM-L6-v2)

  Docker / docker-compose Runs Redis and Qdrant as isolated containers

  SMTP (smtplib)          Sends success/failure email notifications

  python-dotenv           Loads environment variables from a .env file
                          into config.py
  -----------------------------------------------------------------------

2\. System Technology Stack

  -----------------------------------------------------------------------------------
  **Layer**          **Choice**                               **Why it is used here**
  ------------------ ---------------------------------------- -----------------------
  Operating System   Linux server                             Hosts the venv, the
                                                              Docker daemon, and both
                                                              containers

  Language           Python 3                                 All modules
                                                              (consumer.py,
                                                              producer.py, services)
                                                              are plain Python

  Message Broker     Redis Streams                            Append-only log
                                                              semantics give
                                                              ordering, replay, and
                                                              consumer groups

  Vector Database    Qdrant                                   Stores 384-dim vectors
                                                              with payload and
                                                              performs cosine search

  Embedding Model    sentence-transformers/all-MiniLM-L6-v2   Configured via
                                                              EMBEDDING_MODEL in
                                                              config.py

  Containerization   Docker Compose                           docker-compose.yml pins
                                                              redis:7-alpine and
                                                              qdrant/qdrant:v1.4.0

  Remote Access      SSH                                      Used to reach the Linux
                                                              server and optionally
                                                              tunnel Redis/Qdrant
                                                              ports

  Environment        Python virtual environment (venv)        Isolates project
                                                              dependencies from
                                                              system Python
  -----------------------------------------------------------------------------------

3\. Project Directory Structure

> IITM/
>
> │
>
> ├── consumer.py \# document/student/employee/search consumer
> entrypoint
>
> ├── search_consumer.py \# dedicated search-command consumer
> (consumer-group based)
>
> ├── producer.py \# CLI producer for documents and search commands
>
> │
>
> ├── config.py \# central configuration (env-driven)
>
> ├── redis_client.py \# Redis connection factory
>
> │
>
> ├── validation.py \# input validation + priority ordering
>
> ├── retry_handler.py \# retry decision and exponential backoff
>
> ├── dlq_handler.py \# Dead Letter Queue publish/reprocess
>
> ├── email_service.py \# SMTP notification sending
>
> │
>
> ├── embedding_service.py \# Sentence-Transformer wrapper
>
> ├── qdrant_helper.py \# Qdrant client + collection bootstrap
>
> ├── qdrant_service.py \# thin re-export facade over qdrant_helper
>
> ├── search_service.py \# Qdrant similarity search via HTTP API
>
> │
>
> ├── dlq_review.py \# CLI to list/reprocess DLQ messages
>
> │
>
> ├── requirements.txt
>
> ├── docker-compose.yml
>
> ├── README.md
>
> └── venv/

3.1 File Responsibility Table

  -------------------------------------------------------------------------
  **File**               **Technical Responsibility**
  ---------------------- --------------------------------------------------
  consumer.py            Main entrypoint for
                         document/student/employee/search consumers;
                         orchestrates validation, embedding, upsert, email,
                         ack

  search_consumer.py     Consumer-group based search command processor
                         writing to result_stream

  producer.py            CLI that publishes document and search payloads
                         via XADD

  config.py              Loads all environment-driven settings in one place

  redis_client.py        Builds and pings the shared Redis connection

  validation.py          Validates document and search fields; defines
                         PRIORITY_ORDER

  retry_handler.py       Decides retry eligibility and computes backoff
                         delay

  dlq_handler.py         Publishes failed messages to the DLQ stream and
                         reprocesses them

  email_service.py       Sends SMTP success/failure notification emails

  embedding_service.py   Loads the SentenceTransformer model and encodes
                         text to vectors

  qdrant_helper.py       Creates the Qdrant client and ensures the
                         collection exists

  qdrant_service.py      Re-exports qdrant_helper symbols for other modules

  search_service.py      Embeds the query and calls the Qdrant HTTP search
                         endpoint

  dlq_review.py          Operator CLI to list DLQ entries and reprocess
                         them into a target stream
  -------------------------------------------------------------------------

3.2 Module Dependency Graph

> consumer.py
>
> │
>
> ├── config.py
>
> ├── validation.py
>
> ├── retry_handler.py
>
> ├── dlq_handler.py
>
> ├── email_service.py
>
> ├── embedding_service.py
>
> ├── qdrant_service.py → qdrant_helper.py
>
> ├── redis_client.py
>
> └── search_service.py

4\. Python Environment Setup

> System Python
>
> │
>
> ▼
>
> Python Virtual Environment (venv)
>
> │
>
> ▼
>
> Project Dependencies (requirements.txt)

Create and activate the virtual environment:

> python3 -m venv venv
>
> source venv/bin/activate

Install dependencies:

> pip install -r requirements.txt

Reasons a virtual environment is used:

-   Dependency isolation from system Python packages

-   Avoids version conflicts with other projects on the same server

-   Reproducible environment across machines

-   Keeps requirements.txt authoritative for this project only

5\. Configuration Design (config.py)

All Redis, stream, consumer group, Qdrant, retry, and SMTP settings are
centralized in config.py and populated from environment variables via
python-dotenv, with sensible defaults.

> from dotenv import load_dotenv
>
> import os
>
> load_dotenv()
>
> REDIS_HOST = os.getenv(\"REDIS_HOST\", \"localhost\")
>
> REDIS_PORT = int(os.getenv(\"REDIS_PORT\", 6379))
>
> DOCUMENT_STREAM = os.getenv(\"DOCUMENT_STREAM\", \"document_stream\")
>
> STUDENT_STREAM = os.getenv(\"STUDENT_STREAM\", \"student_stream\")
>
> EMPLOYEE_STREAM = os.getenv(\"EMPLOYEE_STREAM\", \"employee_stream\")
>
> COMMAND_STREAM = os.getenv(\"COMMAND_STREAM\",
> os.getenv(\"SEARCH_STREAM\", \"command_stream\"))
>
> SEARCH_STREAM = COMMAND_STREAM
>
> RESULT_STREAM = os.getenv(\"RESULT_STREAM\", \"result_stream\")
>
> DLQ_STREAM = os.getenv(\"DLQ_STREAM\", \"dead_letter_stream\")
>
> DOCUMENT_GROUP = os.getenv(\"DOCUMENT_GROUP\", \"document_group\")
>
> DOCUMENT_CONSUMER = os.getenv(\"DOCUMENT_CONSUMER\",
> \"document_consumer_1\")
>
> STUDENT_GROUP = os.getenv(\"STUDENT_GROUP\", \"student_group\")
>
> STUDENT_CONSUMER = os.getenv(\"STUDENT_CONSUMER\",
> \"student_consumer_1\")
>
> EMPLOYEE_GROUP = os.getenv(\"EMPLOYEE_GROUP\", \"employee_group\")
>
> EMPLOYEE_CONSUMER = os.getenv(\"EMPLOYEE_CONSUMER\",
> \"employee_consumer_1\")
>
> SEARCH_GROUP = os.getenv(\"SEARCH_GROUP\", \"search_group\")
>
> SEARCH_CONSUMER = os.getenv(\"SEARCH_CONSUMER\",
> \"search_consumer_1\")
>
> QDRANT_HOST = os.getenv(\"QDRANT_HOST\", \"localhost\")
>
> QDRANT_PORT = int(os.getenv(\"QDRANT_PORT\", 6333))
>
> QDRANT_COLLECTION = os.getenv(\"QDRANT_COLLECTION\", \"documents\")
>
> EMBEDDING_MODEL = os.getenv(\"EMBEDDING_MODEL\",
> \"sentence-transformers/all-MiniLM-L6-v2\")
>
> VECTOR_SIZE = int(os.getenv(\"VECTOR_SIZE\", 384))
>
> MAX_RETRIES = int(os.getenv(\"MAX_RETRIES\", 3))
>
> SMTP_HOST = os.getenv(\"SMTP_HOST\", \"smtp.gmail.com\")
>
> SMTP_PORT = int(os.getenv(\"SMTP_PORT\", 587))
>
> SMTP_USERNAME = os.getenv(\"SMTP_USERNAME\", \"\")
>
> SMTP_PASSWORD = os.getenv(\"SMTP_PASSWORD\", \"\")
>
> EMAIL_TO = os.getenv(\"EMAIL_TO\", \"\")
>
> \# Consumer behavior
>
> REDIS_BLOCK_MS = int(os.getenv(\"REDIS_BLOCK_MS\", 5000))
>
> PENDING_CLAIM_IDLE_MS = int(os.getenv(\"PENDING_CLAIM_IDLE_MS\",
> 60000))
>
> DEFER_CHECK_INTERVAL_SECONDS =
> int(os.getenv(\"DEFER_CHECK_INTERVAL_SECONDS\", 5))

*config.py --- full source*

Note the search/command stream aliasing: COMMAND_STREAM falls back to
SEARCH_STREAM, and SEARCH_STREAM is then bound to COMMAND_STREAM, so
both names resolve to the same underlying Redis stream throughout the
codebase.

5.1 Logical Configuration Model

> config.py
>
> │
>
> ├── Redis Settings (REDIS_HOST, REDIS_PORT)
>
> ├── Stream Settings (DOCUMENT_STREAM, COMMAND_STREAM, RESULT_STREAM,
> DLQ_STREAM, \...)
>
> ├── Consumer Group Settings (DOCUMENT_GROUP/CONSUMER,
> SEARCH_GROUP/CONSUMER, \...)
>
> ├── Qdrant Settings (QDRANT_HOST, QDRANT_PORT, QDRANT_COLLECTION,
> VECTOR_SIZE)
>
> ├── Retry Settings (MAX_RETRIES)
>
> ├── SMTP Settings (SMTP_HOST/PORT/USERNAME/PASSWORD, EMAIL_TO)
>
> └── Consumer Behavior (REDIS_BLOCK_MS, PENDING_CLAIM_IDLE_MS,
> DEFER_CHECK_INTERVAL_SECONDS)

Centralizing configuration avoids the same value (e.g. QDRANT_COLLECTION
or MAX_RETRIES) being hardcoded differently across consumer.py,
search_service.py, and qdrant_helper.py, which would otherwise drift out
of sync.

6\. Redis Client Implementation (redis_client.py)

> import logging
>
> from redis import Redis
>
> from redis.exceptions import RedisError
>
> from config import REDIS_HOST, REDIS_PORT
>
> logger = logging.getLogger(\_\_name\_\_)
>
> def get_redis_client() -\> Redis:
>
> try:
>
> client = Redis(
>
> host=REDIS_HOST,
>
> port=REDIS_PORT,
>
> decode_responses=True,
>
> socket_timeout=30, \# 30 second timeout for socket operations
>
> socket_keepalive=True,
>
> )
>
> client.ping()
>
> logger.info(\"Connected to Redis at %s:%s\", REDIS_HOST, REDIS_PORT)
>
> return client
>
> except RedisError as exc:
>
> logger.exception(\"Failed to connect to Redis: %s\", exc)
>
> raise

*redis_client.py --- full source*

get_redis_client() returns a single, reusable connection with
decode_responses=True (so stream fields arrive as Python str, not
bytes), a 30-second socket timeout, and keepalive enabled. client.ping()
fails fast at startup if Redis is unreachable rather than surfacing
errors later during XADD/XREADGROUP calls.

> Python Application
>
> │
>
> ▼
>
> get_redis_client() → Redis(\...) → client.ping()
>
> │
>
> ▼
>
> Redis Server (REDIS_HOST:REDIS_PORT)
>
> │
>
> ▼
>
> XADD / XREADGROUP / XACK / XPENDING / XCLAIM

7\. Redis Streams Implementation

Streams actually used in this project (from config.py):

> document_stream (DOCUMENT_STREAM)
>
> student_stream (STUDENT_STREAM)
>
> employee_stream (EMPLOYEE_STREAM)
>
> command_stream (COMMAND_STREAM, aliased as SEARCH_STREAM)
>
> result_stream (RESULT_STREAM)
>
> dead_letter_stream (DLQ_STREAM)

Each stream entry is an auto-ID, ordered log record made of flat
field/value string pairs. Example document entry as produced by
producer.py:

> document_id = DOC3001
>
> title = Redis Streams
>
> text = Redis Streams allow asynchronous processing
>
> priority = HIGH
>
> execute_at = 2026-07-28 12:00:00
>
> retry_count = 0

Entries are appended with XADD using an auto-generated ID (the \*
argument), guaranteeing monotonically increasing, unique message IDs
that consumers use for XACK/XCLAIM.

8\. Producer Technical Implementation (producer.py)

> import argparse
>
> import logging
>
> from redis_client import get_redis_client
>
> from datetime import datetime
>
> from config import COMMAND_STREAM, DOCUMENT_STREAM
>
> logger = logging.getLogger(\_\_name\_\_)
>
> def publish_document(redis_client, document_id: str, title: str, text:
> str, priority: str, execute_at: str) -\> str:
>
> payload = {
>
> \"document_id\": document_id,
>
> \"title\": title,
>
> \"text\": text,
>
> \"priority\": priority.upper(),
>
> \"execute_at\": execute_at,
>
> \"retry_count\": \"0\",
>
> }
>
> message_id = redis_client.xadd(DOCUMENT_STREAM, payload)
>
> logger.info(\"Published document message %s\", message_id)
>
> return message_id
>
> def publish_search(redis_client, query: str, top_k: int) -\> str:
>
> payload = {
>
> \"command\": \"search\",
>
> \"query\": query,
>
> \"top_k\": str(top_k),
>
> }
>
> message_id = redis_client.xadd(COMMAND_STREAM, payload)
>
> logger.info(\"Published search command %s\", message_id)
>
> return message_id

*producer.py --- publish_document() and publish_search()*

The CLI (argparse) exposes two subcommands. Document publishing example:

> python producer.py document \\
>
> \--document-id DOC001 \\
>
> \--title \"AI\" \\
>
> \--text \"Artificial intelligence is a field of computer science.\" \\
>
> \--priority HIGH \\
>
> \--execute-at \"2026-07-28 12:00:00\"

Search publishing example:

> python producer.py search \--query \"What is artificial
> intelligence?\" \--top-k 5

Note retry_count is always initialized to \"0\" by the producer;
validation.py and retry_handler.py read and increment this field on the
consumer side.

9\. Consumer Group Implementation

> def create_consumer_group(redis_client, stream_name, group_name):
>
> try:
>
> redis_client.xgroup_create(stream_name, group_name, id=\"\$\",
> mkstream=True)
>
> logger.info(\"Created Redis consumer group %s on stream %s\",
> group_name, stream_name)
>
> except Exception as exc:
>
> if \"BUSYGROUP\" in str(exc):
>
> logger.info(\"Redis consumer group %s already exists\", group_name)
>
> else:
>
> logger.exception(\"Error creating consumer group: %s\", exc)
>
> raise

*consumer.py --- create_consumer_group()*

id=\"\$\" means the group starts consuming only new messages appended
after the group is created; mkstream=True auto-creates the stream if it
does not exist yet. A BUSYGROUP exception is caught and treated as a
normal, idempotent no-op --- the group already exists from a previous
run.

> Redis Stream
>
> │
>
> ▼
>
> XGROUP CREATE stream group \$ MKSTREAM
>
> │
>
> ▼
>
> Consumer Group
>
> │
>
> ▼
>
> XREADGROUP GROUP group consumer STREAMS stream \>
>
> │
>
> ▼
>
> Message Delivered → Process → XACK stream group message_id

XACK is only called after successful processing (or after a message has
been safely requeued for retry, or archived to the DLQ) --- never before
--- so a crash mid-processing leaves the message pending and eligible
for XCLAIM rather than silently lost.

10\. Main Consumer Implementation (consumer.py)

> Start Application
>
> │
>
> ▼
>
> Configure Logging (logging.basicConfig)
>
> │
>
> ▼
>
> Connect Redis (get_redis_client)
>
> │
>
> ▼
>
> Connect Qdrant (get_qdrant_client) + ensure_collection
>
> │
>
> ▼
>
> Load Embedding Model (EmbeddingService())
>
> │
>
> ▼
>
> Create Consumer Group (create_consumer_group)
>
> │
>
> ▼
>
> Loop: claim_pending → XREADGROUP → sort by priority/execute_at →
> process_message

10.1 run_stream_consumer() --- the shared loop

> def run_stream_consumer(stream_name, group_name, consumer_name,
> entity_type=\"document\"):
>
> redis_client = get_redis_client()
>
> qdrant_client = get_qdrant_client()
>
> ensure_collection(qdrant_client)
>
> create_consumer_group(redis_client, stream_name, group_name)
>
> embedding_service = EmbeddingService()
>
> logger.info(\"Starting %s consumer\", entity_type)
>
> while True:
>
> try:
>
> claim_pending(redis_client, qdrant_client, embedding_service,
> stream_name, group_name, consumer_name, entity_type)
>
> messages = redis_client.xreadgroup(
>
> groupname=group_name,
>
> consumername=consumer_name,
>
> streams={stream_name: \"\>\"},
>
> count=10,
>
> block=REDIS_BLOCK_MS,
>
> )
>
> if not messages:
>
> continue
>
> for \_, entries in messages:
>
> entries.sort(
>
> key=lambda item: (
>
> PRIORITY_ORDER.get(item\[1\].get(\"priority\", \"MEDIUM\"), 1),
>
> item\[1\].get(\"execute_at\", \"\") or \"\", \# Handle None values
>
> )
>
> )
>
> for message_id, fields in entries:
>
> logger.info(\"Received %s message %s\", entity_type, message_id)
>
> process_message(redis_client, qdrant_client, embedding_service,
> message_id, fields, stream_name, group_name, entity_type)
>
> except KeyboardInterrupt:
>
> logger.info(\"%s consumer stopped by user\", entity_type.title())
>
> break
>
> except Exception as exc:
>
> logger.exception(\"Consumer error: %s\", exc)
>
> sleep(5)

*consumer.py --- run_stream_consumer()*

This single generic loop is reused for the document, student, and
employee entity types simply by parameterizing stream_name, group_name,
consumer_name, and entity_type --- avoiding three near-duplicate
consumer implementations.

10.2 process_message() --- validation, embedding, upsert, ack

> def process_message(redis_client, qdrant_client, embedding_service,
> message_id, fields, stream_name, group_name,
> entity_type=\"document\"):
>
> try:
>
> validated = validate_document_message(fields)
>
> \# If execute_at is provided, check if we should defer processing
>
> if validated\[\"execute_at\"\] is not None:
>
> now = datetime.now()
>
> if validated\[\"execute_at\"\] \> now:
>
> logger.info(\"Deferring message %s until execute_at %s\", message_id,
> validated\[\"execute_at\"\])
>
> return False
>
> else:
>
> \# execute_at not provided - use current time for payload timestamp
>
> validated\[\"execute_at\"\] = datetime.now()
>
> validated\[\"vector\"\] =
> embedding_service.embed_text(validated\[\"text\"\])
>
> entity_id = validated.get(f\"{entity_type}\_id\") or
> validated.get(\"document_id\")
>
> upsert_point(qdrant_client, entity_id, validated, entity_type)
>
> send_email(
>
> f\"{entity_type.title()} inserted: {entity_id}\",
>
> f\"{entity_type.title()} {entity_id} inserted into Qdrant collection
> successfully.\",
>
> )
>
> redis_client.xack(stream_name, group_name, message_id)
>
> logger.info(\"Acknowledged Redis message %s\", message_id)
>
> return True
>
> except Exception as exc:
>
> error_message = str(exc)
>
> retry_count = int(fields.get(\"retry_count\", \"0\"))
>
> if should_retry(retry_count):
>
> retry_payload = build_retry_payload(fields, error_message)
>
> delay_seconds = get_retry_delay_seconds(retry_count)
>
> logger.warning(
>
> \"Retrying message %s after %s seconds (attempt %s): %s\",
>
> message_id, delay_seconds, retry_count + 1, error_message,
>
> )
>
> sleep(delay_seconds)
>
> redis_client.xadd(stream_name, retry_payload)
>
> redis_client.xack(stream_name, group_name, message_id)
>
> logger.info(\"Requeued failed message %s for retry\", message_id)
>
> return True
>
> else:
>
> dlq_id = publish_to_dlq(redis_client, stream_name, message_id, fields,
> error_message)
>
> redis_client.xack(stream_name, group_name, message_id)
>
> send_email(
>
> f\"{entity_type.title()} failed permanently\",
>
> f\"Message {message_id} failed permanently and was sent to DLQ
> {dlq_id}. Error: {error_message}\",
>
> )
>
> logger.error(\"Moved message %s to DLQ %s\", message_id, dlq_id)
>
> return True

*consumer.py --- process_message()*

A message that is not yet due (execute_at in the future) returns False
without acking --- it is deliberately left pending in the stream and is
picked up again on the next poll or by claim_pending once its idle time
exceeds PENDING_CLAIM_IDLE_MS. On any other exception, the retry/DLQ
decision is delegated to retry_handler.should_retry().

10.3 upsert_point() --- building and writing the Qdrant point

> def deterministic_point_id(document_id: str) -\> str:
>
> return str(uuid.uuid5(uuid.NAMESPACE_URL, document_id))
>
> def upsert_point(qdrant_client: QdrantClient, entity_id: str,
> entity_data: dict, entity_type: str = \"document\") -\> None:
>
> point_id = deterministic_point_id(entity_id)
>
> timestamp = entity_data\[\"execute_at\"\]
>
> if timestamp is None:
>
> timestamp = datetime.now(timezone.utc)
>
> elif timestamp.tzinfo is None:
>
> timestamp = timestamp.replace(tzinfo=timezone.utc)
>
> payload = {
>
> f\"{entity_type}\_id\": entity_id,
>
> \"title\": entity_data\[\"title\"\],
>
> \"text\": entity_data\[\"text\"\],
>
> \"priority\": entity_data\[\"priority\"\],
>
> \"created_at\": timestamp.astimezone(timezone.utc).isoformat(),
>
> \"type\": entity_type,
>
> }
>
> qdrant_client.upsert(
>
> collection_name=QDRANT_COLLECTION,
>
> points=\[{\"id\": point_id, \"vector\": entity_data\[\"vector\"\],
> \"payload\": payload}\],
>
> )
>
> logger.info(\"Upserted %s point %s into Qdrant\", entity_type,
> entity_id)

*consumer.py --- deterministic_point_id() and upsert_point()*

10.4 claim_pending() --- recovering stalled messages

> def claim_pending(redis_client, qdrant_client, embedding_service,
> stream_name, group_name, consumer_name, entity_type=\"document\"):
>
> try:
>
> pending_messages = redis_client.xpending_range(
>
> stream_name, group_name, min=\"-\", max=\"+\", count=10
>
> )
>
> if not pending_messages:
>
> return
>
> message_ids = \[msg\[\'message_id\'\] for msg in pending_messages\]
>
> if not message_ids:
>
> return
>
> claimed = redis_client.xclaim(
>
> stream_name, group_name, consumer_name,
>
> min_idle_time=PENDING_CLAIM_IDLE_MS, message_ids=message_ids,
>
> )
>
> for message_id, fields in claimed:
>
> logger.info(\"Claimed and reprocessing pending message %s\",
> message_id)
>
> process_message(redis_client, qdrant_client, embedding_service,
> message_id, fields, stream_name, group_name, entity_type)
>
> except Exception as exc:
>
> logger.exception(\"Failed to claim pending messages: %s\", exc)

*consumer.py --- claim_pending()*

This is the recovery path for a consumer that crashed after XREADGROUP
but before XACK: on every loop iteration, claim_pending() looks at
XPENDING for entries idle longer than PENDING_CLAIM_IDLE_MS and
re-delivers them to the current consumer via XCLAIM before new messages
are read.

10.5 Embedded Search Consumer inside consumer.py

consumer.py also contains run_search_consumer(), an alternate,
XREAD-based (non consumer-group) search loop that reads COMMAND_STREAM
with the special \$ ID (only new messages), validates the command, calls
search_documents(), prints results to stdout, and republishes to
RESULT_STREAM. This is distinct from the consumer-group based
search_consumer.py described in Section 19.

10.6 CLI Entrypoint

> if \_\_name\_\_ == \"\_\_main\_\_\":
>
> import sys
>
> if len(sys.argv) \> 1:
>
> consumer_type = sys.argv\[1\].lower()
>
> if consumer_type == \"search\":
>
> run_search_consumer()
>
> elif consumer_type == \"student\":
>
> run_student_consumer()
>
> elif consumer_type == \"employee\":
>
> run_employee_consumer()
>
> else:
>
> run_document_consumer()
>
> else:
>
> run_document_consumer()

*consumer.py --- CLI dispatch*

> python consumer.py \# document consumer (default)
>
> python consumer.py student \# student consumer
>
> python consumer.py employee \# employee consumer
>
> python consumer.py search \# embedded XREAD-based search loop

11\. Validation Implementation (validation.py)

> PRIORITY_ORDER = {\"HIGH\": 0, \"MEDIUM\": 1, \"LOW\": 2}
>
> def parse_execute_at(value: str) -\> datetime:
>
> \"\"\"Parse execute_at timestamp. If empty or missing, returns None
> for immediate processing.\"\"\"
>
> if not value or not isinstance(value, str):
>
> return None
>
> try:
>
> return datetime.strptime(value, \"%Y-%m-%d %H:%M:%S\")
>
> except ValueError as exc:
>
> logger.exception(\"Invalid execute_at format: %s\", value)
>
> raise ValueError(\"execute_at must be in YYYY-MM-DD HH:MM:SS format\")
> from exc
>
> def validate_document_message(fields: dict\[str, str\]) -\> dict:
>
> document_id = fields.get(\"document_id\")
>
> title = fields.get(\"title\")
>
> text = fields.get(\"text\")
>
> priority = fields.get(\"priority\", \"MEDIUM\").upper()
>
> execute_at = fields.get(\"execute_at\")
>
> retry_count = int(fields.get(\"retry_count\", \"0\"))
>
> if not document_id:
>
> raise ValueError(\"document_id is required\")
>
> if not title:
>
> raise ValueError(\"title is required\")
>
> if not text:
>
> raise ValueError(\"text is required\")
>
> if priority not in PRIORITY_ORDER:
>
> raise ValueError(\"priority must be HIGH, MEDIUM, or LOW\")
>
> parsed_execute_at = parse_execute_at(execute_at)
>
> return {
>
> \"document_id\": document_id,
>
> \"title\": title,
>
> \"text\": text,
>
> \"priority\": priority,
>
> \"execute_at\": parsed_execute_at,
>
> \"retry_count\": retry_count,
>
> }
>
> def validate_search_command(fields: dict\[str, str\]) -\> dict:
>
> command = fields.get(\"command\", \"\").lower()
>
> if command != \"search\":
>
> raise ValueError(\"search command must specify command=search\")
>
> query = fields.get(\"query\")
>
> if not query:
>
> raise ValueError(\"query is required for search commands\")
>
> try:
>
> top_k = int(fields.get(\"top_k\", \"5\"))
>
> except ValueError as exc:
>
> raise ValueError(\"top_k must be an integer\") from exc
>
> if top_k \<= 0:
>
> raise ValueError(\"top_k must be a positive integer\")
>
> return {\"query\": query, \"top_k\": top_k}

*validation.py --- full source*

Required document fields are document_id, title, and text; priority,
execute_at, and retry_count are optional and default to MEDIUM, None
(immediate), and 0 respectively. Any raised ValueError propagates up to
process_message(), which routes it into the retry/DLQ path.

12\. Priority Processing

Priority is a plain string field (HIGH, MEDIUM, LOW) stored directly on
the Redis Stream entry and echoed into the Qdrant payload.
PRIORITY_ORDER in validation.py maps it to a numeric sort key:

> PRIORITY_ORDER = {\"HIGH\": 0, \"MEDIUM\": 1, \"LOW\": 2}

Inside run_stream_consumer(), each batch of messages returned by
XREADGROUP is sorted before processing:

> entries.sort(
>
> key=lambda item: (
>
> PRIORITY_ORDER.get(item\[1\].get(\"priority\", \"MEDIUM\"), 1),
>
> item\[1\].get(\"execute_at\", \"\") or \"\",
>
> )
>
> )

This gives HIGH \> MEDIUM \> LOW ordering within each fetched batch
(count=10), with execute_at as the tiebreaker. It is a local, per-batch
reordering --- not a global priority queue across the whole stream ---
since Redis Streams themselves are strictly append-ordered.

13\. Scheduled Processing (execute_at)

> Message
>
> │
>
> ▼
>
> parse_execute_at(value) → datetime or None
>
> │
>
> ▼
>
> if execute_at is not None and execute_at \> datetime.now():
>
> Message Not Ready → return False (left pending, no XACK)
>
> else:
>
> Process Message Now

execute_at is parsed from the strict format YYYY-MM-DD HH:MM:SS using
datetime.strptime. In process_message(), it is compared against the
naive local datetime.now() (note: this comparison is naive-vs-naive; the
timezone normalization to UTC happens later, only when the payload is
written into Qdrant in upsert_point()). If the message is not yet due,
it is simply left unacknowledged in the stream so a later poll or
claim_pending() call can pick it up again.

14\. Embedding Service Technical Implementation (embedding_service.py)

> import logging
>
> from sentence_transformers import SentenceTransformer
>
> from config import EMBEDDING_MODEL
>
> logger = logging.getLogger(\_\_name\_\_)
>
> class EmbeddingService:
>
> def \_\_init\_\_(self):
>
> logger.info(\"Loading embedding model: %s\", EMBEDDING_MODEL)
>
> self.model = SentenceTransformer(EMBEDDING_MODEL)
>
> logger.info(\"Embedding model loaded\")
>
> def embed_text(self, text: str) -\> list\[float\]:
>
> if not text or not isinstance(text, str):
>
> raise ValueError(\"Text must be a non-empty string for embedding\")
>
> embedding = self.model.encode(text, convert_to_numpy=True).tolist()
>
> logger.debug(\"Generated embedding vector of size %s\",
> len(embedding))
>
> return embedding

*embedding_service.py --- full source*

The model (default sentence-transformers/all-MiniLM-L6-v2, 384
dimensions, matching VECTOR_SIZE in config.py) is loaded once in the
constructor when EmbeddingService() is instantiated at consumer startup
--- not per message --- so model load time (a few seconds) is paid once
per process rather than per document.

> EmbeddingService()
>
> │
>
> ▼
>
> SentenceTransformer(EMBEDDING_MODEL) ← loaded once, cached on
> self.model
>
> │
>
> ▼
>
> embed_text(text)
>
> │
>
> ▼
>
> self.model.encode(text, convert_to_numpy=True).tolist()
>
> │
>
> ▼
>
> list\[float\] of length VECTOR_SIZE (384)

15\. Qdrant Connection Implementation (qdrant_helper.py)

> from config import QDRANT_HOST, QDRANT_PORT, QDRANT_COLLECTION,
> VECTOR_SIZE
>
> try:
>
> from qdrant_client import QdrantClient
>
> from qdrant_client.http import models as rest
>
> except ImportError:
>
> \# Fallback to custom import mechanism if standard import fails
>
> import importlib.util
>
> import site
>
> import sys
>
> from pathlib import Path
>
> def \_import_external_qdrant_package():
>
> search_paths = \[\]
>
> try:
>
> search_paths.extend(site.getsitepackages())
>
> except Exception:
>
> pass
>
> try:
>
> search_paths.append(site.getusersitepackages())
>
> except Exception:
>
> pass
>
> for base in search_paths:
>
> candidate = Path(base) / \"qdrant_client\"
>
> if candidate.is_dir():
>
> init_path = candidate / \"\_\_init\_\_.py\"
>
> if init_path.exists():
>
> spec = importlib.util.spec_from_file_location(\"qdrant_client\",
> str(init_path))
>
> if spec and spec.loader:
>
> module = importlib.util.module_from_spec(spec)
>
> sys.modules\[\"qdrant_client\"\] = module
>
> spec.loader.exec_module(module)
>
> return module
>
> elif candidate.with_suffix(\".py\").exists():
>
> file_path = candidate.with_suffix(\".py\")
>
> spec = importlib.util.spec_from_file_location(\"qdrant_client\",
> str(file_path))
>
> if spec and spec.loader:
>
> module = importlib.util.module_from_spec(spec)
>
> sys.modules\[\"qdrant_client\"\] = module
>
> spec.loader.exec_module(module)
>
> return module
>
> raise ImportError(\"Could not import the external qdrant_client
> package from site-packages\")
>
> \_qdrant_pkg = \_import_external_qdrant_package()
>
> QdrantClient = \_qdrant_pkg.QdrantClient
>
> rest = \_qdrant_pkg.http.models
>
> def get_qdrant_client() -\> QdrantClient:
>
> try:
>
> client = QdrantClient(url=f\"http://{QDRANT_HOST}:{QDRANT_PORT}\")
>
> logger.info(\"Connected to Qdrant at %s:%s\", QDRANT_HOST,
> QDRANT_PORT)
>
> return client
>
> except Exception as exc:
>
> logger.exception(\"Failed to connect to Qdrant: %s\", exc)
>
> raise
>
> def ensure_collection(client: QdrantClient) -\> None:
>
> try:
>
> if not client.get_collection(collection_name=QDRANT_COLLECTION):
>
> raise ValueError(\"Collection does not exist\")
>
> except Exception:
>
> logger.info(\"Creating Qdrant collection %s\", QDRANT_COLLECTION)
>
> client.recreate_collection(
>
> collection_name=QDRANT_COLLECTION,
>
> vectors=rest.VectorsConfig(size=VECTOR_SIZE,
> distance=rest.Distance.COSINE),
>
> )
>
> logger.info(\"Qdrant collection %s created\", QDRANT_COLLECTION)

*qdrant_helper.py --- full source*

The standard import is attempted first; only if qdrant_client cannot be
imported normally does the module fall back to manually locating and
loading the package from site-packages via importlib.
get_qdrant_client() connects over HTTP to QDRANT_HOST:QDRANT_PORT.
ensure_collection() attempts get_collection(); any exception (including
a missing collection) is treated as \"does not exist yet\" and triggers
recreate_collection() with COSINE distance and VECTOR_SIZE dimensions
--- this must be created before any upsert or search call.

15.1 qdrant_service.py --- facade

> from qdrant_helper import get_qdrant_client, ensure_collection,
> QdrantClient
>
> \_\_all\_\_ = \[\"get_qdrant_client\", \"ensure_collection\",
> \"QdrantClient\"\]

*qdrant_service.py --- full source*

consumer.py and search_consumer.py import from qdrant_service rather
than qdrant_helper directly, giving a stable internal import path if the
underlying helper implementation changes.

16\. Qdrant Collection Design

Collection name: documents (QDRANT_COLLECTION, config.py).

  -----------------------------------------------------------------------
  **Property**              **Value / Source**
  ------------------------- ---------------------------------------------
  Vector dimension          VECTOR_SIZE = 384, must match
                            EmbeddingService output exactly

  Distance metric           COSINE (rest.Distance.COSINE)

  Point ID                  Deterministic UUID5 from entity_id
                            (deterministic_point_id())

  Payload fields            {entity_type}\_id, title, text, priority,
                            created_at, type
  -----------------------------------------------------------------------

The configured vector dimension (VECTOR_SIZE) must exactly match the
embedding model\'s actual output dimension --- all-MiniLM-L6-v2 emits
384-dimensional vectors, matching the default. Changing EMBEDDING_MODEL
to a model with a different output size requires updating VECTOR_SIZE
and recreating the collection.

17\. Document Upsert Implementation

> Document Fields (validated)
>
> │
>
> ▼
>
> Extract text
>
> │
>
> ▼
>
> embedding_service.embed_text(text) → vector
>
> │
>
> ▼
>
> deterministic_point_id(entity_id) → uuid.uuid5(NAMESPACE_URL,
> entity_id)
>
> │
>
> ▼
>
> Create Point { id, vector, payload }
>
> │
>
> ▼
>
> qdrant_client.upsert(collection_name=QDRANT_COLLECTION,
> points=\[point\])

Point IDs are derived deterministically from the business key
(document_id / student_id / employee_id) using uuid.uuid5 with the
NAMESPACE_URL namespace. Because the same input string always yields the
same UUID, re-processing the same document_id (e.g. on retry, or if the
producer republishes it) upserts the same Qdrant point rather than
creating a duplicate vector record.

18\. Search Service Implementation (search_service.py)

> import logging
>
> import json
>
> from urllib.request import Request, urlopen
>
> from qdrant_service import QdrantClient
>
> from config import QDRANT_COLLECTION, QDRANT_HOST, QDRANT_PORT
>
> from embedding_service import EmbeddingService
>
> logger = logging.getLogger(\_\_name\_\_)
>
> def search_documents(qdrant_client: QdrantClient, embedding_service:
> EmbeddingService, query: str, top_k: int) -\> list\[dict\]:
>
> query_vector = embedding_service.embed_text(query)
>
> logger.info(\"Performing semantic search for query: %s\", query)
>
> request = Request(
>
> url=f\"http://{QDRANT_HOST}:{QDRANT_PORT}/collections/{QDRANT_COLLECTION}/points/search\",
>
> data=json.dumps({
>
> \"vector\": query_vector,
>
> \"limit\": top_k,
>
> \"with_payload\": True,
>
> \"with_vector\": False,
>
> }).encode(\"utf-8\"),
>
> headers={\"Content-Type\": \"application/json\"},
>
> method=\"POST\",
>
> )
>
> with urlopen(request, timeout=30) as response:
>
> search_result =
> json.loads(response.read().decode(\"utf-8\"))\[\"result\"\]
>
> formatted = \[
>
> {\"id\": str(hit\[\"id\"\]), \"score\": float(hit\[\"score\"\]),
> \"payload\": hit.get(\"payload\", {})}
>
> for hit in search_result
>
> \]
>
> logger.info(\"Search returned %d results\", len(formatted))
>
> return formatted

*search_service.py --- full source*

Rather than calling qdrant_client\'s own .search() method, this module
builds the Qdrant HTTP request manually with urllib and POSTs directly
to /collections/documents/points/search --- this is why qdrant_service
is imported only for its QdrantClient type hint here, while the actual
network call bypasses the client library. with_vector is set to False so
the (potentially large) stored vector is not returned, only score and
payload.

19\. Search Consumer Implementation (search_consumer.py)

> command_stream (SEARCH_STREAM)
>
> │
>
> ▼
>
> XGROUP CREATE search_group \$ MKSTREAM (create_search_group)
>
> │
>
> ▼
>
> XREADGROUP GROUP search_group CONSUMER search_consumer_1
>
> │
>
> ▼
>
> validate_search_command(fields)
>
> │
>
> ▼
>
> search_documents(qdrant_client, embedding_service, query, top_k)
>
> │
>
> ▼
>
> format_search_result(query, results)
>
> │
>
> ▼
>
> XADD result_stream
>
> │
>
> ▼
>
> XACK command_stream search_group message_id
>
> def process_search(redis_client, qdrant_client, embedding_service,
> message_id, fields):
>
> try:
>
> validated = validate_search_command(fields)
>
> results = search_documents(qdrant_client, embedding_service,
> validated\[\"query\"\], validated\[\"top_k\"\])
>
> payload = format_search_result(validated\[\"query\"\], results)
>
> redis_client.xadd(RESULT_STREAM, payload)
>
> redis_client.xack(SEARCH_STREAM, SEARCH_GROUP, message_id)
>
> logger.info(\"Published search results for message %s\", message_id)
>
> except Exception as exc:
>
> logger.exception(\"Search processing failed for %s: %s\", message_id,
> exc)
>
> redis_client.xack(SEARCH_STREAM, SEARCH_GROUP, message_id)
>
> redis_client.xadd(RESULT_STREAM, {
>
> \"query\": fields.get(\"query\", \"\"),
>
> \"result_count\": \"0\",
>
> \"error\": str(exc),
>
> })

*search_consumer.py --- process_search()*

Unlike the document consumer, a failed search is acknowledged
immediately (no retry/DLQ path) and an error result is written straight
to result_stream --- a failed search is not worth retrying with backoff
since it is a synchronous, user-facing request.

Search command format published by producer.py:

> command = search
>
> query = \"What are Redis Streams?\"
>
> top_k = 5

Result structure written to result_stream: query, result_count, results
(and error on failure).

20\. Retry Handler Implementation (retry_handler.py)

> import logging
>
> from config import MAX_RETRIES
>
> logger = logging.getLogger(\_\_name\_\_)
>
> def should_retry(retry_count: int) -\> bool:
>
> return retry_count \< MAX_RETRIES
>
> def build_retry_payload(fields: dict\[str, str\], error: str) -\>
> dict\[str, str\]:
>
> retry_count = int(fields.get(\"retry_count\", \"0\")) + 1
>
> payload = fields.copy()
>
> payload\[\"retry_count\"\] = str(retry_count)
>
> payload\[\"last_error\"\] = error
>
> return payload
>
> def get_retry_delay_seconds(retry_count: int) -\> int:
>
> return min(5 \* (2 \*\* retry_count), 60)

*retry_handler.py --- full source*

Delay grows exponentially and is capped at 60 seconds:

  -----------------------------------------------------------------------------
  **retry_count at failure**    **get_retry_delay_seconds()**   **Computed
                                                                delay**
  ----------------------------- ------------------------------- ---------------
  0 (attempt 1)                 5 \* 2\^0                       5 seconds

  1 (attempt 2)                 5 \* 2\^1                       10 seconds

  2 (attempt 3)                 5 \* 2\^2                       20 seconds

  3 (attempt 4, blocked by      n/a                             routed to DLQ
  MAX_RETRIES=3)                                                instead
  -----------------------------------------------------------------------------

> Processing Failure
>
> │
>
> ▼
>
> retry_count = int(fields\[\"retry_count\"\])
>
> │
>
> ▼
>
> should_retry(retry_count)?
>
> ┌────┴────┐
>
> YES NO
>
> │ │
>
> ▼ ▼
>
> build_retry_payload() + publish_to_dlq()
>
> sleep(delay) + XADD retry

Note that sleep(delay_seconds) runs synchronously inside
process_message(), meaning the consumer process blocks for the backoff
duration before requeueing --- this is a simple, single-threaded backoff
rather than a scheduled/deferred retry.

21\. Dead Letter Queue Implementation

21.1 dlq_handler.py

> import logging
>
> from datetime import datetime
>
> from config import DLQ_STREAM
>
> logger = logging.getLogger(\_\_name\_\_)
>
> def publish_to_dlq(redis_client, original_stream: str, message_id:
> str, fields: dict\[str, str\], error: str) -\> str:
>
> payload = {
>
> \"original_stream\": original_stream,
>
> \"original_message_id\": message_id,
>
> \"document_id\": fields.get(\"document_id\", \"\"),
>
> \"retry_count\": fields.get(\"retry_count\", \"0\"),
>
> \"error\": error,
>
> \"failed_at\": datetime.utcnow().strftime(\"%Y-%m-%d %H:%M:%S\"),
>
> \"original_data\": str(fields),
>
> }
>
> dlq_id = redis_client.xadd(DLQ_STREAM, payload)
>
> logger.info(\"Published message to DLQ %s\", dlq_id)
>
> return dlq_id
>
> def reprocess_dlq_message(redis_client, dlq_message_id: str,
> target_stream: str) -\> str:
>
> message = redis_client.xrange(DLQ_STREAM, dlq_message_id,
> dlq_message_id)
>
> if not message:
>
> raise ValueError(\"DLQ message not found\")
>
> \_, fields = message\[0\]
>
> target_id = redis_client.xadd(target_stream, fields)
>
> logger.info(\"Reprocessed DLQ message %s to stream %s\",
> dlq_message_id, target_stream)
>
> return target_id

*dlq_handler.py --- full source*

The DLQ entry preserves the original stream, message ID, document ID,
retry count, error text, failure timestamp, and a string dump of the
entire original field dict --- enough for a developer to diagnose and
manually correct the payload.

21.2 dlq_review.py --- operator CLI

> import argparse
>
> import logging
>
> from redis_client import get_redis_client
>
> from dlq_handler import reprocess_dlq_message
>
> logger = logging.getLogger(\_\_name\_\_)
>
> def list_dlq(redis_client):
>
> messages = redis_client.xrange(\"dead_letter_stream\", \"-\", \"+\")
>
> for message_id, fields in messages:
>
> print(message_id, fields)
>
> def reprocess(redis_client, message_id, target_stream):
>
> target_id = reprocess_dlq_message(redis_client, message_id,
> target_stream)
>
> print(f\"Reprocessed DLQ message {message_id} to {target_stream} as
> {target_id}\")
>
> def main():
>
> parser = argparse.ArgumentParser(description=\"DLQ review and
> reprocessing\")
>
> subparsers = parser.add_subparsers(dest=\"command\", required=True)
>
> list_parser = subparsers.add_parser(\"list\", help=\"List all DLQ
> messages\")
>
> reprocess_parser = subparsers.add_parser(\"reprocess\",
> help=\"Reprocess a DLQ message\")
>
> reprocess_parser.add_argument(\"message_id\", help=\"DLQ message ID\")
>
> reprocess_parser.add_argument(\"target_stream\", help=\"Stream to
> re-add the message to\")
>
> args = parser.parse_args()
>
> logging.basicConfig(level=logging.INFO, format=\"%(asctime)s
> %(levelname)s %(message)s\")
>
> redis_client = get_redis_client()
>
> if args.command == \"list\":
>
> list_dlq(redis_client)
>
> elif args.command == \"reprocess\":
>
> reprocess(redis_client, args.message_id, args.target_stream)

*dlq_review.py --- full source*

Usage:

> python dlq_review.py list
>
> python dlq_review.py reprocess \<dlq_message_id\> document_stream
>
> Maximum Retries Reached
>
> │
>
> ▼
>
> publish_to_dlq() → XADD dead_letter_stream
>
> │
>
> ▼
>
> dlq_review.py list ← developer inspects failures
>
> │
>
> ▼
>
> dlq_review.py reprocess \<id\> \<target_stream\> → fixes/replays
> original fields
>
> │
>
> ▼
>
> Message re-enters the normal processing pipeline

22\. Email Service Implementation (email_service.py)

> import logging
>
> import smtplib
>
> from email.mime.multipart import MIMEMultipart
>
> from email.mime.text import MIMEText
>
> from config import SMTP_HOST, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD,
> EMAIL_TO
>
> logger = logging.getLogger(\_\_name\_\_)
>
> def send_email(subject: str, body: str) -\> None:
>
> if not SMTP_USERNAME or not SMTP_PASSWORD or not EMAIL_TO:
>
> logger.warning(\"SMTP credentials or recipient not fully configured;
> skipping email\")
>
> return
>
> message = MIMEMultipart()
>
> message\[\"From\"\] = SMTP_USERNAME
>
> message\[\"To\"\] = EMAIL_TO
>
> message\[\"Subject\"\] = subject
>
> message.attach(MIMEText(body, \"plain\"))
>
> try:
>
> with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as smtp:
>
> smtp.starttls()
>
> smtp.login(SMTP_USERNAME, SMTP_PASSWORD)
>
> smtp.send_message(message)
>
> logger.info(\"Email sent: %s\", subject)
>
> except Exception as exc:
>
> logger.exception(\"Failed to send email: %s\", exc)
>
> raise

*email_service.py --- full source*

Credentials (SMTP_USERNAME, SMTP_PASSWORD) and the recipient (EMAIL_TO)
are read only from environment variables via config.py, never hardcoded.
If any of the three is missing, send_email() logs a warning and returns
silently rather than failing the whole pipeline. STARTTLS is used before
login() to encrypt the SMTP session. send_email() is called for both
success (\"X inserted\") and permanent failure (\"X failed
permanently\... sent to DLQ\") notifications from within
process_message().

23\. Error Handling Architecture

> try: process_message(\...)
>
> │
>
> ▼
>
> except Exception as exc:
>
> │
>
> ▼
>
> error_message = str(exc)
>
> │
>
> ▼
>
> retry_count = int(fields.get(\"retry_count\", \"0\"))
>
> │
>
> ▼
>
> should_retry(retry_count)?
>
> ┌─────┴─────┐
>
> YES NO
>
> │ │
>
> ▼ ▼
>
> Requeue (XADD + Publish to DLQ + XACK +
>
> XACK + backoff) Failure email

Every stage that can fail --- validation, embedding, Qdrant upsert,
email sending --- is wrapped in the single try/except in
process_message(), so any exception from any of those stages is handled
uniformly by the same retry/DLQ decision rather than needing per-stage
handlers. The outer while True loop in run_stream_consumer()
additionally catches any exception escaping the per-message handling
(e.g. a Redis connection drop during XREADGROUP itself) and sleeps 5
seconds before retrying the read.

24\. Logging Implementation

> logging.basicConfig(
>
> level=logging.INFO,
>
> format=\"%(asctime)s %(levelname)s %(message)s\"
>
> )

This exact call appears at the top of each consumer entrypoint
(run_document_consumer, run_student_consumer, run_employee_consumer,
run_search_consumer, dlq_review.main) so every module shares the same
timestamped, level-tagged log format.

  ------------------------------------------------------------------------
  **Level**      **Meaning**            **Example from this codebase**
  -------------- ---------------------- ----------------------------------
  INFO           Normal processing      \"Acknowledged Redis message %s\"

  WARNING        Recoverable problem    \"Retrying message %s after %s
                                        seconds\"

  ERROR          Processing failure     \"Moved message %s to DLQ %s\"

  EXCEPTION      Failure with traceback logger.exception(\...) in
                                        redis_client.py, qdrant_helper.py,
                                        dlq_handler.py
  ------------------------------------------------------------------------

25\. Redis Monitoring Commands

Enter the Redis CLI first:

> redis-cli

View stream messages:

> XRANGE document_stream - +
>
> XRANGE command_stream - +
>
> XRANGE result_stream - +
>
> XRANGE dead_letter_stream - +

Inspect consumer groups and pending entries:

> XINFO GROUPS document_stream
>
> XPENDING document_stream document_group
>
> XINFO CONSUMERS document_stream document_group

These commands must be run inside redis-cli, not the Linux shell:

> Correct:
>
> 127.0.0.1:6379\> XRANGE document_stream - +
>
> Incorrect:
>
> administrator@server:\~\$ XRANGE document_stream - +

26\. Qdrant Technical Monitoring

> Qdrant Server
>
> │
>
> ▼
>
> Port 6333 (HTTP API / Dashboard) Port 6334 (gRPC)
>
> │
>
> ▼
>
> Collection: documents
>
> │
>
> ▼
>
> Points → Vectors + Payloads

Dashboard: http://localhost:6333/dashboard. Verify the documents
collection exists, its point count grows as documents are processed, and
payload/vector size match VECTOR_SIZE (384). Conceptually: Redis stores
and transports messages; Qdrant stores vectors and performs similarity
search --- the two systems are complementary, not overlapping.

27\. Docker Infrastructure (docker-compose.yml)

> version: \"3.9\"
>
> services:
>
> redis:
>
> image: redis:7-alpine
>
> ports:
>
> \- \"6379:6379\"
>
> command: \[\"redis-server\", \"\--save\", \"\", \"\--appendonly\",
> \"no\"\]
>
> qdrant:
>
> image: qdrant/qdrant:v1.4.0
>
> ports:
>
> \- \"6333:6333\"
>
> \- \"6334:6334\"
>
> command: \[\"./qdrant\", \"\--host\", \"0.0.0.0\", \"\--port\",
> \"6333\", \"\--service-port\", \"6334\"\]

*docker-compose.yml --- full source*

Redis runs with persistence intentionally disabled (\--save \"\" and
\--appendonly no), meaning this configuration is designed for
ephemeral/dev-style durability --- a container restart clears all stream
data unless this is changed for production. Qdrant exposes both its HTTP
API/dashboard (6333) and gRPC service port (6334).

Bring the stack up and check it:

> docker compose up -d
>
> sudo docker ps
>
> sudo docker ps -a
>
> sudo docker start redis
>
> sudo docker start qdrant
>
> sudo docker logs redis
>
> sudo docker logs qdrant

Distinguish the Linux shell prompt from the Redis CLI prompt:

> Linux Shell:
>
> administrator@template:\~\$
>
> Redis CLI:
>
> 127.0.0.1:6379\>

28\. SSH Technical Access

> Local Computer
>
> │
>
> │ SSH
>
> ▼
>
> Remote Linux Server
>
> │
>
> ├── Python Application (venv, consumer.py, producer.py, \...)
>
> ├── Redis Container (port 6379)
>
> └── Qdrant Container (ports 6333 / 6334)

SSH port forwarding lets a service bound to the remote server (e.g.
Qdrant\'s dashboard on 6333, or Redis on 6379) be reached from a local
browser or redis-cli as if it were running locally:

> ssh -L 6333:localhost:6333 -L 6379:localhost:6379 user@remote-server

After tunneling, http://localhost:6333/dashboard on the local machine
reaches the remote Qdrant instance, and a local redis-cli -p 6379
reaches the remote Redis instance.

29\. Complete Technical Data Flow

> Producer (producer.py)
>
> │
>
> ▼
>
> XADD document_stream
>
> │
>
> ▼
>
> Redis Stream
>
> │
>
> ▼
>
> XREADGROUP (document_group / document_consumer_1)
>
> │
>
> ▼
>
> Python Consumer (consumer.py: run_stream_consumer)
>
> │
>
> ▼
>
> Validation (validate_document_message)
>
> │
>
> ▼
>
> Priority Sort (PRIORITY_ORDER)
>
> │
>
> ▼
>
> execute_at Check
>
> │
>
> ▼
>
> Embedding Service (embed_text)
>
> │
>
> ▼
>
> Vector (384-dim list\[float\])
>
> │
>
> ▼
>
> Qdrant Upsert (upsert_point → qdrant_client.upsert)
>
> │
>
> ▼
>
> Email (send_email)
>
> │
>
> ▼
>
> XACK

29.1 Failure Path

> Processing Error (any exception in process_message)
>
> │
>
> ▼
>
> Retry Handler (should_retry / build_retry_payload /
> get_retry_delay_seconds)
>
> │
>
> ▼
>
> retry_count \< MAX_RETRIES ?
>
> ┌─────┴─────┐
>
> Retry (XADD + XACK) DLQ (publish_to_dlq + XACK + failure email)
>
> │
>
> ▼
>
> Developer Review (dlq_review.py)

30\. Complete Search Technical Flow

> Search Command (producer.py publish_search)
>
> │
>
> ▼
>
> XADD command_stream
>
> │
>
> ▼
>
> XREADGROUP (search_group / search_consumer_1)
>
> │
>
> ▼
>
> Search Consumer (search_consumer.py: process_search)
>
> │
>
> ▼
>
> Validate Query (validate_search_command)
>
> │
>
> ▼
>
> Embedding Model (embed_text)
>
> │
>
> ▼
>
> Query Vector
>
> │
>
> ▼
>
> Qdrant Query (search_service.search_documents → HTTP POST
> /points/search)
>
> │
>
> ▼
>
> Similarity Score (cosine)
>
> │
>
> ▼
>
> Top-K Results
>
> │
>
> ▼
>
> Result Stream (XADD result_stream) + XACK

31\. Technical Troubleshooting

  ----------------------------------------------------------------------------
  **Problem**          **Possible Cause**     **Technical Solution**
  -------------------- ---------------------- --------------------------------
  XADD: command not    Command run in Linux   Enter redis-cli first
  found                shell                  

  unknown command      Docker command run     Exit Redis CLI using exit
  \'docker\'           inside Redis CLI       

  QdrantClient has no  Client API version     search_service.py bypasses this
  attribute search     mismatch               by calling the HTTP endpoint
                                              directly

  document_id is       Missing required field Add document_id to the producer
  required                                    payload

  execute_at must be   Malformed execute_at   Match the exact strptime format
  in YYYY-MM-DD        string                 in parse_execute_at()
  HH:MM:SS format                             

  Search returns zero  Qdrant collection has  Confirm documents were processed
  results              no points yet          (check XACK / Qdrant point
                                              count)

  Consumer receives no Wrong stream or group  Verify
  messages             name                   DOCUMENT_STREAM/DOCUMENT_GROUP
                                              or COMMAND_STREAM/SEARCH_GROUP
                                              in config.py

  Docker permission    User lacks Docker      Use sudo docker \...
  denied               permission             

  Model loading is     SentenceTransformer    Expected on first
  slow at startup      initialization cost    EmbeddingService() call; reused
                                              afterward

  Message repeatedly   Validation or          Check logs, fields, and
  retries then hits    embedding error        MAX_RETRIES in config.py
  DLQ                  persists               
  ----------------------------------------------------------------------------

32\. Technical Dependencies (requirements.txt)

> redis
>
> qdrant-client
>
> sentence-transformers
>
> python-dotenv
>
> numpy
>
> torch

*requirements.txt --- full source*

  --------------------------------------------------------------------------
  **Package**             **Role in this project**
  ----------------------- --------------------------------------------------
  redis                   Redis client library used by redis_client.py for
                          XADD/XREADGROUP/XACK/XPENDING/XCLAIM

  qdrant-client           Provides QdrantClient and REST models used in
                          qdrant_helper.py

  sentence-transformers   Loads the embedding model used in
                          embedding_service.py

  python-dotenv           load_dotenv() in config.py reads the .env file

  numpy                   Underlying array type produced by
                          convert_to_numpy=True during encoding

  torch                   Backend tensor library required by
                          sentence-transformers
  --------------------------------------------------------------------------

Manage dependencies with:

> pip install -r requirements.txt
>
> pip freeze

33\. Technical Data Structures

33.1 Document Stream Message

> {
>
> \"document_id\": \"DOC3001\",
>
> \"title\": \"Redis Streams\",
>
> \"text\": \"Redis Streams allow asynchronous processing\",
>
> \"priority\": \"HIGH\",
>
> \"execute_at\": \"2026-07-28 12:00:00\",
>
> \"retry_count\": \"0\"
>
> }

33.2 Search Command

> {
>
> \"command\": \"search\",
>
> \"query\": \"What are Redis Streams?\",
>
> \"top_k\": \"5\"
>
> }

33.3 Search Result

> {
>
> \"query\": \"What are Redis Streams?\",
>
> \"result_count\": \"3\",
>
> \"results\": \"\[{\'id\': \'\...\', \'score\': 0.87, \'payload\':
> {\...}}, \...\]\"
>
> }

33.4 Qdrant Point

> {
>
> \"id\": \"\<uuid5 from document_id\>\",
>
> \"vector\": \[0.12, -0.44, 0.78, \"\... 384 floats\"\],
>
> \"payload\": {
>
> \"document_id\": \"DOC3001\",
>
> \"title\": \"Redis Streams\",
>
> \"text\": \"Redis Streams allow asynchronous processing\",
>
> \"priority\": \"HIGH\",
>
> \"created_at\": \"2026-07-28T12:00:00+00:00\",
>
> \"type\": \"document\"
>
> }
>
> }

33.5 DLQ Entry

> {
>
> \"original_stream\": \"document_stream\",
>
> \"original_message_id\": \"1690000000000-0\",
>
> \"document_id\": \"DOC3001\",
>
> \"retry_count\": \"3\",
>
> \"error\": \"text is required\",
>
> \"failed_at\": \"2026-07-28 12:05:00\",
>
> \"original_data\": \"{\'document_id\': \'DOC3001\', \...}\"
>
> }

34\. Technical Interfaces

  -----------------------------------------------------------------------
  **Interface**             **Input**              **Output**
  ------------------------- ---------------------- ----------------------
  Redis Producer            document_id, title,    Redis stream message
  (producer.py)             text, priority,        ID
                            execute_at / query,    
                            top_k                  

  Redis Consumer            Stream message (fields Processed data + XACK
  (consumer.py)             dict)                  / retry / DLQ

  Embedding Service         Text (str)             Vector (list\[float\],
  (embedding_service.py)                           length 384)

  Qdrant Upsert             Vector + payload dict  Success (point stored)
  (upsert_point)                                   

  Qdrant Search             Query vector, top_k    List of {id, score,
  (search_service.py)                              payload}

  Email Service             Subject, body          Email sent (or skipped
  (email_service.py)                               if unconfigured)

  DLQ Handler               Failed message         DLQ stream entry ID
  (dlq_handler.py)          fields + error         
  -----------------------------------------------------------------------

35\. Performance Considerations

-   Embedding model load time is paid once per consumer process at
    EmbeddingService() construction, not per message.

-   Each XREADGROUP call fetches up to count=10 messages per stream in
    one round trip, reducing per-message network overhead.

-   Qdrant search latency depends on collection size and is affected by
    top_k in search_documents().

-   Redis Stream throughput depends on producer XADD rate versus
    consumer XREADGROUP batch size and block time (REDIS_BLOCK_MS).

-   Only one consumer name per stream is configured by default (e.g.
    document_consumer_1); running multiple processes with distinct
    consumer names against the same group would parallelize processing.

-   Retry backoff (get_retry_delay_seconds) uses a blocking sleep()
    inside the consumer loop, which pauses further message processing
    during the wait.

> Batch Processing (count=10 per XREADGROUP)
>
> ↓
>
> Multiple Consumers (distinct consumer names on the same group)
>
> ↓
>
> Model Reuse (EmbeddingService loaded once)
>
> ↓
>
> Connection Reuse (get_redis_client / get_qdrant_client called once at
> startup)
>
> ↓
>
> Qdrant Index Optimization (collection tuning, outside this codebase\'s
> scope)

36\. Technical Security

-   SMTP credentials (SMTP_USERNAME, SMTP_PASSWORD) and recipient
    (EMAIL_TO) are read only from environment variables via config.py,
    never hardcoded in source.

-   python-dotenv loads secrets from a local .env file which should be
    excluded from version control (see .env.example as the committed
    template).

-   email_service.py uses STARTTLS before authenticating, encrypting the
    SMTP session.

-   Redis and Qdrant, as configured in docker-compose.yml, are exposed
    on host ports without authentication --- production use would
    require adding Redis AUTH / Qdrant API keys and firewalling ports
    6379/6333/6334 to trusted hosts.

-   SSH is the access path to the remote Linux server; port forwarding
    (Section 28) is preferred over exposing Redis/Qdrant ports publicly.

> Application
>
> │
>
> ▼
>
> Environment Variables (.env, loaded by config.py)
>
> │
>
> ▼
>
> Credentials (SMTP_USERNAME, SMTP_PASSWORD, REDIS_HOST, QDRANT_HOST,
> \...)

37\. Technical Limitations

-   Each consumer process depends on loading the embedding model at
    startup, which is CPU/GPU-bound depending on torch\'s available
    backend.

-   Processing is single-threaded per consumer; retry backoff via
    sleep() blocks that consumer\'s message loop.

-   Redis and Qdrant availability directly affects processing ---
    get_redis_client() and get_qdrant_client() raise on connection
    failure.

-   docker-compose.yml disables Redis persistence (\--save \"\",
    \--appendonly no), so unacknowledged/unprocessed stream data is lost
    on container restart.

-   The naive datetime.now() comparison in process_message() for
    execute_at does not account for server timezone differences between
    producer and consumer hosts.

-   Large documents are embedded as a single text without chunking,
    which may exceed the effective input length of the embedding model.

-   Search quality is bounded by the chosen embedding model
    (all-MiniLM-L6-v2 by default).

-   This is a single-node deployment (one Redis, one Qdrant container)
    with no built-in fault tolerance or clustering.

38\. Future Technical Enhancements

> Current
>
> │
>
> ▼
>
> Redis Streams → Python Consumers → Qdrant
>
> Future
>
> │
>
> ▼
>
> API Gateway → Redis Cluster → Multiple Consumers → Embedding Service →
> Qdrant Cluster → Monitoring

-   REST API (e.g. FastAPI) in front of producer.py for external
    integrations

-   Multiple consumer worker processes per stream for horizontal scaling

-   Redis Cluster for higher throughput and availability

-   Qdrant Cluster / sharded collections for larger datasets

-   Prometheus + Grafana for metrics and dashboards

-   Centralized log aggregation instead of per-process stdout logging

-   Kubernetes for orchestration and auto-restart of consumers

-   Automatic DLQ reprocessing instead of the current manual
    dlq_review.py workflow

-   Document chunking before embedding for long texts

-   Hybrid keyword + vector search and payload-based metadata filtering

39\. Final Technical Summary

> TECHNICAL PIPELINE
>
> Input Data → Redis XADD → Redis Stream → XREADGROUP → Python Consumer
>
> → Validation → Retry / DLQ → Embedding Model → Vector
>
> → Qdrant → Semantic Search → Results

This implementation separates message transport, business validation,
embedding generation, vector storage, semantic search, failure handling,
and notification into independent modules --- redis_client.py and
config.py for transport plumbing, validation.py and
retry_handler.py/dlq_handler.py for correctness and fault tolerance,
embedding_service.py and qdrant_helper.py/qdrant_service.py for the
vector pipeline, search_service.py and search_consumer.py for query
handling, and email_service.py for operator visibility. Redis Streams
provide ordered, replayable, at-least-once delivery through consumer
groups; Python consumers (consumer.py, search_consumer.py) drive the
business logic; the embedding service converts text into 384-dimensional
vectors; and Qdrant stores and searches those vectors by cosine
similarity. Retry and DLQ mechanisms, backed by deterministic Qdrant
point IDs, give the pipeline fault tolerance without duplicate records,
while structured logging and the Redis/Qdrant monitoring commands in
Sections 25--26 support day-to-day operational visibility.
