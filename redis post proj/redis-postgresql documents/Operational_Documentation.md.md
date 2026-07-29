**OPERATIONAL DOCUMENTATION**

**Redis → Python Consumer → PostgreSQL**

Data Processing System

*Operations & Maintenance Runbook*

Document Type: Operational Runbook

Version: 1.0

Status: Active

**Table of Contents**

**1. System Overview**

**1.1 Purpose of the System**

This system provides reliable, ordered, and fault-tolerant processing of
application events. Producers publish messages to a Redis Stream, a
Python consumer processes each message according to timing and priority
rules, and validated results are persisted into PostgreSQL. Messages
that fail processing are retried automatically, and messages that
exhaust all retries are routed to a Dead Letter Queue (DLQ) for manual
developer review and reprocessing.

**1.2 Role of Redis**

Redis acts as the message broker and buffer between producers and the
consumer. It uses Redis Streams to durably hold messages, track delivery
to consumer groups, and maintain a Pending Entries List (PEL) for
messages that have been read but not yet acknowledged.

**1.3 Role of the Python Consumer**

The Python consumer continuously reads new messages from the Redis
Stream using a consumer group. For each message it checks the optional
execute_at scheduling field, applies priority ordering, validates the
payload, writes the result to PostgreSQL, and acknowledges the message
on success. On failure it applies the retry policy and, if retries are
exhausted, moves the message to the DLQ.

**1.4 Role of PostgreSQL**

PostgreSQL is the system of record. It stores the final, validated
output of message processing and supports downstream reporting,
auditing, and application queries.

**1.5 Overall Data Flow**

Producer / Application │ ▼ Redis Stream │ ▼ Python Consumer │ ▼ Time
Check (execute_at, if provided) │ ▼ Priority Processing │ ▼ PostgreSQL │
┌─────┴─────┐ Success Failure │ │ ACK Redis Retry Processing │ │ Send
Email Max Retries Reached │ ▼ Dead Letter Queue (DLQ) │ ▼ Developer
Reviews Message │ ▼ Reprocess

**2. System Components and Services**

  -----------------------------------------------------------------------
  **Component**       **Description**
  ------------------- ---------------------------------------------------
  Redis               In-memory data store used as the message broker.
                      Hosts the stream, consumer group metadata, and
                      pending-entry tracking.

  Redis Streams       Append-only log data structure inside Redis that
                      stores each message with a unique ID and
                      field/value payload.

  Consumer Groups     Redis construct that allows one or more consumer
                      processes to share the work of reading a stream,
                      with per-consumer delivery and acknowledgment
                      tracking.

  Python Consumer     The application process that reads from the stream,
                      applies business logic (time check, priority,
                      validation), and writes to PostgreSQL.

  PostgreSQL          Relational database that stores the final processed
                      records.

  Dead Letter Queue   A separate Redis stream (or dedicated key space)
  (DLQ)               holding messages that failed after the maximum
                      number of retries, awaiting manual review.

  Email /             Sends success or failure notifications, typically
  Notification System via SMTP or a transactional email API, triggered
                      after processing completes.
  -----------------------------------------------------------------------

**3. System Startup Procedure**

**3.1 Start Redis**

sudo systemctl start redis \# or, if running via Docker: docker start
redis-server

**3.2 Verify Redis Is Running**

redis-cli ping \# Expected output: PONG

**3.3 Start PostgreSQL**

sudo systemctl start postgresql \# or, if running via Docker: docker
start postgres-db

**3.4 Verify PostgreSQL Is Running**

pg_isready -h localhost -p 5432 \# Expected output: localhost:5432 -
accepting connections

**3.5 Start the Python Consumer**

cd /opt/app/consumer source venv/bin/activate python consumer.py \# or,
under a process manager: systemctl start redis-consumer \# or pm2 start
consumer.py \--interpreter python3

**3.6 Verify the Consumer Is Connected**

-   Check the consumer log for a startup confirmation line, e.g.
    \"Connected to Redis\" and \"Connected to PostgreSQL\".

-   Confirm the consumer is registered in the Redis consumer group:
    redis-cli XINFO CONSUMERS mystream mygroup

-   Confirm PostgreSQL shows an active connection from the consumer:
    SELECT \* FROM pg_stat_activity WHERE application_name =
    \'redis_consumer\';

**4. System Shutdown Procedure**

Always shut the system down in the following order to avoid message loss
or orphaned in-flight transactions:

1.  Stop the Python consumer first. Send a graceful termination signal
    so it finishes the message currently being processed and
    acknowledges it before exiting.

2.  Confirm no messages are left mid-processing by checking the Pending
    Entries List: redis-cli XPENDING mystream mygroup

3.  Stop PostgreSQL only after confirming there are no open transactions
    from the consumer.

4.  Stop Redis last, since it holds the durable stream data that other
    components depend on.

\# Graceful stop systemctl stop redis-consumer sudo systemctl stop
postgresql sudo systemctl stop redis

**5. Redis Stream Operations**

**Add a new message to the stream**

XADD mystream \'\*\' payload
\'{\"id\":123,\"execute_at\":\"2026-07-26T10:00:00Z\",\"priority\":1}\'

**Read messages**

XRANGE mystream - + COUNT 10 XREAD COUNT 10 STREAMS mystream 0

**Check pending messages**

XPENDING mystream mygroup XPENDING mystream mygroup - + 10 myconsumer

**Check consumer groups**

XINFO GROUPS mystream XINFO CONSUMERS mystream mygroup

**Check failed messages**

XRANGE mystream-failed - +

**Check the Dead Letter Queue**

XRANGE mystream-dlq - + COUNT 20 XLEN mystream-dlq

**Reprocess a failed message**

\# Read the message from the DLQ, fix the payload if needed, then re-add
it to the main stream XADD mystream \'\*\' payload \'\<corrected
payload\>\' \# Then remove it from the DLQ XDEL mystream-dlq
\<message-id\>

**6. Message Processing Operations**

The full lifecycle of a single message:

5.  Message enters the Redis Stream via XADD, either from the producer
    application or a manual operator action.

6.  The Python consumer reads the message using XREADGROUP as part of
    its assigned consumer group.

7.  If the message includes an execute_at field, the consumer compares
    it to the current time. Messages scheduled for the future are
    deferred and re-checked on a later polling cycle.

8.  If a priority field is present, higher-priority messages are
    processed ahead of lower-priority ones within the same polling
    batch.

9.  The payload is validated against the expected schema (required
    fields, data types, value ranges).

10. Valid data is inserted or updated in PostgreSQL inside a
    transaction.

11. On success, the message is acknowledged with XACK, and a success
    notification/email is sent.

12. On failure, the message is retried according to the retry policy
    (see Section 11).

13. If the message still fails after the maximum retry count, it is
    moved to the DLQ for manual review.

**7. Monitoring**

  -----------------------------------------------------------------------
  **What to Monitor**    **How to Check**
  ---------------------- ------------------------------------------------
  Redis availability     redis-cli ping

  Redis Stream message   redis-cli XLEN mystream
  count                  

  Pending messages       redis-cli XPENDING mystream mygroup

  Consumer group status  redis-cli XINFO GROUPS mystream

  Python consumer status systemctl status redis-consumer / ps aux \| grep
                         consumer.py

  PostgreSQL             pg_isready -h localhost -p 5432
  availability           

  Successful DB          SELECT count(\*) FROM processed_messages WHERE
  insertions             status=\'success\' AND created_at \> now() -
                         interval \'1 hour\';

  Failed messages        SELECT count(\*) FROM processed_messages WHERE
                         status=\'failed\';

  Retry attempts         Check consumer logs for \'RETRY\' entries, or a
                         retry_count column in the tracking table

  DLQ messages           redis-cli XLEN mystream-dlq
  -----------------------------------------------------------------------

**8. Health Checks**

**Redis**

**☐** redis-cli ping returns PONG

**☐** Memory usage is within expected limits (INFO memory)

**Python Consumer**

**☐** Process is running and not restarting in a loop

**☐** Latest log entry is recent (no stale/frozen process)

**PostgreSQL**

**☐** pg_isready reports accepting connections

**☐** No long-running or blocked transactions in pg_stat_activity

**Redis Streams**

**☐** Stream length (XLEN) is not growing unbounded

**☐** Oldest pending entry is not older than the expected processing
window

**Consumer Groups**

**☐** Consumer group lag is low (XINFO GROUPS shows small lag)

**☐** Expected consumer(s) are visible and active in XINFO CONSUMERS

**9. Logging**

  -----------------------------------------------------------------------
  **Log Level /    **Meaning**          **Example Log Line**
  Type**                                
  ---------------- -------------------- ---------------------------------
  INFO             General operational  2026-07-26 09:00:01 INFO Consumer
                   events               started, connected to Redis and
                                        PostgreSQL

  WARNING          Recoverable or       2026-07-26 09:05:12 WARNING
                   unusual conditions   Message 1691-0 deferred,
                                        execute_at in the future

  ERROR            Processing or        2026-07-26 09:06:44 ERROR Failed
                   connection failures  to insert record for message
                                        1691-0: connection refused

  Connection       Connect/disconnect   2026-07-26 09:00:00 INFO
                   events               Connected to PostgreSQL at
                                        db-host:5432

  Successful       Confirmed completion 2026-07-26 09:06:50 INFO Message
  processing       of a message         1691-0 processed and acknowledged

  Retry            Retry attempts and   2026-07-26 09:07:10 WARNING Retry
                   counts               2/5 for message 1691-0

  DLQ              Messages moved to    2026-07-26 09:10:00 ERROR Message
                   the DLQ              1691-0 moved to DLQ after 5
                                        failed attempts
  -----------------------------------------------------------------------

**10. Error Handling and Troubleshooting**

  ------------------------------------------------------------------------
  **Problem**    **Possible        **How to Check**   **Solution**
                 Cause**                              
  -------------- ----------------- ------------------ --------------------
  Redis          Redis service     redis-cli ping     Restart Redis
  connection     down, network     from the consumer  service, verify
  failure        issue, wrong      host               network/firewall,
                 host/port                            correct config

  PostgreSQL     DB service down,  pg_isready; check  Restart PostgreSQL,
  connection     wrong             pg_stat_activity   verify credentials,
  failure        credentials, max                     increase
                 connections                          max_connections if
                 reached                              needed

  Python         Unhandled         systemctl status   Review error log,
  consumer       exception, crash, redis-consumer;    fix root cause,
  stopped        out-of-memory     check logs         restart consumer
                 kill                                 

  Invalid        Producer sent     Inspect the raw    Reject and log;
  message format malformed JSON or message with       alert the producing
                 wrong schema      XRANGE             team; correct and
                                                      resend

  Missing        Producer omitted  Compare payload    Return validation
  required       a mandatory field against schema     error; message goes
  fields                                              to retry/DLQ per
                                                      policy

  PostgreSQL     Constraint        Check PostgreSQL   Fix data issue or
  insertion      violation,        logs and consumer  reconnect; retry the
  failure        connection drop   error log          message
                 mid-transaction                      

  Duplicate data Message processed Check for          Use idempotent
                 twice due to      duplicate keys in  upserts keyed on a
                 retry after a     the target table   unique message ID
                 late ACK                             

  Message stuck  Consumer crashed  redis-cli XPENDING Use
  in Pending     after XREADGROUP  mystream mygroup   XCLAIM/XAUTOCLAIM to
  Entries List   but before XACK                      reassign to an
                                                      active consumer

  Consumer crash Unhandled         Check process      Restart via process
                 exception or      manager status and manager; investigate
                 resource          system logs        stack trace in logs
                 exhaustion                           

  Retry failure  Underlying issue  Check retry_count  Investigate root
                 not resolved      and last error per cause; consider
                 between retries   message            manual DLQ review
                                                      sooner

  DLQ message    Message exhausted redis-cli XRANGE   Developer reviews,
                 all retries       mystream-dlq - +   corrects
                                                      payload/config,
                                                      reprocesses
  ------------------------------------------------------------------------

**11. Retry and Dead Letter Queue Operations**

**11.1 How Retry Attempts Work**

When processing fails, the consumer increments a retry counter attached
to the message and re-attempts processing after a configured delay,
typically with exponential backoff.

**11.2 Maximum Retry Attempts**

A configurable maximum (commonly 5) limits how many times a message is
retried before it is considered permanently failed.

**11.3 When a Message Moves to the DLQ**

Once the retry counter reaches the maximum, the message is written to
the DLQ stream along with its error history and removed from active
retry processing.

**11.4 How a Developer Reviews a Failed Message**

redis-cli XRANGE mystream-dlq - + COUNT 20

Inspect the payload and the recorded error reason, then determine
whether the issue is in the data, the downstream service, or the
consumer logic.

**11.5 How to Reprocess a DLQ Message**

14. Correct the payload or resolve the underlying issue (e.g. schema
    fix, downstream service restored).

15. Re-add the corrected message to the main stream with XADD.

16. Remove the original entry from the DLQ with XDEL once confirmed
    processed.

**11.6 How to Prevent Repeated Failures**

-   Validate messages as close to the producer as possible.

-   Add alerting when DLQ size grows beyond a threshold.

-   Track recurring error reasons to identify systemic issues rather
    than one-off data problems.

**12. PostgreSQL Operations**

**Verify database connectivity**

psql -h localhost -U app_user -d appdb -c \'SELECT 1;\'

**Check whether data was inserted**

SELECT \* FROM processed_messages WHERE message_id = \'\<id\>\';

**Check whether data was updated**

SELECT updated_at FROM processed_messages WHERE message_id = \'\<id\>\';

**Verify table records**

SELECT count(\*) FROM processed_messages;

**Handle database errors**

Review the PostgreSQL log for constraint violations, deadlocks, or
connection errors. Cross-reference the timestamp with the consumer\'s
error log to confirm the exact failing message.

**Verify transaction success**

SELECT xact_commit, xact_rollback FROM pg_stat_database WHERE datname =
\'appdb\';

**13. Backup and Recovery**

**Redis data backup**

redis-cli SAVE \# or trigger a background save: redis-cli BGSAVE \# Copy
the resulting dump.rdb to backup storage

**PostgreSQL database backup**

pg_dump -h localhost -U app_user -F c -f appdb_backup.dump appdb

**Restoring PostgreSQL data**

pg_restore -h localhost -U app_user -d appdb appdb_backup.dump

**Recovering after Redis failure**

Restore the most recent dump.rdb (or AOF file) to the Redis data
directory and restart Redis. Recreate the consumer group if it was lost,
using XGROUP CREATE with the correct starting ID.

**Recovering after Python consumer failure**

Restart the consumer process. Because messages remain in the Pending
Entries List until acknowledged, no data is lost; use XCLAIM/XAUTOCLAIM
to reassign any messages left pending by the failed instance.

**14. Deployment Operations**

17. Install required software: Redis, PostgreSQL, Python 3.x, and pip.

18. Configure Redis: set persistence options (RDB/AOF), bind address,
    and memory limits in redis.conf.

19. Configure PostgreSQL: create the database, application user, and
    required schema/tables.

20. Configure the Python environment: create a virtual environment for
    the consumer application.

21. Install Python dependencies: pip install -r requirements.txt

22. Configure environment variables for Redis host/port, PostgreSQL
    connection string, retry settings, and SMTP/notification
    credentials.

23. Start the Python consumer and confirm it registers with the Redis
    consumer group.

24. Perform a complete end-to-end test (see Section 18) before declaring
    the deployment complete.

python -m venv venv source venv/bin/activate pip install -r
requirements.txt export REDIS_HOST=localhost export
POSTGRES_DSN=postgresql://app_user@localhost:5432/appdb python
consumer.py

**15. Configuration Management**

  -----------------------------------------------------------------------
  **Configuration    **Description**               **Example Value**
  Item**                                           
  ------------------ ----------------------------- ----------------------
  Redis host         Hostname or IP of the Redis   localhost
                     server                        

  Redis port         Port Redis listens on         6379

  Stream name        Name of the main Redis Stream mystream

  Consumer group     Name of the shared consumer   mygroup
  name               group                         

  Consumer name      Unique identifier for this    consumer-1
                     consumer instance             

  PostgreSQL host    Hostname or IP of the         localhost
                     PostgreSQL server             

  PostgreSQL port    Port PostgreSQL listens on    5432

  Database name      Target database name          appdb

  Retry count        Maximum retry attempts before 5
                     DLQ                           

  Retry delay        Delay between retries (with   30s, 60s, 120s \...
                     backoff)                      

  execute_at         Messages with a future        Deferred / polled each
  behavior           execute_at are deferred until cycle
                     due                           

  Priority behavior  Lower number = higher         1 (highest) -- 5
                     priority, processed first     (lowest)
                     within a batch                
  -----------------------------------------------------------------------

*Note: Do not store real passwords, API keys, or secrets in this
document or in plain configuration files. Use a secrets manager or
environment-variable injection at deploy time.*

**16. Routine Maintenance**

**Daily**

**☐** Check Redis (ping, memory usage)

**☐** Check PostgreSQL (connectivity, active connections)

**☐** Check Python consumer (process status, recent log activity)

**☐** Check pending messages (XPENDING)

**☐** Check DLQ size

**Weekly**

**☐** Review failed messages and recurring error patterns

**☐** Review consumer and database logs

**☐** Check database storage usage

**☐** Check Redis memory trends

**Monthly**

**☐** Test backup restore procedure

**☐** Review overall system performance and throughput

**☐** Archive or clean old processed data if required

**☐** Review failure patterns and update runbook/troubleshooting guide

**17. Incident Response**

  -----------------------------------------------------------------------
  **Incident**        **Response**
  ------------------- ---------------------------------------------------
  Redis is down       Failover to a replica if available; restart the
                      primary; notify the on-call team; consumer will
                      reconnect automatically once Redis is back.

  PostgreSQL is down  Restart the database service; check disk space and
                      connection limits; consumer will queue/retry until
                      DB is reachable.

  Python consumer     Restart via the process manager; check logs for the
  crashes             crash cause; verify no messages are stuck in the
                      Pending Entries List.

  Messages are        Check consumer throughput and PostgreSQL latency;
  accumulating        consider scaling out additional consumer instances
                      in the same group.

  Database insertions Check PostgreSQL logs for constraint or
  are failing         connectivity errors; pause the consumer if
                      necessary to avoid a retry storm.

  DLQ size increases  Sample recent DLQ messages to find the common
  rapidly             failure cause; fix the root cause before
                      reprocessing in bulk.
  -----------------------------------------------------------------------

**18. End-to-End Operational Test**

25. Add a test message to the Redis Stream using XADD.

26. Verify the message is received by the Python consumer (check logs
    for the message ID).

27. Verify the data is processed (time check and priority logic applied
    correctly).

28. Verify the data appears in PostgreSQL with a matching query.

29. Verify the Redis message is acknowledged (XACK) and no longer
    appears in XPENDING.

30. Test a failure scenario by submitting an intentionally invalid
    message.

31. Verify retry behavior by observing retry log entries and the
    increasing retry counter.

32. Verify DLQ behavior after the maximum retries are reached.

33. Verify developer reprocessing by correcting the DLQ message and
    confirming it processes successfully on resubmission.

**19. Operational Runbook (Quick Reference)**

  -----------------------------------------------------------------------
  **Action**          **Command**
  ------------------- ---------------------------------------------------
  Start services      sudo systemctl start redis && sudo systemctl start
                      postgresql && systemctl start redis-consumer

  Stop services       systemctl stop redis-consumer && sudo systemctl
                      stop postgresql && sudo systemctl stop redis

  Check Redis         redis-cli ping

  Check PostgreSQL    pg_isready -h localhost -p 5432

  Check stream        redis-cli XLEN mystream
  messages            

  Check pending       redis-cli XPENDING mystream mygroup
  messages            

  Check consumer      redis-cli XINFO GROUPS mystream
  groups              

  Check DLQ           redis-cli XLEN mystream-dlq

  Restart the Python  systemctl restart redis-consumer
  consumer            

  Verify database     psql -h localhost -U app_user -d appdb -c \"SELECT
  records             count(\*) FROM processed_messages;\"
  -----------------------------------------------------------------------

**20. Final System Operations Flow**

Redis Stream │ ▼ Python Consumer │ ▼ Validate Message │ ▼ Check
execute_at │ ▼ Check Priority │ ▼ Process Data │ ▼ PostgreSQL │
┌───┴────┐ Success Failure │ │ ACK Retry │ │ Notify Max Retries Reached
│ ▼ Dead Letter Queue │ ▼ Developer Review │ ▼ Reprocess

*End of Document*
