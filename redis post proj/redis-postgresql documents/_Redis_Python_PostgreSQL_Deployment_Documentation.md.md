**DEPLOYMENT DOCUMENTATION**

**Redis → Python Consumer → PostgreSQL**

Data Processing System

*Fresh Server to Production Deployment Guide*

Document Type: Deployment Guide

Version: 1.0

Status: Active

**Table of Contents**

**1. Deployment Overview**

**1.1 Purpose of Deployment**

This document explains how to deploy the Redis → Python Consumer →
PostgreSQL system from a fresh server to a fully working production
environment. It covers only deployment: installation, configuration, and
go-live validation. It does not cover day-to-day operations, monitoring,
or long-term maintenance --- see the separate Operational Documentation
for that.

**1.2 Components to Be Deployed**

-   Redis (message broker, Redis Streams, consumer groups)

-   Python Consumer application (validation, execute_at check, priority
    processing)

-   PostgreSQL (database, schema, tables)

-   Email/Notification system (success/failure notifications)

-   Process manager for the consumer (systemd, Supervisor, or Docker)

**1.3 Deployment Architecture**

User / Application │ ▼ Redis │ ▼ Python Consumer │ ▼ PostgreSQL │ ▼
Email / Notification System

**1.4 Development Environment vs Production Environment**

  -----------------------------------------------------------------------
  **Aspect**      **Development**             **Production**
  --------------- --------------------------- ---------------------------
  Data            Optional, may reset         Required, Redis persistence
  persistence     frequently                  (AOF/RDB) and PostgreSQL
                                              backups enabled

  Credentials     Local/test credentials      Strong, unique credentials
                                              stored securely, never
                                              hardcoded

  Process         Run manually in a terminal  systemd, Supervisor, or
  management                                  Docker with automatic
                                              restart

  Network         Localhost only              Restricted to internal
  exposure                                    network; no public exposure
                                              of Redis/PostgreSQL

  Logging         Console output              Persistent log files with
                                              rotation, forwarded to a
                                              log store if available
  -----------------------------------------------------------------------

**1.5 Deployment Flow**

At a high level: prepare the server, install Redis and PostgreSQL,
deploy the Python project, configure environment variables, configure
the Redis Stream and consumer group, start the consumer under a process
manager, then validate with end-to-end and failure tests before
declaring the system production-ready. The full flow diagram is provided
in Section 22.

**2. Deployment Prerequisites**

**2.1 Hardware Requirements**

  ------------------------------------------------------------------------
  **Requirement**   **Recommendation**
  ----------------- ------------------------------------------------------
  CPU               2 vCPUs minimum (4+ recommended for higher message
                    throughput)

  RAM               4 GB minimum (8 GB+ recommended; Redis and PostgreSQL
                    both benefit from more memory)

  Storage           20 GB minimum SSD storage (more depending on
                    PostgreSQL data volume and Redis persistence needs)

  Network           Stable internal network connectivity between the
                    consumer host, Redis, and PostgreSQL; outbound access
                    for the notification/email service
  ------------------------------------------------------------------------

**2.2 Software Requirements**

  -----------------------------------------------------------------------
  **Software**     **Version / Notes**
  ---------------- ------------------------------------------------------
  Operating System Ubuntu 22.04 LTS or later (or equivalent Linux
                   distribution)

  Python           Python 3.10 or later

  pip              Latest version bundled with Python 3

  Redis            Redis 6.2 or later (Streams support required)

  PostgreSQL       PostgreSQL 13 or later

  Git              Latest stable version

  Docker (if used) Docker Engine 24+ and Docker Compose v2

  Python packages  redis, psycopg2-binary, python-dotenv, plus any
                   project-specific packages (see Section 5)
  -----------------------------------------------------------------------

**2.3 Required Ports**

  -----------------------------------------------------------------------
  **Service**         **Port**       **Exposure**
  ------------------- -------------- ------------------------------------
  Redis               6379           Internal only

  PostgreSQL          5432           Internal only

  Python Consumer     None required  No inbound port needed; it only
                                     connects outward to Redis and
                                     PostgreSQL
  -----------------------------------------------------------------------

**2.4 Firewall, Network, and Permissions**

-   Allow outbound/inbound traffic between the consumer host and
    Redis/PostgreSQL hosts on their respective ports only.

-   Block public internet access to ports 6379 and 5432 unless
    explicitly required and authenticated.

-   The deploying user must have sudo privileges to install packages and
    manage services.

-   A dedicated, least-privilege PostgreSQL user should be used for the
    application (see Section 7).

**3. Deployment Environment Setup**

The following steps prepare a fresh Ubuntu server before the project is
deployed.

1.  Connect to the server.

ssh \<SERVER_USER\>@\<SERVER_IP\>

2.  Update the operating system.

sudo apt update sudo apt upgrade -y

3.  Install required base packages.

sudo apt install -y build-essential curl software-properties-common

4.  Install Python.

sudo apt install -y python3 python3-venv python3-dev

5.  Install pip.

sudo apt install -y python3-pip

6.  Install Git.

sudo apt install -y git

7.  Install Redis.

sudo apt install -y redis-server

8.  Install PostgreSQL.

sudo apt install -y postgresql postgresql-contrib

9.  Verify all installations.

python3 \--version pip3 \--version git \--version redis-server
\--version psql \--version

**4. Project Deployment**

10. Clone the project from Git.

git clone \<repository-url\> cd \<project-directory\>

11. Create a Python virtual environment.

python3 -m venv venv

12. Activate the virtual environment.

source venv/bin/activate

13. Install dependencies.

pip install -r requirements.txt

14. Verify the project files (confirm consumer.py, requirements.txt, and
    schema files are present).

ls -la

15. Configure the application (create the .env file --- see Section 9).

**Why Use a Virtual Environment?**

A virtual environment isolates the project\'s Python dependencies from
the system-wide Python installation. This prevents version conflicts
with other applications on the same server and makes the deployment
reproducible across environments.

**5. Python Dependencies**

Core dependencies typically required by this system:

  -----------------------------------------------------------------------
  **Package**         **Purpose**
  ------------------- ---------------------------------------------------
  redis               Redis client for Python; used to read/write Streams
                      and manage consumer groups

  psycopg2-binary     PostgreSQL driver used by the consumer to
                      insert/update records

  python-dotenv       Loads configuration from the .env file into
                      environment variables
  -----------------------------------------------------------------------

Add any other project-specific packages (e.g. an email/notification SDK)
to this list as needed.

**Generating requirements.txt**

pip freeze \> requirements.txt

**Installing from requirements.txt**

pip install -r requirements.txt

**Verifying Dependencies**

pip list python3 -c \"import redis, psycopg2, dotenv; print(\'All core
dependencies import successfully\')\"

**6. Redis Deployment**

16. Install Redis.

sudo apt install -y redis-server

17. Start Redis.

sudo systemctl start redis

18. Enable Redis to start automatically on boot.

sudo systemctl enable redis

19. Verify Redis is running.

sudo systemctl status redis

20. Check the Redis port.

sudo ss -tlnp \| grep 6379

21. Test Redis connectivity.

redis-cli ping \# Expected result: PONG

22. Configure Redis if required (edit /etc/redis/redis.conf for memory
    limits, persistence, bind address).

23. Configure Redis authentication if required.

\# In /etc/redis/redis.conf requirepass \<REDIS_PASSWORD\> \# Then
restart Redis sudo systemctl restart redis

24. Configure Redis network access (bind address in redis.conf; restrict
    to internal network only).

**Verifying the Redis Stream**

redis-cli XINFO STREAM \<stream_name\>

**Creating / Verifying the Consumer Group**

redis-cli XGROUP CREATE \<stream_name\> \<group_name\> \$ MKSTREAM
redis-cli XINFO GROUPS \<stream_name\>

**7. PostgreSQL Deployment**

25. Install PostgreSQL.

sudo apt install -y postgresql postgresql-contrib

26. Start PostgreSQL.

sudo systemctl start postgresql

27. Enable PostgreSQL at startup.

sudo systemctl enable postgresql

28. Verify PostgreSQL is running.

sudo systemctl status postgresql

29. Create the database.

sudo -u postgres psql -c \"CREATE DATABASE \<DB_NAME\>;\"

30. Create the required database user.

sudo -u postgres psql -c \"CREATE USER \<DB_USERNAME\> WITH PASSWORD
\'\<DB_PASSWORD\>\';\"

31. Configure user permissions.

sudo -u postgres psql -c \"GRANT ALL PRIVILEGES ON DATABASE \<DB_NAME\>
TO \<DB_USERNAME\>;\"

32. Create the required tables (see Section 8).

33. Verify database connectivity.

psql -h localhost -U \<DB_USERNAME\> -d \<DB_NAME\> -c \"SELECT 1;\"

34. Verify table structure.

psql -h localhost -U \<DB_USERNAME\> -d \<DB_NAME\> -c \"\\dt\"

**How the Application Connects to PostgreSQL**

The Python consumer reads connection details (host, port, database,
user, password) from environment variables at startup and opens a
connection pool using psycopg2. No credentials are hardcoded in the
application code.

**8. Database Schema Deployment**

Example schema for the tables used by this project. Adjust column names
and types to match the actual project schema.

**Student Table**

  -----------------------------------------------------------------------
  **Column**       **Data Type**    **Primary     **Constraints**
                                    Key**         
  ---------------- ---------------- ------------- -----------------------
  id               SERIAL           Yes           Primary key

  name             VARCHAR(255)     No            NOT NULL

  age              INTEGER          No            NOT NULL

  department       VARCHAR(100)     No            NOT NULL

  priority         INTEGER          No            DEFAULT 3

  execute_at       TIMESTAMP        No            Nullable

  created_at       TIMESTAMP        No            DEFAULT now()
  -----------------------------------------------------------------------

**Employee Table**

  -----------------------------------------------------------------------
  **Column**       **Data Type**    **Primary     **Constraints**
                                    Key**         
  ---------------- ---------------- ------------- -----------------------
  id               SERIAL           Yes           Primary key

  name             VARCHAR(255)     No            NOT NULL

  role             VARCHAR(100)     No            NOT NULL

  department       VARCHAR(100)     No            NOT NULL

  priority         INTEGER          No            DEFAULT 3

  execute_at       TIMESTAMP        No            Nullable

  created_at       TIMESTAMP        No            DEFAULT now()
  -----------------------------------------------------------------------

Add any other project-specific tables using the same documentation
format: column name, data type, primary key, and constraints.

**Executing the Schema**

psql -U \<DB_USERNAME\> -d \<DB_NAME\> -f schema.sql

**Verifying the Tables**

psql -U \<DB_USERNAME\> -d \<DB_NAME\> -c \"\\d student\" psql -U
\<DB_USERNAME\> -d \<DB_NAME\> -c \"\\d employee\"

**9. Environment Configuration**

The application is configured entirely through environment variables,
loaded from a .env file in the project directory.

REDIS_HOST=\<REDIS_HOST\> REDIS_PORT=6379 REDIS_STREAM=\<STREAM_NAME\>
REDIS_GROUP=\<CONSUMER_GROUP\> REDIS_CONSUMER=\<CONSUMER_NAME\>
POSTGRES_HOST=\<POSTGRES_HOST\> POSTGRES_PORT=5432
POSTGRES_DB=\<DATABASE_NAME\> POSTGRES_USER=\<DATABASE_USER\>
POSTGRES_PASSWORD=\<DATABASE_PASSWORD\> RETRY_COUNT=\<MAX_RETRIES\>

**Why Environment Variables Are Used**

Environment variables separate configuration from code, allowing the
same codebase to be deployed to development, staging, and production
with different settings, without any code changes.

**Why Passwords Should Not Be Hardcoded**

Hardcoded credentials end up in version control history, are difficult
to rotate, and are visible to anyone with source code access.
Environment variables (or a secrets manager) keep credentials out of the
codebase entirely.

**Why .env Should Not Be Committed to Git**

The .env file contains environment-specific secrets. Committing it would
expose production credentials to anyone with repository access,
including in the commit history even if later removed.

**Configuring .gitignore**

.env venv/ \_\_pycache\_\_/ \*.pyc

**10. Redis Stream and Consumer Group Deployment**

  ---------------------------------------------------------------------------
  **Item**            **Description**                    **Example**
  ------------------- ---------------------------------- --------------------
  Stream name         The Redis Stream key that receives \<STREAM_NAME\>
                      messages                           

  Consumer group name Shared group used to distribute    \<CONSUMER_GROUP\>
                      message reads                      

  Consumer name       Unique name for this consumer      \<CONSUMER_NAME\>
                      instance                           
  ---------------------------------------------------------------------------

**Message Structure**

Example fields expected in each message payload:

-   table --- required, target table name

-   id --- required, unique record identifier

-   name --- required

-   age --- required (for student records)

-   department --- required

-   priority --- optional, defaults to a standard priority if omitted

-   execute_at --- optional, ISO timestamp; message is deferred until
    this time if present

**Creating the Consumer Group**

redis-cli XGROUP CREATE \<STREAM_NAME\> \<CONSUMER_GROUP\> \$ MKSTREAM

**Verifying the Consumer Group**

redis-cli XINFO GROUPS \<STREAM_NAME\>

**If the Consumer Group Already Exists**

Redis returns a BUSYGROUP error if XGROUP CREATE is run against an
existing group. This is expected on redeployments; the command can be
safely skipped or wrapped to ignore this specific error, since the group
and its state should be preserved across deployments.

**11. Python Consumer Deployment**

35. Activate the virtual environment.

source venv/bin/activate

36. Load environment variables (handled automatically by python-dotenv
    when the consumer starts, provided the .env file is present).

37. Start the consumer.

python consumer.py

38. Verify the Redis connection (check startup log).

39. Verify the PostgreSQL connection (check startup log).

40. Verify message processing by sending a test message.

**Expected Startup Logs**

Connected to Redis Connected to PostgreSQL Consumer started Waiting for
messages

**Testing the Consumer**

redis-cli XADD \<STREAM_NAME\> \'\*\' table student id 1 name \"Test
User\" age 21 department \"CS\" priority 1

Confirm the message appears in the consumer\'s log output and that a
corresponding row appears in PostgreSQL.

**12. End-to-End Deployment Test**

41. Add a test message to the Redis Stream.

42. Verify the message exists in Redis (XRANGE/XLEN).

43. Verify the Python consumer receives the message (check logs).

44. Verify validation is successful (no validation error logged).

45. Verify execute_at processing if provided (message is
    deferred/processed at the correct time).

46. Verify priority processing (higher-priority messages processed first
    in a batch).

47. Verify the data is inserted into PostgreSQL.

48. Verify the message is acknowledged in Redis (no longer in XPENDING).

49. Verify the success notification/email is sent.

50. Verify the complete system flow end-to-end, from message submission
    to database record to notification.

**13. Failure Deployment Testing**

  ------------------------------------------------------------------------------------------------
  **Scenario**   **Expected      **Retry Behavior**   **Max Retry   **DLQ        **Recovery
                 Behavior**                           Behavior**    Behavior**   Procedure**
  -------------- --------------- -------------------- ------------- ------------ -----------------
  Redis is       Consumer logs a N/A                  N/A           N/A          Restart Redis;
  unavailable    connection      (connection-level,                              consumer
                 error and       not message-level                               reconnects
                 retries         retry)                                          automatically
                 connecting                                                      

  PostgreSQL is  Consumer logs a Message retried per  Moves to DLQ  Message      Restore
  unavailable    connection      policy               if DB remains appears in   PostgreSQL;
                 error for the                        unavailable   DLQ          reprocess DLQ
                 affected                             through max                messages
                 message                              retries                    

  Invalid Redis  Validation      Depends on policy    Moves to DLQ  Message      Developer
  message        fails, error    (often retried once)               appears in   corrects payload
                 logged                                             DLQ          and reprocesses

  Missing        Validation      Depends on policy    Moves to DLQ  Message      Developer
  required field fails, error                                       appears in   corrects payload
                 logged                                             DLQ          and reprocesses

  Duplicate      Insertion fails Retried if failure   Moves to DLQ  Message      Review uniqueness
  database       or upserts      is transient         if persistent appears in   constraints; use
  record         depending on                                       DLQ          idempotent
                 schema                                                          upserts
                 constraints                                                     

  Database       Error logged    Retried per policy   Moves to DLQ  Message      Fix root cause;
  insertion      with reason                          after max     appears in   reprocess
  failure                                             retries       DLQ          

  Python         Process exits;  N/A                  N/A           N/A          Confirm restart
  consumer crash process manager                                                 via
                 restarts it                                                     systemd/Docker;
                                                                                 check XPENDING
                                                                                 for orphaned
                                                                                 messages

  Message        Error logged,   Retried per policy   Moves to DLQ  Message      Investigate root
  processing     message not                          after max     appears in   cause; reprocess
  failure        acknowledged                         retries       DLQ          corrected message
  (general)                                                                      
  ------------------------------------------------------------------------------------------------

**14. Production Process Management**

The consumer must run continuously in production and restart
automatically after failure or server reboot. Options include systemd,
Supervisor, or Docker.

**Recommended systemd Service: consumer.service**

\[Unit\] Description=Redis to PostgreSQL Python Consumer
After=network.target redis.service postgresql.service \[Service\]
Type=simple User=\<SERVICE_USER\>
WorkingDirectory=/opt/\<project-directory\>
EnvironmentFile=/opt/\<project-directory\>/.env
ExecStart=/opt/\<project-directory\>/venv/bin/python consumer.py
Restart=always RestartSec=5 \[Install\] WantedBy=multi-user.target

**Creating and Managing the Service**

sudo cp consumer.service /etc/systemd/system/consumer.service sudo
systemctl daemon-reload sudo systemctl start consumer sudo systemctl
stop consumer sudo systemctl restart consumer sudo systemctl status
consumer sudo systemctl enable consumer

Restart=always with RestartSec ensures the consumer restarts
automatically after a crash, and WantedBy=multi-user.target ensures it
starts automatically after a server reboot once enabled.

**15. Docker Deployment Option**

As an alternative to native installation, the full system can be
deployed using Docker and Docker Compose.

**Dockerfile for the Python Consumer**

FROM python:3.11-slim WORKDIR /app COPY requirements.txt . RUN pip
install \--no-cache-dir -r requirements.txt COPY . . CMD \[\"python\",
\"consumer.py\"\]

**docker-compose.yml**

version: \"3.9\" services: redis: image: redis:7 ports: - \"6379:6379\"
volumes: - redis_data:/data networks: - backend postgres: image:
postgres:15 environment: POSTGRES_DB: \<DATABASE_NAME\> POSTGRES_USER:
\<DATABASE_USER\> POSTGRES_PASSWORD: \<DATABASE_PASSWORD\> ports: -
\"5432:5432\" volumes: - postgres_data:/var/lib/postgresql/data
networks: - backend consumer: build: . depends_on: - redis - postgres
env_file: - .env networks: - backend restart: always networks: backend:
volumes: redis_data: postgres_data:

**Managing the Docker Deployment**

docker compose up -d docker compose ps docker compose logs -f consumer
docker compose restart consumer

**Native Installation vs Docker Deployment**

  -----------------------------------------------------------------------
  **Aspect**         **Native Installation**   **Docker Deployment**
  ------------------ ------------------------- --------------------------
  Setup complexity   Requires manual install   Single docker-compose up
                     of each service           -d command

  Isolation          Services share the host   Each service runs in its
                     OS                        own container

  Portability        Tied to the specific      Highly portable across
                     server configuration      environments

  Resource overhead  Lower overhead            Slightly higher due to
                                               containerization
  -----------------------------------------------------------------------

**16. Network and Port Configuration**

  ------------------------------------------------------------------------
  **Service**         **Port**         **Exposure**
  ------------------- ---------------- -----------------------------------
  Redis               6379             Private (internal network only)

  PostgreSQL          5432             Private (internal network only)

  Python Consumer     None required    No public port required if it only
                                       consumes Redis messages
  ------------------------------------------------------------------------

**Firewall Configuration Example**

\# Allow only the application server to reach Redis and PostgreSQL sudo
ufw allow from \<APP_SERVER_IP\> to any port 6379 sudo ufw allow from
\<APP_SERVER_IP\> to any port 5432 sudo ufw deny 6379 sudo ufw deny 5432

Redis and PostgreSQL should not be exposed to the public internet.
Internal communication between the consumer, Redis, and PostgreSQL
should occur over a private network or Docker bridge network.

**17. Security Configuration**

-   Do not hardcode passwords anywhere in the codebase.

-   Use environment variables (or a secrets manager) for all
    credentials.

-   Use strong, unique database passwords.

-   Restrict Redis access to trusted internal hosts only; enable
    requirepass.

-   Restrict PostgreSQL access using pg_hba.conf to allow only the
    application host.

-   Configure firewall rules to block public access to Redis and
    PostgreSQL ports.

-   Never expose Redis publicly without authentication.

-   Never expose PostgreSQL publicly unless explicitly required, and
    then only with strict access controls.

-   Use SSH key-based authentication for secure server access; disable
    password-based SSH login.

-   Store secrets in a secrets manager or encrypted vault where
    available, rather than plain .env files, for higher-security
    environments.

**18. Production Deployment Checklist**

**Server**

**☐** Server is available

**☐** Operating system updated

**☐** Required ports configured

**☐** Firewall configured

**Redis**

**☐** Redis installed

**☐** Redis running

**☐** Redis connectivity verified

**☐** Stream verified

**☐** Consumer group verified

**PostgreSQL**

**☐** PostgreSQL installed

**☐** Database created

**☐** User created

**☐** Permissions configured

**☐** Tables created

**☐** Database connectivity verified

**Python**

**☐** Python installed

**☐** Virtual environment created

**☐** Dependencies installed

**☐** Environment variables configured

**☐** Consumer tested

**System**

**☐** End-to-end test completed

**☐** Failure test completed

**☐** Retry verified

**☐** DLQ verified

**☐** Automatic restart configured

**☐** Logs verified

**19. Rollback Procedure**

51. Stop the new Python Consumer.

sudo systemctl stop consumer

52. Restore the previous application version (checkout the previous Git
    tag/commit).

git checkout \<previous-release-tag\>

53. Reinstall the previous dependencies if required.

pip install -r requirements.txt

54. Restart the previous consumer version.

sudo systemctl start consumer

55. Verify Redis connectivity.

56. Verify PostgreSQL connectivity.

57. Verify message processing resumes correctly.

58. Check pending messages and the DLQ for anything affected during the
    rollback window.

Using Git release tags for every production deployment (e.g. v1.2.0,
v1.3.0) makes rollback a simple, predictable git checkout to the last
known-good tag.

**20. Upgrade Procedure**

59. Backup required data (PostgreSQL dump, Redis RDB snapshot).

60. Stop or safely update the consumer (allow the in-flight message to
    finish first).

61. Pull the latest code.

git pull origin main \# or checkout a specific release tag git checkout
\<new-release-tag\>

62. Install new dependencies.

pip install -r requirements.txt

63. Update configuration if required (new .env variables).

64. Run database migrations if required.

psql -U \<DB_USERNAME\> -d \<DB_NAME\> -f migration.sql

65. Start the consumer.

sudo systemctl restart consumer

66. Verify logs for a clean startup.

67. Run end-to-end tests (Section 12).

68. Monitor the system closely for a period after deployment.

**21. Disaster Recovery**

  -----------------------------------------------------------------------
  **Failure Type**    **Recovery Action**
  ------------------- ---------------------------------------------------
  Server failure      Provision a new server and repeat the full
                      deployment procedure (Sections 3--11) using the
                      latest configuration and backups.

  Redis failure       Restart Redis; restore the most recent RDB/AOF
                      backup if data was lost; recreate the consumer
                      group if necessary.

  PostgreSQL failure  Restart PostgreSQL; restore the most recent pg_dump
                      backup if data was lost.

  Python application  Redeploy the consumer from the last known-good
  failure             release tag; restart via the process manager.

  Network failure     Verify connectivity between hosts; check firewall
                      rules; restart affected services once connectivity
                      is restored.
  -----------------------------------------------------------------------

**General Disaster Recovery Steps**

69. Restore the PostgreSQL backup.

70. Restart Redis.

71. Redeploy the Python Consumer.

72. Verify Redis Streams.

73. Verify consumer groups.

74. Verify pending messages.

75. Verify the DLQ.

76. Perform end-to-end testing (Section 12) before resuming normal
    operation.

**22. Complete Deployment Flow**

Fresh Server │ ▼ Install Operating System Dependencies │ ▼ Install Redis
│ ▼ Install PostgreSQL │ ▼ Create Database and Tables │ ▼ Deploy Python
Project │ ▼ Create Virtual Environment │ ▼ Install Dependencies │ ▼
Configure Environment Variables │ ▼ Configure Redis Stream │ ▼ Configure
Consumer Group │ ▼ Start Python Consumer │ ▼ Configure Automatic Restart
│ ▼ Run End-to-End Test │ ▼ Run Failure Test │ ▼ System Ready for
Production

**23. Final Deployment Verification**

Deployment is considered successful only when all of the following are
true:

**☐** Redis is running.

**☐** Redis Stream is working.

**☐** Consumer Group is working.

**☐** Python Consumer is running.

**☐** PostgreSQL is running.

**☐** Database connection is successful.

**☐** Test data reaches PostgreSQL.

**☐** Successful messages are acknowledged.

**☐** Failed messages are retried.

**☐** Permanently failed messages reach the DLQ.

**☐** The consumer restarts automatically after failure.

**☐** The system works after a server reboot.

*End of Document*
