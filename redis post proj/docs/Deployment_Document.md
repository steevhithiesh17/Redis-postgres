# Deployment Document
## Redis Streams-Based Asynchronous Data Processing and Qdrant Semantic Search System

---

## 1. Deployment Overview

This document describes the complete deployment process for the Redis Streams-based asynchronous data processing and Qdrant semantic search system, starting from a fresh Linux server and ending with a fully operational production runtime.

The system is deployed on a **remote Linux server**, accessed from a local computer using SSH. The Python application (the "IITM Project") runs inside a virtual environment on the server, while Redis and Qdrant run as Docker containers on the same host.

```text
                   LOCAL COMPUTER
                         │
                         │ SSH
                         ▼
                 ┌─────────────────┐
                 │  LINUX SERVER   │
                 │                 │
                 │  IITM Project   │
                 │  Python App     │
                 └────────┬────────┘
                          │
                          ▼
                       Docker
                    ┌─────┴─────┐
                    │           │
                    ▼           ▼
                 Redis       Qdrant
                Port 6379   Port 6333
```

Deployment architecture in more detail:

```text
Local Machine
     │
     │ SSH
     ▼
Remote Linux Server
     │
     ├── Python Virtual Environment
     │       │
     │       ├── consumer.py
     │       └── search_consumer.py
     │
     └── Docker
            │
            ├── Redis Container
            │
            └── Qdrant Container
```

**Role of each component:**

- **Redis** stores and streams data using Redis Streams, acting as the asynchronous message backbone of the system.
- **Qdrant** stores vector embeddings and performs semantic similarity search.
- **Python consumers** (document, student, employee, search) process messages from their respective streams.
- **SSH** provides secure remote access to the Linux server for administration and port forwarding.
- **Docker** hosts Redis and Qdrant as isolated, reproducible infrastructure services.

---

## 2. Deployment Prerequisites

Before beginning deployment, confirm that the following prerequisites are available.

| Requirement          | Purpose                      |
| --------------------- | ----------------------------- |
| Linux Server          | Hosts the application and infrastructure |
| SSH Access            | Remote server administration |
| Python 3              | Runs the Python consumers and producer |
| pip                   | Installs Python dependencies |
| Docker                | Runs Redis and Qdrant containers |
| Git or Project Files  | Deploys the application source code |
| Network Access        | Downloads packages, images, and the embedding model |
| Required Ports        | Application connectivity |

**Required ports:**

```text
Redis            → 6379
Qdrant HTTP       → 6333
Qdrant gRPC       → 6334
Redis Stack UI    → 8001   (only if Redis Stack is deployed)
```

Port `8001` is available **only** if the Redis Stack image (`redis/redis-stack`) is used instead of standard Redis. Standard Redis (`redis:6.2`) does not expose a web UI.

---

## 3. Server Access Using SSH

All deployment commands below (unless explicitly marked as run on the local machine) are executed **on the remote Linux server**, after connecting via SSH.

```text
Developer Computer
       │
       │ SSH
       ▼
Linux Server
       │
       └── IITM Project
```

Connect to the server:

```bash
ssh administrator@SERVER_IP
```

After a successful login, the shell prompt changes to:

```text
administrator@template:~$
```

Every command entered after this point runs on the remote server, not the local machine. This distinction matters throughout deployment:

```text
Local Computer            Remote Linux Server
(your laptop)              (where Docker,
                            Redis, Qdrant, and
                            the Python app run)
```

The project files and Docker containers live entirely on the remote server. The local computer is only used to connect via SSH and, later, to access dashboards through port forwarding.

---

## 4. Server Preparation

Once connected, prepare the server with the required system packages.

Update the package index:

```bash
sudo apt update
```

Upgrade existing packages:

```bash
sudo apt upgrade -y
```

Install Python, pip, virtual environment support, Git, and curl:

```bash
sudo apt install -y python3 python3-pip python3-venv git curl
```

Verify each tool installed correctly:

```bash
python3 --version
pip3 --version
git --version
curl --version
```

**Why each tool is needed:**

- `python3` — runs the application code (consumers, producer, services).
- `python3-pip` — installs Python package dependencies from `requirements.txt`.
- `python3-venv` — creates an isolated Python environment for the project.
- `git` — used to clone the project repository (if deploying via Git).
- `curl` — used to test HTTP endpoints such as the Qdrant API.

---

## 5. Docker Installation and Verification

Docker must be installed and running before Redis or Qdrant can be deployed. Install Docker using your distribution's standard installation method, then verify it:

```bash
docker --version
```

If the current user has not been added to the `docker` group, commands may fail with:

```text
permission denied while trying to connect to the Docker API
```

Until the user is added to the `docker` group (and the session refreshed), run Docker commands with `sudo`:

```bash
sudo docker ps
```

List running containers:

```bash
sudo docker ps
```

List all containers, including stopped ones:

```bash
sudo docker ps -a
```

**Difference between `docker ps` and `sudo docker ps`:**

- `docker ps` — works only if the current user has Docker group permissions.
- `sudo docker ps` — always works, since it runs with root privileges. Use this form until group permissions are confirmed.

---

## 6. Project Deployment

Deploy the project into a dedicated directory on the server, conventionally `~/IITM`.

Expected project structure:

```text
IITM/
│
├── consumer.py
├── search_consumer.py
├── producer.py
│
├── config.py
├── redis_client.py
│
├── validation.py
├── retry_handler.py
├── dlq_handler.py
├── email_service.py
│
├── embedding_service.py
├── qdrant_service.py
├── qdrant_helper.py
├── search_service.py
│
├── dlq_review.py
├── requirements.txt
├── docker-compose.yml
├── README.md
│
└── venv/
```

The project can be deployed in one of two ways:

1. **Git Clone** — clone the repository directly onto the server.
2. **Copy Project Files** — transfer the project files (for example with `scp` or `rsync`) from the local machine to the server.

After deployment, confirm the files are present:

```bash
cd ~/IITM
ls
```

Check that all core files listed above exist before proceeding.

---

## 7. Python Virtual Environment Deployment

Create an isolated Python environment inside the project directory:

```bash
python3 -m venv venv
```

Activate it:

```bash
source venv/bin/activate
```

Once active, the shell prompt changes to:

```text
(venv) administrator@template:~/IITM$
```

Install all Python dependencies:

```bash
pip install -r requirements.txt
```

Verify installed packages:

```bash
pip list
```

**Why activation matters:** `consumer.py`, `search_consumer.py`, and `producer.py` depend on packages installed inside `venv`. If the virtual environment is not activated, these scripts may fail with missing-module errors or may accidentally use a different, system-wide Python interpreter.

---

## 8. Redis Deployment

Redis is deployed as a Docker container and acts as the message streaming backbone for the application.

```text
Python Application
       │
       │ Redis Protocol
       ▼
Redis Container
       │
       ▼
Port 6379
```

Run the Redis container:

```bash
sudo docker run -d \
  --name redis \
  -p 6379:6379 \
  redis:6.2
```

Verify the container is running:

```bash
sudo docker ps
```

Test connectivity by entering the Redis CLI:

```bash
sudo docker exec -it redis redis-cli
```

The prompt changes to:

```text
127.0.0.1:6379>
```

Inside the Redis CLI, test with:

```text
PING
```

Expected response:

```text
PONG
```

**Important:** Redis commands (such as `PING` or `XADD`) must be entered **inside** the Redis CLI prompt, not the Linux shell.

```text
Correct:
127.0.0.1:6379> XRANGE document_stream - +

Incorrect:
administrator@template:~$ XRANGE document_stream - +
```

---

## 9. Qdrant Deployment

Qdrant is deployed as a second Docker container and stores vector embeddings for semantic search.

```text
Python Application
       │
       ▼
Qdrant Client
       │
       ▼
Qdrant Container
       │
       ├── Port 6333 HTTP
       └── Port 6334 gRPC
```

Run the Qdrant container:

```bash
sudo docker run -d \
  --name qdrant \
  -p 6333:6333 \
  -p 6334:6334 \
  qdrant/qdrant
```

Verify it is running:

```bash
sudo docker ps
```

Test the HTTP API:

```bash
curl http://localhost:6333
```

A successful response confirms Qdrant is reachable.

- **Port 6333** — Qdrant HTTP API and web dashboard.
- **Port 6334** — Qdrant gRPC interface, used for higher-performance client communication.

---

## 10. Docker Deployment Verification

After both containers are started, confirm the overall Docker state:

```text
CONTAINER ID   IMAGE             STATUS       PORTS

redis          redis:6.2        Up           6379

qdrant         qdrant/qdrant    Up           6333-6334
```

```text
                 Docker Host
                     │
            ┌────────┴────────┐
            │                 │
            ▼                 ▼
        Redis             Qdrant
       Port 6379        Ports 6333/6334
```

Run the following to confirm health:

```bash
sudo docker ps
sudo docker logs redis
sudo docker logs qdrant
```

Both containers should show a status of `Up`, and their logs should show normal startup messages without errors.

---

## 11. Application Configuration

Before starting any Python consumer, the application must be correctly configured.

Configuration typically covers:

```text
Redis Host
Redis Port
Qdrant URL
Qdrant Collection
Stream Names
Consumer Groups
Consumer Names
Retry Settings
Email Configuration
```

```text
config.py
    │
    ├── Redis Configuration
    │
    ├── Qdrant Configuration
    │
    ├── Redis Streams
    │
    ├── Consumer Groups
    │
    └── Retry Configuration
```

All application components — the producer and every consumer — must point to the **same** Redis instance and the **same** Qdrant instance. Misconfigured hosts or ports are one of the most common causes of deployment failures.

---

## 12. Environment Variables

Sensitive configuration values should be supplied through environment variables rather than hardcoded in source files.

Typical variables:

```text
REDIS_HOST
REDIS_PORT
QDRANT_URL
QDRANT_COLLECTION
SMTP_HOST
SMTP_PORT
SMTP_USERNAME
SMTP_PASSWORD
```

```text
Application
      │
      ▼
Environment Variables
      │
      ▼
Configuration
```

**Do not hardcode:**

- Passwords
- SMTP credentials
- API keys
- Any production secret

Use a `.env` file (excluded from version control) or the server's environment configuration to inject these values at runtime.

---

## 13. Redis Stream Initialization

Redis Streams do not need to be created manually — each stream is created automatically the first time a message is added to it with `XADD`.

Streams used by the system:

```text
document_stream
student_stream
employee_stream
search_stream
```

```text
Redis
 │
 ├── document_stream
 ├── student_stream
 ├── employee_stream
 └── search_stream
```

To initialize the document stream, enter the Redis CLI:

```bash
redis-cli
```

Then add a message:

```text
XADD document_stream * \
document_id DOC3001 \
title "Redis Streams" \
text "Redis Streams allow asynchronous processing." \
priority HIGH \
retry_count 0
```

If `document_stream` does not already exist, Redis creates it automatically at this point.

---

## 14. Consumer Group Deployment

Each stream has a corresponding consumer group, which tracks message delivery and acknowledgment for its consumer.

```text
document_stream
       │
       ▼
document_group
       │
       ▼
Document Consumer
```

```text
student_stream  →  student_group
employee_stream →  employee_group
search_stream   →  search_group
```

Consumer groups are created automatically by the Python application on startup — no manual `XGROUP CREATE` step is required under normal operation, but understanding this mapping is important for troubleshooting stream/group mismatches.

---

## 15. Embedding Model Deployment

On first startup, each consumer that generates embeddings loads the embedding model.

```text
Python Consumer
       │
       ▼
EmbeddingService
       │
       ▼
Load Embedding Model
       │
       ▼
Model Cache
       │
       ▼
Generate Vectors
```

Notes on first startup:

- The model may take noticeably longer to load the **first** time, since it may need to download model files.
- Subsequent restarts typically load faster, using the local model cache.
- CPU-based inference is slower than GPU-based inference; expect longer embedding generation times on CPU-only servers.

Expected log line once the model has loaded successfully:

```text
Embedding model loaded
```

---

## 16. Starting the Document Consumer

Activate the environment and start the document consumer:

```bash
cd ~/IITM
source venv/bin/activate
python3 consumer.py document
```

Expected startup output:

```text
Embedding model loaded
Starting document consumer
```

```text
consumer.py
     │
     ▼
document_stream
     │
     ▼
document_group
```

---

## 17. Starting the Student Consumer

```bash
python3 consumer.py student
```

```text
student_stream
       │
       ▼
student_group
       │
       ▼
Student Consumer
```

The student consumer processes student records independently of the document and employee consumers.

---

## 18. Starting the Employee Consumer

```bash
python3 consumer.py employee
```

```text
employee_stream
       │
       ▼
employee_group
       │
       ▼
Employee Consumer
```

Employee message processing is isolated from document and student processing, so a failure in one consumer does not affect the others.

---

## 19. Starting the Search Consumer

```bash
python3 search_consumer.py
```

```text
search_stream
       │
       ▼
search_group
       │
       ▼
search_consumer.py
       │
       ▼
Qdrant
```

Expected startup output:

```text
Embedding model loaded
Starting search consumer
```

---

## 20. Running Multiple Consumers

Each consumer should run continuously in its own terminal session (or process, when using a process manager — see Section 38).

```text
Terminal 1  → Document Consumer
Terminal 2  → Student Consumer
Terminal 3  → Employee Consumer
Terminal 4  → Search Consumer
```

```text
                   Redis
                     │
        ┌────────────┼────────────┐
        │            │            │
        ▼            ▼            ▼
    Document      Student      Employee
    Consumer      Consumer     Consumer
                     │
                     ▼
                Search Consumer
```

Every terminal session must activate the virtual environment before starting its process:

```bash
source venv/bin/activate
```

---

## 21. Document Deployment Test

Use this end-to-end test to confirm the document pipeline works correctly.

**Step 1 — Enter the Redis CLI:**

```bash
redis-cli
```

**Step 2 — Add a test message:**

```text
XADD document_stream * \
document_id DOC3001 \
title "Redis Streams" \
text "Redis Streams allow applications to send and process messages asynchronously." \
priority HIGH \
retry_count 0
```

**Step 3 — Expected processing flow:**

```text
Document Consumer
       │
       ▼
Reads Message
       │
       ▼
Validates
       │
       ▼
Generates Embedding
       │
       ▼
Stores in Qdrant
       │
       ▼
Sends Email
       │
       ▼
XACK
```

**Step 4 — Verify** that the corresponding point was created in the Qdrant collection (see Section 25 for dashboard access).

---

## 22. Student Deployment Test

```text
XADD student_stream * \
student_id STU1001 \
name "Ravi" \
age 22 \
department CSE \
priority HIGH
```

```text
Redis
  ↓
student_stream
  ↓
Student Consumer
  ↓
Validation
  ↓
Processing
  ↓
Qdrant
  ↓
Success
```

---

## 23. Employee Deployment Test

```text
XADD employee_stream * \
employee_id EMP3001 \
name "Employee One" \
department IT \
role Developer \
priority HIGH
```

```text
Redis
  ↓
employee_stream
  ↓
Employee Consumer
  ↓
Validation
  ↓
Processing
  ↓
Qdrant
  ↓
Success
```

Field names in test messages must exactly match the validation rules implemented in `validation.py`; mismatched field names are a common cause of test failures.

---

## 24. Search Deployment Test

Before running a search test, ensure that at least one document has already been successfully inserted into Qdrant.

```text
XADD search_stream * \
command search \
query "What are Redis Streams?" \
top_k 5
```

```text
Search Command
      │
      ▼
search_stream
      │
      ▼
Search Consumer
      │
      ▼
Query Embedding
      │
      ▼
Qdrant Search
      │
      ▼
Top-K Results
```

Expected log output:

```text
Performing semantic search for query
Search returned results
Published search results
```

---

## 25. Qdrant Dashboard Access

Qdrant runs on the remote server, so its dashboard is accessed locally through an SSH tunnel.

```text
Local Browser
     │
     ▼
SSH Tunnel
     │
     ▼
Remote Server
     │
     ▼
Qdrant Port 6333
```

From the **local machine**, open an SSH tunnel:

```bash
ssh -L 6333:localhost:6333 administrator@SERVER_IP
```

Then, in the local browser, open:

```text
http://localhost:6333/dashboard
```

`localhost` here refers to the **local computer**; the SSH tunnel transparently forwards that connection to Qdrant running on the remote server.

To confirm Qdrant is reachable directly on the server, run:

```bash
curl http://localhost:6333
```

---

## 26. Redis Web UI Deployment

It is important to distinguish between standard Redis and Redis Stack.

**Standard Redis** (`redis:6.2`) provides:

```text
Redis Server
Redis CLI
Redis Streams
```

It does **not** provide a web UI on port `8001`.

**Redis Stack** (`redis/redis-stack`) additionally provides:

```text
Redis Server
Redis Stack Features
Redis Insight Web UI
```

If Redis Stack is deployed instead of standard Redis, expose port `8001`:

```text
8001:8001
```

Then, from the local machine, open an SSH tunnel:

```bash
ssh -L 8001:localhost:8001 administrator@SERVER_IP
```

And open locally:

```text
http://localhost:8001
```

If the deployed container is only `redis:6.2`, port `8001` will **not** work unless a separate Redis Stack / Redis Insight service is deployed alongside it.

---

## 27. SSH Port Forwarding

Overall port forwarding architecture:

```text
LOCAL COMPUTER
     │
     │ localhost:6333
     │
     │ SSH Tunnel
     ▼
REMOTE SERVER
     │
     └── Qdrant:6333
```

Qdrant dashboard tunnel:

```bash
ssh -L 6333:localhost:6333 administrator@SERVER_IP
```

Redis UI tunnel (Redis Stack only):

```bash
ssh -L 8001:localhost:8001 administrator@SERVER_IP
```

```text
Local Port
     │
     ▼
SSH Tunnel
     │
     ▼
Remote Port
```

---

## 28. Deployment Verification Checklist

**Server**

```text
☐ SSH connection successful
☐ Python installed
☐ pip installed
☐ Virtual environment created
☐ Dependencies installed
```

**Docker**

```text
☐ Docker installed
☐ Redis container running
☐ Qdrant container running
```

**Redis**

```text
☐ Redis responds to PING
☐ document_stream available
☐ student_stream available
☐ employee_stream available
☐ search_stream available
```

**Qdrant**

```text
☐ Qdrant API accessible
☐ Collection created
☐ Vector points inserted
☐ Search returns results
```

**Python**

```text
☐ Document consumer running
☐ Student consumer running
☐ Employee consumer running
☐ Search consumer running
```

**Application**

```text
☐ Message received
☐ Validation successful
☐ Embedding generated
☐ Qdrant upsert successful
☐ Email sent
☐ Redis XACK completed
```

---

## 29. Deployment Health Checks

```text
Server
  │
  ▼
Docker
  │
  ▼
Redis
  │
  ▼
Qdrant
  │
  ▼
Python Consumers
  │
  ▼
End-to-End Test
```

Run the following checks in sequence:

```bash
sudo docker ps
curl http://localhost:6333
sudo docker exec -it redis redis-cli PING
ps aux | grep python
```

A healthy deployment shows: both containers `Up`, a `200`-style response from Qdrant, `PONG` from Redis, and four running `python3` processes (document, student, employee, and search consumers).

---

## 30. Application Logs

Application logs are the primary source of information for confirming correct behavior and diagnosing issues.

**Expected logs during normal operation:**

```text
Embedding model loaded
Starting consumer
Received message
Validation successful
Generating embedding
Qdrant upsert successful
Email sent
Message acknowledged
```

**Logs indicating a failure condition:**

```text
Validation failed
Retrying message
Search processing failed
Published message to DLQ
```

Always check application logs first when troubleshooting a deployment issue, before investigating Redis or Qdrant directly.

---

## 31. Docker Logs

Redis logs:

```bash
sudo docker logs redis
```

Follow Redis logs in real time:

```bash
sudo docker logs -f redis
```

Qdrant logs:

```bash
sudo docker logs qdrant
```

Follow Qdrant logs in real time:

```bash
sudo docker logs -f qdrant
```

The `-f` flag means "follow" — it streams new log lines live instead of only showing historical output.

---

## 32. Restart Procedure

Restarts should always follow this order to avoid inconsistent state:

```text
Stop Python Consumers
        │
        ▼
Restart Redis/Qdrant if Required
        │
        ▼
Verify Infrastructure
        │
        ▼
Restart Python Consumers
```

Restart Redis:

```bash
sudo docker restart redis
```

Restart Qdrant:

```bash
sudo docker restart qdrant
```

Then restart the application layer:

```text
consumer.py
search_consumer.py
```

After any infrastructure restart, re-verify that consumer groups and stream data are intact before resuming normal operation.

---

## 33. Stop Procedure

Correct shutdown order:

```text
Stop Consumers
     │
     ▼
Stop Application
     │
     ▼
Stop Docker Services
```

Stop the Docker containers:

```bash
sudo docker stop redis
sudo docker stop qdrant
```

**Command distinctions:**

- `docker stop` — stops a running container, preserving its data and configuration.
- `docker start` — starts an existing, previously-stopped container.
- `docker rm` — permanently removes a container; use with caution, and only after confirming the container is no longer needed and any required data is backed up or stored in a persistent volume.

---

## 34. Deployment Troubleshooting

| Problem                           | Cause                                | Solution                             |
| ---------------------------------- | -------------------------------------- | --------------------------------------- |
| SSH connection fails               | Incorrect IP or credentials            | Verify server details                   |
| `docker` permission denied         | User lacks Docker access               | Use `sudo docker`                       |
| Redis connection refused           | Redis container stopped                | Check `sudo docker ps`                  |
| Qdrant connection refused          | Qdrant stopped                         | Restart Qdrant                          |
| `XADD: command not found`          | Command run in Linux shell             | Enter Redis CLI first                   |
| `unknown command docker`           | Docker command run inside Redis CLI    | Exit Redis CLI                          |
| Consumer receives nothing          | Wrong stream/group                     | Check Redis stream contents             |
| `document_id is required`          | Invalid message                        | Add the required field                  |
| Search returns empty results       | No vectors in Qdrant                   | Insert documents first                  |
| Model loading takes time           | First model startup                    | Wait for model initialization           |
| Port 6333 unavailable              | Port conflict                          | Check running processes on that port    |
| Port 8001 unavailable              | Redis Stack UI not deployed             | Deploy Redis Stack                      |
| SSH tunnel fails                   | Incorrect server IP                    | Verify the server is reachable          |
| Localhost dashboard does not open  | Tunnel not running                     | Recreate the SSH tunnel                 |
| Qdrant search API error            | Client API mismatch                    | Use a compatible Qdrant client version  |

---

## 35. Data Persistence and Volumes

Production deployments should use persistent Docker volumes so that data survives container restarts or recreation.

**Redis:**

```text
Redis Container
       │
       ▼
Persistent Volume
       │
       ▼
Redis Data
```

**Qdrant:**

```text
Qdrant Container
       │
       ▼
Persistent Volume
       │
       ▼
Collections
Vectors
Payloads
```

Persistent volumes matter because they provide:

- Safety across container restarts
- Long-term data persistence
- A basis for recovery after failures
- Protection against data loss when containers are recreated or upgraded

---

## 36. Backup and Recovery

**Redis backup/recovery flow:**

```text
Redis Data
     │
     ▼
Backup
     │
     ▼
Recovery
```

**Qdrant backup/recovery flow:**

```text
Qdrant Collection
       │
       ▼
Snapshot
       │
       ▼
Backup Storage
```

Production deployments should maintain regular backups of:

- Redis data
- Qdrant snapshots
- Project source code
- Configuration files
- Secrets, stored securely (never in public or unencrypted backups)

Never store passwords or credentials in plaintext backup files.

---

## 37. Production Deployment Architecture

```text
                    Internet
                       │
                       ▼
                 Firewall
                       │
                       ▼
                Linux Server
                       │
              ┌────────┴────────┐
              │                 │
              ▼                 ▼
           Docker            Python
              │              Services
        ┌─────┴─────┐
        │           │
        ▼           ▼
      Redis       Qdrant
```

A production-grade deployment should include:

- A firewall restricting access to only required ports
- SSH access limited to authorized administrators
- Managed application process lifecycle (see Section 38)
- Persistent storage for Redis and Qdrant
- Ongoing monitoring of logs, containers, and consumers

---

## 38. Recommended Production Process Management

Running the consumers manually is appropriate for development and testing:

```bash
python3 consumer.py
```

For production, use a process manager instead, such as:

```text
systemd
Supervisor
Docker Compose
Docker
Kubernetes
```

```text
Process Manager
      │
      ├── Document Consumer
      ├── Student Consumer
      ├── Employee Consumer
      └── Search Consumer
```

A process manager provides automatic:

- Startup on boot
- Restart on failure
- Failure recovery and retry
- Centralized log management

---

## 39. Complete Deployment Flow

```text
START
  │
  ▼
Connect to Server Using SSH
  │
  ▼
Prepare Linux Server
  │
  ▼
Install Python and Docker
  │
  ▼
Deploy Project Files
  │
  ▼
Create Virtual Environment
  │
  ▼
Install Python Dependencies
  │
  ▼
Start Redis Container
  │
  ▼
Start Qdrant Container
  │
  ▼
Verify Infrastructure
  │
  ▼
Load Embedding Model
  │
  ▼
Start Python Consumers
  │
  ▼
Create Redis Consumer Groups
  │
  ▼
Send Test Message
  │
  ▼
Process Message
  │
  ▼
Store Vector in Qdrant
  │
  ▼
Run Search Test
  │
  ▼
Verify Results
  │
  ▼
DEPLOYMENT COMPLETE
```

---

## 40. Final Deployment Architecture

```text
                    LOCAL COMPUTER
                          │
                          │ SSH
                          ▼
                 ┌──────────────────┐
                 │  REMOTE LINUX    │
                 │     SERVER       │
                 │                  │
                 │  IITM Project    │
                 │  Python venv     │
                 └────────┬─────────┘
                          │
                          ▼
                       Docker
                    ┌─────┴─────┐
                    │           │
                    ▼           ▼
                 Redis       Qdrant
                :6379       :6333
                              :6334
                    │           │
                    └─────┬─────┘
                          │
                          ▼
                    Python Consumers
                          │
                ┌─────────┼─────────┐
                │         │         │
                ▼         ▼         ▼
             Document   Student   Employee
             Consumer   Consumer  Consumer
                          │
                          ▼
                    Search Consumer
```

The deployment process establishes the complete runtime environment for the asynchronous Redis Streams and Qdrant semantic search platform. The Linux server hosts the Python application, Redis provides message streaming, Qdrant provides vector storage and semantic search, and Docker provides isolated infrastructure services. SSH provides secure remote administration, and port forwarding enables local access to remote dashboards and services.

---

### Document Set

```text
1. Operational Document   → How to run, monitor, and troubleshoot
2. Design Document         → How the system is architected
3. Technical Document      → How the code and technologies work
4. Deployment Document     → How to install and deploy the system  (this document)
```

The next recommended document is a **User Guide / Usage Document**, explaining how a normal user or developer sends documents, student data, employee data, and search queries into the system.
