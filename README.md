# Distributed Compute Scheduler

A fault-tolerant distributed compute platform for scheduling heterogeneous workloads across dynamically scaled workers.

**Tech Stack:** Python · FastAPI · Redis Streams · PostgreSQL · SQLAlchemy · Docker

---

## Overview

The Distributed Compute Scheduler is a backend system designed to distribute computational workloads across multiple workers while maintaining job state, handling failures, and supporting retries.

The project explores core distributed systems concepts, including asynchronous job processing, message acknowledgment, worker coordination, fault tolerance, execution timeouts, and horizontal scaling.

Clients submit jobs through a REST API. The scheduler places work onto a Redis Stream, where independent workers consume and execute jobs. The system tracks job lifecycle state separately from Redis message state, allowing it to reason about both execution progress and message delivery.

The project is currently evolving toward using PostgreSQL as the durable source of truth for job metadata and execution state, while Redis remains responsible for message distribution and worker coordination.

## Motivation

Modern cloud platforms execute large numbers of computational jobs across distributed worker nodes. These systems must distribute workloads efficiently while accounting for worker failures, execution limits, retries, and changing resource availability.

This project implements a simplified batch-computing platform to explore how these systems are designed and built. The goal is to develop a deeper understanding of backend infrastructure, distributed execution, and fault-tolerant system architecture.

---

# Architecture

## System Overview

The intended architecture separates job submission, message distribution, job persistence, and execution.

```text
                         ┌──────────────────┐
                         │      Client      │
                         │    REST API      │
                         └────────┬─────────┘
                                  │
                                  ▼
                         ┌──────────────────┐
                         │     FastAPI      │
                         │   API Server     │
                         └────────┬─────────┘
                                  │
                         Submit / Query Jobs
                                  │
                    ┌─────────────┴─────────────┐
                    │                           │
                    ▼                           ▼
          ┌──────────────────┐        ┌──────────────────┐
          │    PostgreSQL    │        │      Redis       │
          │  Durable Job Data│        │   Redis Streams  │
          │                  │        │   Message Queue  │
          └──────────────────┘        └────────┬─────────┘
                                               │
                                  ┌────────────┼────────────┐
                                  │            │            │
                                  ▼            ▼            ▼
                            ┌──────────┐ ┌──────────┐ ┌──────────┐
                            │ Worker 1 │ │ Worker 2 │ │ Worker N │
                            └────┬─────┘ └────┬─────┘ └────┬─────┘
                                 │            │            │
                                 └────────────┼────────────┘
                                              ▼
                                    ┌──────────────────┐
                                    │  Job Execution   │
                                    │     Engine       │
                                    └──────────────────┘

                           ┌──────────────────────┐
                           │  Retry Manager       │
                           │  Worker Monitor      │
                           └──────────────────────┘
```

*Note: PostgreSQL integration and the transition to this fully separated architecture are in progress.*

## Core Components

### FastAPI — Job Submission API

FastAPI exposes the scheduler through a REST interface.

Responsibilities include:
- Accepting job submissions.
- Validating job requests.
- Assigning unique job identifiers.
- Managing job creation and persistence.
- Publishing jobs to the Redis queue.
- Providing an interface for clients to interact with the scheduler.

### Redis Streams — Distributed Job Queue

Redis Streams provides asynchronous message distribution between the API and workers.

Responsibilities include:
- Publishing jobs to the stream.
- Distributing work across multiple consumers.
- Managing consumer groups.
- Tracking unacknowledged messages through the Pending Entries List (PEL).
- Acknowledging completed messages using `XACK`.
- Supporting recovery of messages after worker failures.

Redis message state is distinct from the actual execution state of a job.

### PostgreSQL — Durable Job Persistence

PostgreSQL is being integrated as the durable source of truth for job metadata and execution state.

The SQLAlchemy `JobDB` model represents persisted job information, including:
- Job ID and type.
- Payload and priority.
- Current status.
- Assigned worker.
- Retry count.
- Execution timeout.
- Result.
- Creation, start, and completion timestamps.

The intended architecture is for workers to receive a job ID through Redis and retrieve the authoritative job record from PostgreSQL.

### Workers — Distributed Execution

Workers independently consume messages from Redis Streams and execute submitted jobs.

Responsibilities include:
- Consuming jobs through Redis consumer groups.
- Retrieving job information.
- Updating execution state.
- Running supported computational workloads.
- Reporting results.
- Acknowledging processed messages.
- Handling execution failures and timeouts.

Multiple workers can operate concurrently, allowing the scheduler to distribute workloads across independent processes.

### Retry Manager

The retry manager coordinates failed job execution.

Current functionality includes:
- Retry scheduling.
- Exponential backoff.
- Delayed retries.
- Retry-count tracking.

The longer-term goal is to make retry state durable through PostgreSQL and ensure retries interact correctly with worker recovery.

### Worker Monitor

The monitor tracks worker health through periodic heartbeats.

It is designed to detect workers that stop responding and support recovery of work associated with failed workers.

---

# Job Models and Lifecycle

## JobRequest

`JobRequest` represents the information supplied by a client when submitting a job.

Fields include:

| Field | Description |
|---|---|
| `type` | Type of computation to execute |
| `payload` | Parameters required by the computation |
| `priority` | Scheduling priority (0–10) |
| `timeout_seconds` | Maximum permitted execution time |

System-managed information such as job IDs, timestamps, worker assignments, and results is generated internally by the scheduler.

## Job

`Job` represents the scheduler's internal representation of a job throughout its lifecycle.

Important fields include:

| Field | Description |
|---|---|
| `id` | Unique job identifier |
| `type` | Computation type |
| `payload` | Execution parameters |
| `priority` | Scheduling priority |
| `status` | Current lifecycle state |
| `created_at` | Submission timestamp |
| `started_at` | Execution start timestamp |
| `completed_at` | Execution completion timestamp |
| `worker_id` | Assigned worker |
| `retry_count` | Number of execution attempts or retries |
| `timeout_seconds` | Configured execution limit |
| `result` | Execution output |
| `message_id` | Redis Stream message identifier used for acknowledgment |

The Redis `message_id` connects the job's application-level identity to its corresponding stream message. This is important because acknowledging a Redis message requires its stream message ID, not the application's job ID.

## JobStatus

`JobStatus` is an enumeration defining valid states in a job's lifecycle.

```text
                  ┌───────────┐
                  │  QUEUED   │
                  └─────┬─────┘
                        │
                        ▼
                  ┌───────────┐
                  │  RUNNING  │
                  └─────┬─────┘
                        │
              ┌─────────┼──────────┐
              │         │          │
              ▼         ▼          ▼
         COMPLETED    FAILED    TIMED_OUT
                        │
                        ▼
                    RETRYING
                        │
                        ▼
                     QUEUED
```

A job can complete successfully, fail, or exceed its configured execution timeout. Failed jobs may be scheduled for another attempt, subject to retry policies.

---

# Fault Tolerance and Reliability

A major focus of the project is handling failures without relying on every worker remaining operational.

### Consumer Groups and Message Acknowledgment

Redis consumer groups allow multiple workers to process jobs from the same stream.

Messages remain pending until acknowledged. Once a worker successfully processes a message, it acknowledges that message using `XACK`.

### Pending Entries and Worker Recovery

Redis maintains a Pending Entries List containing messages that have been delivered but not acknowledged.

This provides a mechanism for identifying work that may need to be recovered after a worker failure.

### Worker Heartbeats

Workers periodically publish heartbeats. The monitor uses this information to identify workers that may have become unresponsive.

### Retry Scheduling

Failed jobs can be scheduled for another attempt using exponential backoff, reducing the likelihood of repeatedly retrying failing workloads without delay.

### Execution Timeouts

Jobs support configurable execution time limits.

The execution controller uses a separate process to run workloads so that an execution process can be terminated when it exceeds its timeout. This addresses an important limitation of thread-based timeouts: stopping the thread waiting for work does not necessarily stop the underlying computation.

### Stale Result Protection

The scheduler checks job state before accepting execution results. This helps prevent an outdated worker from overwriting a newer job state after a timeout or recovery event.

---

# Redis vs. PostgreSQL: Two Different Responsibilities

The scheduler maintains a distinction between message delivery and job execution.

```text
Redis Stream
    |
    └── Has this message been delivered and acknowledged?
                |
                ▼
              XACK


PostgreSQL Job Record
    |
    └── What happened to the actual job?
                |
                ▼
       QUEUED / RUNNING /
       COMPLETED / FAILED /
       TIMED_OUT
```

These are separate concerns.

- **Redis** manages message delivery, consumer groups, pending messages, acknowledgments, and transient worker coordination.
- **PostgreSQL** is intended to maintain durable job metadata, execution state, and results.

This separation allows the scheduler to reason independently about whether a message was acknowledged and whether the corresponding job actually completed.

---

# Technology Stack

| Technology | Purpose |
|---|---|
| Python | Core application and worker implementation |
| FastAPI | REST API and job submission |
| Redis | Asynchronous messaging and distributed coordination |
| Redis Streams | Job distribution, consumer groups, and pending messages |
| PostgreSQL | Durable job persistence |
| SQLAlchemy | ORM and database interaction |
| Pydantic | Request validation and data models |
| Multiprocessing | Isolated execution and hard timeouts |
| Threading | Background heartbeats and coordination |
| Docker | Containerization and development environment |

---

# Running the Project Locally

Currently, the application is run through multiple Python processes in separate terminals.

## Prerequisites

- Python
- Redis
- PostgreSQL
- Docker Desktop (for running infrastructure)
- Project virtual environment and dependencies

## 1. Start Infrastructure

Start Redis and PostgreSQL through Docker Compose:

```powershell
docker compose up -d redis postgres
```

## 2. Initialize or Reset Database Tables

To create missing tables:

```powershell
python -m scheduler.create_tables
```

To reset the development tables and remove existing job records:

```powershell
python -m scheduler.refresh_tables
```

**Warning:** Refreshing tables deletes existing data.

## 3. Start the Scheduler Processes

Open separate terminals from the project root.

**Retry Manager**

```powershell
python -m scheduler.retry_manager_main
```

**Worker Monitor**

```powershell
python -m scheduler.monitor_main
```

**Worker**

```powershell
python -m worker.main --worker-id worker-1
```

**FastAPI Server**

```powershell
python -m uvicorn scheduler.api:app --reload
```

The API is available at:

```text
http://localhost:8000
```

Interactive API documentation:

```text
http://localhost:8000/docs
```

Additional workers can currently be started in separate terminals using unique worker IDs.

---

# Future Development Roadmap

The long-term goal is to evolve this project into a more production-oriented distributed compute platform.

## 1. Complete PostgreSQL Integration

- Establish PostgreSQL as the authoritative source of job state.
- Refactor Redis messages to contain only job IDs.
- Refactor workers to retrieve and update job records through PostgreSQL.
- Persist retries, timeouts, and execution results.
- Coordinate Redis acknowledgments with durable database state transitions.
- Implement an outbox pattern to prevent jobs from being committed to PostgreSQL without being published to Redis.

## 2. Docker Compose Orchestration

Currently, application processes are launched manually through separate terminals.

The next step is to containerize the API, workers, retry manager, and monitor as independent services.

The intended workflow is:

```powershell
docker compose up --build
```

This will provide a single-command development environment and make it easier to manage application dependencies and logs.

## 3. Horizontal Worker Scaling

Support dynamically increasing the number of workers without manually launching individual processes.

For example:

```powershell
docker compose up --scale worker=5
```

The goal is to demonstrate concurrent job processing and evaluate how throughput changes as additional workers are introduced.

## 4. Automated Testing and Reliability

Develop automated unit and integration tests covering:

- Successful job execution.
- Concurrent job processing.
- Retry behavior and exponential backoff.
- Hard execution timeouts.
- Worker crashes and recovery.
- Pending message reassignment.
- Duplicate execution scenarios.
- Stale-result protection.
- PostgreSQL persistence and Redis acknowledgment behavior.

Also develop load tests to measure throughput, execution latency, and system behavior under increasing workloads.

## 5. Observability and Monitoring

Introduce tools and endpoints to make scheduler behavior measurable.

Planned functionality:

- Structured logging.
- Prometheus metrics.
- Grafana dashboards.
- Queue-depth monitoring.
- Worker utilization.
- Job execution latency.
- Retry and failure statistics.
- `/workers` endpoint.
- `/metrics` endpoint.

## 6. Kubernetes and Helm Deployment

Once the application is reliably containerized, deploy it using Kubernetes.

The intended deployment will separate the API, workers, retry manager, and monitor into independently managed workloads.

This will allow:
- Horizontal worker scaling.
- Independent service deployment.
- Container health monitoring.
- Resource allocation.
- More realistic distributed deployment experiments.

Helm will be used to manage Kubernetes deployment configuration.

## 7. Additional Features

Potential future extensions include:

- More heterogeneous job types.
- Priority-based scheduling.
- Job cancellation.
- Job history and execution metrics.
- A web dashboard for live job and worker visualization.
- WebSocket-based status updates.
- Worker failure simulation.
- ML workload integration.

---

# Project Vision

The ultimate goal is to build a fault-tolerant, horizontally scalable compute scheduler that demonstrates how distributed backend systems manage asynchronous workloads across independent workers.

Beyond basic job execution, the project aims to explore the engineering challenges involved in building reliable infrastructure: coordinating distributed processes, persisting state, recovering from failures, preventing stale results, managing execution resources, and scaling workloads.

The planned progression is to move from manually managed local processes to a Docker Compose environment, build comprehensive reliability and load-testing capabilities, introduce observability, and eventually deploy the system using Kubernetes and Helm.
