# IDEA


# Helpful commands (right now application runs from multiple different terminals)
- python -m scheduler.retry_manager_main      
- python -m scheduler.monitor_main
- python -m worker.main --worker-id worker-1
- python -m uvicorn scheduler.api:app --reload
- python -m scheduler.refresh_tables



Distributed Compute Scheduler

A fault-tolerant distributed compute platform for scheduling heterogeneous workloads across dynamically scaled workers.

# MOTIVATION

Modern cloud platforms execute large numbers of computational jobs across distributed worker nodes. This project implements a simplified batch-computing system to explore scheduling, fault tolerance, workload distribution, and horizontal scaling.


# ARCHITECTURE

### System Overview

```text
                         ┌──────────────────┐
                         │      Client      │
                         │  REST / Web UI   │
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
                                  ▼
                    ┌─────────────────────────┐
                    │          Redis          │
                    │       Job Queue         │
                    │      Redis Streams      │
                    └────────────┬────────────┘
                                 │
                    ┌────────────┼────────────┐
                    │            │            │
                    ▼            ▼            ▼
              ┌──────────┐ ┌──────────┐ ┌──────────┐
              │ Worker 1 │ │ Worker 2 │ │ Worker N │
              └────┬─────┘ └────┬─────┘ └────┬─────┘
                   │             │             │
                   └─────────────┼─────────────┘
                                 ▼
                         ┌────────────────┐
                         │ Job Execution  │
                         │    Engine      │
                         └────────────────┘
```
### Job Models

The scheduler separates job submission from the internal representation of
a job with a job-request model.

#### `JobRequest`

`JobRequest` represents the data provided by a client when submitting a new
job. It contains only the information necessary to create the job:

- `type` — type of computation to perform
- `payload` — parameters required by the computation
- `priority` — scheduling priority from 0–10
- `timeout_seconds` — how many seconds the user wants the job to run for 

Fields such as job ID, timestamps, worker assignment, and execution results
are intentionally excluded because these values are generated or managed by
the scheduler. This is what the client can use to request jobs.

#### `Job`

`Job` represents the complete lifecycle of a job within the scheduler.

In addition to the request data, it contains system-managed information such
as:

- `id` — unique identifier assigned by the scheduler
- `status` — current execution state
- `created_at` — time the job was submitted
- `started_at` — time execution began
- `completed_at` — time execution finished
- `worker_id` — worker currently responsible for the job
- `retry_count` — number of execution attempts
- `result` — output produced by the job
- `retry_at` — what time a worker attempted a retry
- `max_retries` — maximum amount of retries
- `retry_count` — how many retries have already happened
- `message_id` — keep track of redis message as well. Links 2 identity systems. Acking requires the message id

The `Job` model therefore acts as the scheduler's internal representation
of a job and provides the information needed to track its execution.

#### `JobStatus`

`JobStatus` is an enumeration defining the valid states in a job's lifecycle:

```text
QUEUED → RUNNING → COMPLETED or TIMED_OUT
            │
            ▼
          FAILED
            │
            ▼
         RETRYING
            │
            ▼
          QUEUED
```
### SOME EXTRA DOCUMENTATION
- I keep a record of the jobs separate from the redis stream so we can see the state of the jobs. 
```text
Redis Stream
    │
    └── Has this work message been acknowledged?
                │
                ▼
             XACK

Job Record
    │
    └── What happened to the actual job?
                │
                ▼
       QUEUED/RUNNING/COMPLETED/FAILED
```