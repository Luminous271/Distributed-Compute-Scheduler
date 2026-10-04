from fastapi import FastAPI
from datetime import datetime, timezone
from uuid import uuid4
from scheduler.models import Job, JobRequest, JobStatus
from job_queue.redis_queue import RedisQueue

from scheduler.database import SessionLocal
from scheduler.db_models import JobDB

app = FastAPI(
    title = "Distributed Compute Scheduler",
    version = "0.1.0"
)

queue = RedisQueue()

# POST endpoint to add jobs
@app.post("/jobs", response_model=Job)
def create_job(request: JobRequest):
    job = Job(
        id=str(uuid4()),
        type=request.type,
        payload=request.payload,
        priority=request.priority,
        timeout_seconds=request.timeout_seconds,
        created_at=datetime.now(timezone.utc),
    )

    db = SessionLocal()
    try :
        db_job = JobDB(
                id=job.id,
                type=job.type,
                payload=job.payload,
                status=JobStatus.QUEUED.value,
                priority=job.priority,
                retry_count=0,
                timeout_seconds=job.timeout_seconds,
                created_at=job.created_at,
            )
        db.add(db_job)
        db.commit()
    except Exception :
        db.rollback()
        raise
    finally:
        db.close()

    # save a job object to handel job lifecycle
    queue.save_job(job)
    # enqueue the job id into the stream
    queue.enqueue(job)

    return job




@app.get("/health") 
def health_check() :
    return {"status": "healthy"}

