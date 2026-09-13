from fastapi import FastAPI
from datetime import datetime, timezone
from uuid import uuid4
from scheduler.models import Job
from scheduler.models import JobRequest
from job_queue.redis_queue import RedisQueue

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
    # save a job object to handel job lifecycle
    queue.save_job(job)
    # enqueue the job id into the stream
    queue.enqueue(job)

    return job


from scheduler.database import engine
from scheduler.db_models import JobDB



@app.get("/health") 
def health_check() :
    return {"status": "healthy"}

