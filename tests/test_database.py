from sqlalchemy import text
from scheduler.database import engine

def test_connection():
    with engine.connect() as connection:
        result = connection.execute(text("SELECT 1"))
        assert result.scalar() == 1

from datetime import datetime, timezone
from scheduler.database import SessionLocal
from scheduler.db_models import JobDB
from scheduler.models import JobStatus
import uuid

def test_create_job_in_db():
    db = SessionLocal()
    try:
        job_id = str(uuid.uuid4())
        job = JobDB(
            id=job_id,
            type="sleep",
            payload={"duration_ms": 1000},
            status=JobStatus.QUEUED.value,
            retry_count=0,
            timeout_seconds=3,
            created_at=datetime.now(timezone.utc),
        )

        db.add(job)
        db.commit()

        saved_job = db.get(JobDB, job_id)

        assert saved_job is not None
        assert saved_job.id == job_id
        assert saved_job.type == "sleep"
        assert saved_job.payload == {"duration_ms": 1000}
        assert saved_job.status == JobStatus.QUEUED.value
        assert saved_job.retry_count == 0
        assert saved_job.timeout_seconds == 3

    finally:
        db.query(JobDB).filter(JobDB.id == job.id).delete()
        db.commit()
        db.close()