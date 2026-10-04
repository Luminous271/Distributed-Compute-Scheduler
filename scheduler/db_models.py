from datetime import datetime
from sqlalchemy import String, Integer, DateTime, JSON
from sqlalchemy.orm import Mapped, mapped_column

from scheduler.database import Base


class JobDB(Base) :
    __tablename__ = "jobs"

    id: Mapped[str] = mapped_column(String, primary_key=True)

    type: Mapped[str] = mapped_column(String, nullable=False)

    payload: Mapped[dict] = mapped_column(JSON, nullable=False)

    status: Mapped[str] = mapped_column(String, nullable=False)

    priority: Mapped[int] = mapped_column(
        Integer,
        default=0,
        nullable=False,
    )   
    
    worker_id: Mapped[str | None] = mapped_column(
        String,
        nullable=True,
    )

    retry_count: Mapped[int] = mapped_column(
        Integer,
        default=0,
        nullable=False,
    )

    timeout_seconds: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
    )

    result: Mapped[dict | None] = mapped_column(
        JSON,
        nullable=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
