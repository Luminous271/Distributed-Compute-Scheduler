from sqlalchemy import text
from scheduler.database import engine


def test_connection():
    with engine.connect() as connection:
        result = connection.execute(text("SELECT 1"))
        assert result.scalar() == 1