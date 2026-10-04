from scheduler.database import engine, Base
from scheduler.db_models import JobDB

def refresh_tables():
    print("Refreshing database tables...")

    # Drop existing tables
    Base.metadata.drop_all(bind=engine)
    print("Existing tables dropped.")

    # Recreate tables from current SQLAlchemy models
    Base.metadata.create_all(bind=engine)
    print("Tables recreated.")


if __name__ == "__main__":
    refresh_tables()