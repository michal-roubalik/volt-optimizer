import sys
import os
import pytest

# --- CRITICAL FIX ---
# We set the environment variable BEFORE importing main.py or database.py.
# This forces the app to use a local SQLite file instead of looking for "db".
os.environ["DATABASE_URL"] = "sqlite:///./test.db"

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from fastapi.testclient import TestClient

# Import your app components after setting the env var
from database import Base, get_db
from main import app

# Setup a test-specific engine (SQLite)
# connect_args={"check_same_thread": False} is needed for SQLite + FastAPI
engine = create_engine(
    "sqlite:///./test.db", 
    connect_args={"check_same_thread": False}, 
    poolclass=StaticPool
)

TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@pytest.fixture(scope="function")
def db_session():
    """
    Creates a fresh database for each test case.
    """
    # Create tables
    Base.metadata.create_all(bind=engine)
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()
        # Drop tables after test to keep it clean
        Base.metadata.drop_all(bind=engine)

@pytest.fixture(scope="function")
def client(db_session):
    """
    Returns a TestClient that uses the override DB session.
    """
    # Override the get_db dependency in the FastAPI app
    def override_get_db():
        try:
            yield db_session
        finally:
            db_session.close()
    
    app.dependency_overrides[get_db] = override_get_db
    yield TestClient(app)
    # Clean up overrides
    app.dependency_overrides.clear()