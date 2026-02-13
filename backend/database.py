import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# Fallback to local SQLite provides a zero-config dev environment
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./dev.db")

# check_same_thread=False is required for SQLite when used with FastAPI's async handlers
if "sqlite" in DATABASE_URL:
    engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
else:
    engine = create_engine(DATABASE_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    """Dependency for FastAPI endpoints to manage DB session lifecycle"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()