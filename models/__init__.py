from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import MetaData, event
from sqlalchemy.engine import Engine
import sqlite3

# Create database instance
db = SQLAlchemy()

# Metadata for dynamic table creation
metadata = MetaData()

# Configure SQLite for better concurrency and performance
@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    """Configure SQLite connection for better performance and concurrency."""
    if isinstance(dbapi_connection, sqlite3.Connection):
        cursor = dbapi_connection.cursor()
        # Enable WAL mode for better concurrency
        cursor.execute("PRAGMA journal_mode=WAL")
        # Set synchronous to NORMAL for better performance
        cursor.execute("PRAGMA synchronous=NORMAL")
        # Increase timeout for locked database
        cursor.execute("PRAGMA busy_timeout=30000")
        # Store temporary tables in memory
        cursor.execute("PRAGMA temp_store=MEMORY")
        # Set memory map size for better performance
        cursor.execute("PRAGMA mmap_size=134217728")
        cursor.close()
