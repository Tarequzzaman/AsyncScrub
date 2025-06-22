from sqlalchemy import MetaData
from sqlalchemy.engine import Engine

def create_schema(metadata: MetaData, sync_engine: Engine):
    """
    Create all tables in the destination DB using a real synchronous SQLAlchemy engine.
    This avoids using async features for DDL.
    """
    try:
        metadata.create_all(bind=sync_engine, checkfirst=True)
    except Exception as e:
        raise RuntimeError(f"❌ Failed to create schema: {e}")
