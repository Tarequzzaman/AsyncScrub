from sqlalchemy import MetaData
from sqlalchemy.engine import Engine

def reflect_schema(engine: Engine) -> MetaData:
    """
    Reflects the schema from the source database into a SQLAlchemy MetaData object.

    Args:
        engine (Engine): SQLAlchemy engine connected to the source database.

    Returns:
        MetaData: Reflected schema containing tables, columns, constraints.
    """
    metadata = MetaData()
    try:
        metadata.reflect(bind=engine, resolve_fks=True)
    except Exception as e:
        raise RuntimeError(f"❌ Failed to reflect schema: {e}")
    
    if not metadata.tables:
        raise RuntimeError("❌ No tables found in source database.")
    
    return metadata
