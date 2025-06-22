from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import create_engine

def build_sqlalchemy_uri(config: dict, async_mode: bool = False) -> str:
    db_type = config.db_type
    host = config.host
    port = config.port
    user = config.username
    password = config.password
    database = config.database

    if db_type == "postgresql":
        return f"{'postgresql+asyncpg' if async_mode else 'postgresql'}://{user}:{password}@{host}:{port}/{database}"
    elif db_type == "mysql":
        return f"{'mysql+aiomysql' if async_mode else 'mysql+pymysql'}://{user}:{password}@{host}:{port}/{database}"
    elif db_type == "mssql":
        driver = "ODBC+Driver+17+for+SQL+Server"
        return f"{'mssql+aioodbc' if async_mode else 'mssql+pyodbc'}://{user}:{password}@{host}:{port}/{database}?driver={driver}"
    else:
        raise ValueError(f"Unsupported DB type: {db_type}")


def get_sync_engine(config: dict):
    uri = build_sqlalchemy_uri(config, async_mode=False)
    return create_engine(uri)

def get_async_engine(config: dict):
    uri = build_sqlalchemy_uri(config, async_mode=True)
    return create_async_engine(uri, echo=False, future=True)
