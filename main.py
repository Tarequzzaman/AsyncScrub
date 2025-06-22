from getpass import getpass
import asyncio
from config.db_config import DBConfig
from db.connector import get_sync_engine, get_async_engine
from db.schema_reader import reflect_schema
from db.schema_writer import create_schema
from db.data_inserter import insert_fake_data

def prompt_db_config(role: str) -> DBConfig:
    print(f"\n🔧 Enter {role.upper()} database configuration:")
    while True:
        db_type = input("DB Type (postgresql/mysql/mssql): ").strip().lower()
        if db_type in ["postgresql", "mysql", "mssql"]:
            break
        print("❌ Invalid DB type. Choose from: postgresql, mysql, mssql.")
        
    host = input("Host [localhost]: ").strip() or "localhost"

    default_ports = {"postgresql": 5432, "mysql": 3306, "mssql": 1433}
    port = input(f"Port [{default_ports.get(db_type, 5432)}]: ").strip()
    port = int(port) if port else default_ports.get(db_type, 5432)

    username = input("Username: ").strip()
    password = getpass("Password: ")
    database = input("Database name: ").strip()

    return DBConfig(db_type, host, port, username, password, database)

def main():
    print("🎛️  Data Scrubber CLI\n")

    # Source DB
    source_config = prompt_db_config("source")
    print("🔗 Connecting to source DB...")
    source_engine = get_sync_engine(source_config)
    print("✅ Connected to source database.\n")

    # Destination DB
    dest_config = prompt_db_config("destination")
    print("🔗 Connecting to destination DB...")
    dest_async_engine = get_async_engine(dest_config)
    dest_sync_engine = get_sync_engine(dest_config)
    print("✅ Connected to destination database.\n")

    # Rows per table
    num_rows = input("💬 How many fake rows per table? [100]: ").strip()
    num_rows = int(num_rows) if num_rows else 100

    # Reflect schema from source
    print("📥 Reflecting source schema...")
    metadata = reflect_schema(source_engine)

    # Create schema in destination using SYNC engine
    print("📤 Creating destination schema...")
    create_schema(metadata, dest_sync_engine)

    # Insert fake data using ASYNC engine
    print("🧪 Inserting fake data...")
    asyncio.run(insert_fake_data(metadata, dest_async_engine, num_rows))

    print("✅ Data scrubbing complete!")

if __name__ == "__main__":
    main()
