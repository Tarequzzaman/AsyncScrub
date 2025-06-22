from dataclasses import dataclass

@dataclass
class DBConfig:
    db_type: str
    host: str
    port: int
    username: str
    password: str
    database: str

def prompt_db_config(role: str) -> DBConfig:
    """
    Prompt the user for DB connection info for either 'source' or 'destination'.
    """
    print(f"\n🔧 Enter {role.upper()} database configuration:")
    
    db_type = input("DB Type (postgresql /mysql /mssql): ").strip()
    while db_type not in ["postgresql", "mysql", "mssql"]:
        print("❌ Invalid DB type. Choose from: postgresql, mysql, mssql.")
        db_type = input("DB Type (postgresql/mysql/mssql): ").strip()
    
    host = input("Host [localhost]: ").strip() or "localhost"

    default_ports = {"postgresql": 5432, "mysql": 3306, "mssql": 1433}
    port_input = input(f"Port [{default_ports.get(db_type, 5432)}]: ").strip()
    port = int(port_input) if port_input else default_ports.get(db_type, 5432)

    username = input("Username: ").strip()
    from getpass import getpass
    password = getpass("Password: ")

    database = input("Database name: ").strip()

    return DBConfig(
        db_type=db_type,
        host=host,
        port=port,
        username=username,
        password=password,
        database=database
    )
