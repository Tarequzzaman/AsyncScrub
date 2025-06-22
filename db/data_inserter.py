from databases import Database
from utils.faker_generator import generate_fake_row
from utils.dependency_graph import get_insertion_order
import typer
from tqdm import tqdm

MAX_ATTEMPTS = 10


async def insert_fake_data(metadata, async_engine, num_rows: int = 100):
    """
    Insert fake data into each table using real foreign key references.
    Tracks inserted primary key values to handle foreign key dependencies.
    """
    uri = str(async_engine.url)
    db = Database(uri)
    await db.connect()

    primary_keys_data = {}  # Format: { table_name: { primary_key_name: [values] } }

    try:
        tables_in_order = get_insertion_order(metadata)

        for table in tqdm(tables_in_order):
            pk_column = next((col.name for col in table.primary_key.columns), None)
            if not pk_column:
                continue

            primary_keys_data.setdefault(table.name, {pk_column: []})

            for _ in range(num_rows):
                for attempt in range(MAX_ATTEMPTS):
                    try:
                        row = generate_fake_row(table, primary_keys_data)
                        inserted_id = await db.execute(query=table.insert().values(**row))

                        # Always fallback to manually generated PK if inserted_id is None
                        final_id = inserted_id if inserted_id is not None else row.get(pk_column)

                        if pk_column and final_id is not None:
                            primary_keys_data[table.name][pk_column].append(final_id)
                            break  # success
                    except Exception as e:
                        if attempt == MAX_ATTEMPTS - 1:
                            typer.echo(f"❌ Skipping row after {MAX_ATTEMPTS} attempts in '{table.name}': {e}")
                        continue  # retry

    except Exception as e:
        raise RuntimeError(f"❌ Unexpected error during data insertion: {e}")
    finally:
        await db.disconnect()
