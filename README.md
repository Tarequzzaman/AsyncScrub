# AsyncScrub (🧹 Data Scrubber CLI)

This tool connects to a **source database**, reflects its schema (including foreign key relationships), and populates a **destination database** with randomly generated fake data while maintaining referential integrity.

---

### 🔧 How It Works

1. Prompts user for source and destination DB configurations.
2. Reflects source schema using SQLAlchemy.
3. Recreates schema in destination (DDL only).
4. Populates destination tables with fake data, preserving foreign key constraints.

## ✅ Prerequisites

- Python 3.10 or later
- Install required dependencies:

```bash
pip install -r requirements.txt
```

Make sure your source and destination databases are up and accessible (supports `PostgreSQL`, `MySQL`, `MSSQL`).

## 🚀 How to Run
Run the CLI tool:
```
python main.py
```


## Task remaining 

- handle composite key 
- handle unique key
- Integration of Mysql and Mssql