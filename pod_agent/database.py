# pod_agent/database.py

import os
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import OperationalError
import toml

# --- ROBUST DATABASE CONNECTION WITH FALLBACK & DEBUG LOGGING ---
DB_URL = None
engine = None

print("--- [DB] Initializing Database Connection ---")

# Method 1: Try environment variables (standard for most deployments)
DB_URL = os.environ.get("DB_CONNECTION_STRING")
if DB_URL:
    print("--- [DB] Found DB_CONNECTION_STRING in environment variables.")

# Method 2: Fallback for Streamlit Cloud subprocess - read secrets.toml
else:
    print("--- [DB] DB_CONNECTION_STRING not in env. Looking for secrets.toml.")
    try:
        # Get the path of the currently running script (database.py)
        current_script_path = os.path.abspath(__file__)
        # Navigate up to the project root directory (from /pod_agent/database.py to /)
        project_root = os.path.dirname(os.path.dirname(current_script_path))
        # Construct the full path to secrets.toml
        secrets_path = os.path.join(project_root, ".streamlit", "secrets.toml")
        
        print(f"--- [DB] Constructed secrets path: {secrets_path}")

        if os.path.exists(secrets_path):
            print("--- [DB] secrets.toml file found.")
            secrets = toml.load(secrets_path)
            DB_URL = secrets.get("DB_CONNECTION_STRING")
            if DB_URL:
                print("--- [DB] ✅ Successfully loaded DB_CONNECTION_STRING from secrets.toml.")
            else:
                print("--- [DB] 🚨 Found secrets.toml, but 'DB_CONNECTION_STRING' key is missing inside.")
        else:
            print(f"--- [DB] 🚨 Fallback failed: secrets.toml not found at path: {secrets_path}")
    except Exception as e:
        print(f"--- [DB] 🚨 An error occurred while trying to read secrets.toml: {e}")

# Attempt to connect using the DB_URL if found
if DB_URL:
    try:
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            print("--- [DB] ✅ Database engine created and connection successful.")
    except Exception as e:
        print(f"--- [DB] 🚨 DATABASE CONNECTION FAILED. The URL may be invalid or the DB is down.")
        print(f"   Error: {e}")
        engine = None
else:
    print("--- [DB] 🚨 CRITICAL: No DB_CONNECTION_STRING found. Database is not connected.")

# --- The rest of the file is unchanged ---

def init_db_and_seed():
    if engine is None: print("❌ [DB] Engine not initialized. Skipping DB setup."); return
    # ... (rest of the function is unchanged)
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    with engine.connect() as conn:
        if 'skus' not in tables:
            print("🔧 Creating 'skus' table...")
            conn.execute(text("CREATE TABLE skus (id SERIAL PRIMARY KEY, product_name TEXT NOT NULL UNIQUE, sku_id TEXT NOT NULL UNIQUE)"))
            conn.commit()
        if 'retailers' not in tables:
            print("🔧 Creating 'retailers' table...")
            conn.execute(text("CREATE TABLE retailers (id SERIAL PRIMARY KEY, retailer_key TEXT NOT NULL UNIQUE, retailer_name TEXT NOT NULL, division TEXT)"))
            conn.commit()
        if 'transactions' not in tables:
            print("🔧 Creating 'transactions' table...")
            conn.execute(text("CREATE TABLE transactions (trx_id TEXT PRIMARY KEY, sku_id INTEGER NOT NULL REFERENCES skus(id), retailer_id INTEGER NOT NULL REFERENCES retailers(id), status TEXT NOT NULL, quantity_changed INTEGER NOT NULL, effective_date DATE NOT NULL, log_timestamp TIMESTAMP NOT NULL, user_id TEXT NOT NULL, source TEXT NOT NULL)"))
            conn.commit()
        if conn.execute(text("SELECT COUNT(*) FROM skus")).scalar() == 0:
            print("🌱 Seeding SKUs master data...")
            initial_skus = {"18oz quaker oats": "03000001041", "12oz honey nut cheerios": "01600027526", "12oz cheerios": "01600027525", "family size oreos": "04400003327", "10-pack coke zero": "04900003075", "doritos nacho cheese 9.75oz": "02840009089", "t
