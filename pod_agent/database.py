# pod_agent/database.py

import os
import streamlit as st # Import Streamlit to access secrets
import pandas as pd
from sqlalchemy import create_engine, text, inspect

# --- NEW: Use SQLAlchemy to create a connection to your cloud PostgreSQL DB ---
# It fetches the connection string from Streamlit's secrets manager.
try:
    DB_URL = st.secrets["DB_CONNECTION_STRING"]
    engine = create_engine(DB_URL)
except Exception as e:
    # This will show a clear error on the Streamlit page if secrets are not set
    st.error("🚨 Database connection string not found. Please set `DB_CONNECTION_STRING` in your Streamlit secrets.")
    # Set engine to None to prevent further errors
    engine = None

def init_db_and_seed():
    if engine is None:
        print("❌ Database engine is not initialized. Skipping DB setup.")
        return

    # The inspector checks the database for existing tables
    inspector = inspect(engine)
    tables = inspector.get_table_names()

    # Use a connection from the engine
    with engine.connect() as conn:
        # PostgreSQL uses SERIAL for auto-incrementing primary keys
        if 'skus' not in tables:
            print("🔧 Creating 'skus' table...")
            conn.execute(text("""
                CREATE TABLE skus (
                    id SERIAL PRIMARY KEY,
                    product_name TEXT NOT NULL UNIQUE,
                    sku_id TEXT NOT NULL UNIQUE
                )
            """))
            conn.commit()

        if 'retailers' not in tables:
            print("🔧 Creating 'retailers' table...")
            conn.execute(text("""
                CREATE TABLE retailers (
                    id SERIAL PRIMARY KEY,
                    retailer_key TEXT NOT NULL UNIQUE,
                    retailer_name TEXT NOT NULL,
                    division TEXT
                )
            """))
            conn.commit()
            
        if 'transactions' not in tables:
            print("🔧 Creating 'transactions' table...")
            conn.execute(text("""
                CREATE TABLE transactions (
                    trx_id TEXT PRIMARY KEY,
                    sku_id INTEGER NOT NULL REFERENCES skus(id),
                    retailer_id INTEGER NOT NULL REFERENCES retailers(id),
                    status TEXT NOT NULL,
                    quantity_changed INTEGER NOT NULL,
                    effective_date DATE NOT NULL,
                    log_timestamp TIMESTAMP NOT NULL,
                    user_id TEXT NOT NULL,
                    source TEXT NOT NULL
                )
            """))
            conn.commit()

        # Seeding data (only if tables are empty)
        if conn.execute(text("SELECT COUNT(*) FROM skus")).scalar() == 0:
            print("🌱 Seeding SKUs master data...")
            initial_skus = {
                # (Same initial_skus dictionary as before)
            }
            sku_df = pd.DataFrame(initial_skus.items(), columns=['product_name', 'sku_id'])
            sku_df.to_sql('skus', conn, if_exists='append', index=False)
            
        if conn.execute(text("SELECT COUNT(*) FROM retailers")).scalar() == 0:
            print("🌱 Seeding Retailers master data...")
            initial_retailers = {
                # (Same initial_retailers dictionary as before)
            }
            retailer_list = [(k, v['retailer'], v['division']) for k, v in initial_retailers.items()]
            retailer_df = pd.DataFrame(retailer_list, columns=['retailer_key', 'retailer_name', 'division'])
            retailer_df.to_sql('retailers', conn, if_exists='append', index=False)

# --- All functions below are refactored to use the SQLAlchemy engine ---

def get_master_data_from_db(table_name, key_column):
    if engine is None: return []
    with engine.connect() as conn:
        query = text(f"SELECT {key_column} FROM {table_name}")
        result = conn.execute(query).fetchall()
        return [item[0] for item in result]

def get_info_from_names(product_name: str, retailer_key: str):
    if engine is None: return None
    with engine.connect() as conn:
        sku_query = text("SELECT id FROM skus WHERE product_name = :p_name")
        sku_res = conn.execute(sku_query, {"p_name": product_name}).fetchone()
        if not sku_res: return None
        
        retailer_query = text("SELECT id, retailer_name, division FROM retailers WHERE retailer_key = :r_key")
        retailer_res = conn.execute(retailer_query, {"r_key": retailer_key}).fetchone()
        if not retailer_res: return None
        
        return {"sku_id": sku_res[0], "retailer_id": retailer_res[0], "retailer_name": retailer_res[1], "division": retailer_res[2]}

def check_for_duplicate(transaction_data):
    if engine is None: return False
    with engine.connect() as conn:
        sql = text("""
            SELECT COUNT(*) FROM transactions 
            WHERE sku_id = :sku_id AND retailer_id = :retailer_id 
            AND quantity_changed = :qty AND effective_date = :eff_date
        """)
        params = {
            "sku_id": transaction_data['sku_id'], "retailer_id": transaction_data['retailer_id'],
            "qty": transaction_data['quantity_changed'], "eff_date": transaction_data['effective_date']
        }
        count = conn.execute(sql, params).scalar()
        return count > 0

def insert_transaction(transaction_data):
    if engine is None: raise ConnectionError("Database not connected")
    with engine.connect() as conn:
        sql = text("""
            INSERT INTO transactions (trx_id, sku_id, retailer_id, status, quantity_changed, effective_date, log_timestamp, user_id, source) 
            VALUES (:trx_id, :sku_id, :retailer_id, :status, :qty, :eff_date, :log_ts, :user, :src)
        """)
        params = {
            "trx_id": transaction_data['trx_id'], "sku_id": transaction_data['sku_id'], "retailer_id": transaction_data['retailer_id'],
            "status": transaction_data['status'], "qty": transaction_data['quantity_changed'], "eff_date": transaction_data['effective_date'],
            "log_ts": transaction_data['log_timestamp'], "user": transaction_data['user_id'], "src": transaction_data['source']
        }
        conn.execute(sql, params)
        conn.commit()

def get_all_transactions_as_dataframe():
    if engine is None: return pd.DataFrame()
    query = """
        SELECT
            t.trx_id, s.product_name, r.retailer_name as retailer, r.division,
            t.status, t.quantity_changed, t.effective_date, t.log_timestamp,
            t.user_id, t.source
        FROM transactions t
        JOIN skus s ON t.sku_id = s.id
        JOIN retailers r ON t.retailer_id = r.id
    """
    with engine.connect() as conn:
        return pd.read_sql_query(sql=query, con=conn)

# (The remaining functions `get_recent_transactions` and `get_total_for_item_by_date` would be refactored similarly)
