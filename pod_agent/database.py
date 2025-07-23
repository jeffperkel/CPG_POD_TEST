# pod_agent/database.py

import os
import sqlite3
import pandas as pd
from datetime import date

DATABASE_FILE = "pods_ledger.db"

def init_db_and_seed():
    # To apply these changes, you may need to delete your old pods_ledger.db file first
    if os.path.exists(DATABASE_FILE):
        print("ℹ️ Database file already exists. Schema changes will not be applied to existing file.")

    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    
    # Enable foreign key support in SQLite
    cursor.execute("PRAGMA foreign_keys = ON;")

    # --- NEW RELATIONAL SCHEMA ---
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS skus (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_name TEXT NOT NULL UNIQUE,
            sku_id TEXT NOT NULL UNIQUE
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS retailers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            retailer_key TEXT NOT NULL UNIQUE,
            retailer_name TEXT NOT NULL,
            division TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            trx_id TEXT PRIMARY KEY,
            sku_id INTEGER NOT NULL,
            retailer_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            quantity_changed INTEGER NOT NULL,
            effective_date TEXT NOT NULL,
            log_timestamp TEXT NOT NULL,
            user_id TEXT NOT NULL,
            source TEXT NOT NULL,
            FOREIGN KEY (sku_id) REFERENCES skus (id),
            FOREIGN KEY (retailer_id) REFERENCES retailers (id)
        )
    """)

    # Seeding data
    initial_skus = {
        "18oz quaker oats": "03000001041", "12oz honey nut cheerios": "01600027526", "12oz cheerios": "01600027525",
        "family size oreos": "04400003327", "10-pack coke zero": "04900003075", "doritos nacho cheese 9.75oz": "02840009089", 
        "tostitos scoops 10oz": "02840006797", "pepsi 12-pack": "01200080994", "gatorade lemon-lime 28oz": "05200033812",
        "tropicana orange juice 52oz": "04850000574", "starbucks frap vanilla 4-pack": "01200081321",
        "ben & jerrys chocolate fudge brownie": "07684010129", "haagen-dazs vanilla 14oz": "07457002100",
        "diGiorno rising crust pepperoni pizza": "07192100613", "tide pods 3-in-1 72ct": "03700087535",
        "clorox disinfecting wipes 75ct": "04460030623", "colgate total toothpaste 4.8oz": "03500052020",
        "kraft mac & cheese 7.25oz": "02100065883", "heinz tomato ketchup 32oz": "01300000046",
        "campbells chicken noodle soup": "05100001251", "barilla spaghetti 1lb": "07680850001",
        "yoplait strawberry yogurt 6oz": "07047000300", "philadelphia cream cheese 8oz": "02100061221",
        "kelloggs frosted flakes 13.5oz": "03800020108", "pampers swaddlers diapers size 1": "03700074301",
    }
    initial_retailers = {
        "walmart": {"retailer": "Walmart", "division": "National"}, "target": {"retailer": "Target", "division": "National"},
        "kroger": {"retailer": "Kroger", "division": "National"}, "costco": {"retailer": "Costco", "division": "National"},
        "whole foods": {"retailer": "Whole Foods", "division": "National"}, "aldi": {"retailer": "Aldi", "division": "National"},
        "publix": {"retailer": "Publix", "division": "Southeast"}, "h-e-b": {"retailer": "H-E-B", "division": "Southwest"},
        "safeway": {"retailer": "Safeway", "division": "West"}, "albertsons": {"retailer": "Albertsons", "division": "West"},
        "wegmans": {"retailer": "Wegmans", "division": "Northeast"}, "stop & shop": {"retailer": "Stop & Shop", "division": "Northeast"},
        "sprouts": {"retailer": "Sprouts", "division": "National"}, "7-eleven": {"retailer": "7-Eleven", "division": "Convenience"},
    }

    cursor.execute("SELECT COUNT(*) FROM skus")
    if cursor.fetchone()[0] == 0:
        print("🌱 Seeding SKUs master data...")
        sku_data = [(name, sku_id) for name, sku_id in initial_skus.items()]
        cursor.executemany("INSERT INTO skus (product_name, sku_id) VALUES (?, ?)", sku_data)

    cursor.execute("SELECT COUNT(*) FROM retailers")
    if cursor.fetchone()[0] == 0:
        print("🌱 Seeding Retailers master data...")
        retailer_data = [(key, data['retailer'], data['division']) for key, data in initial_retailers.items()]
        cursor.executemany("INSERT INTO retailers (retailer_key, retailer_name, division) VALUES (?, ?, ?)", retailer_data)

    conn.commit()
    conn.close()

def get_master_data_from_db(table_name, key_column):
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute(f"SELECT {key_column} FROM {table_name}")
    items = [item[0] for item in cursor.fetchall()]
    conn.close()
    return items

def get_info_from_names(product_name: str, retailer_key: str):
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM skus WHERE product_name = ?", (product_name,))
    sku_res = cursor.fetchone()
    if not sku_res:
        conn.close()
        return None
    
    cursor.execute("SELECT id, retailer_name, division FROM retailers WHERE retailer_key = ?", (retailer_key,))
    retailer_res = cursor.fetchone()
    conn.close()
    if not retailer_res:
        return None
        
    return {"sku_id": sku_res[0], "retailer_id": retailer_res[0], "retailer_name": retailer_res[1], "division": retailer_res[2]}

def check_for_duplicate(transaction_data):
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    sql = "SELECT COUNT(*) FROM transactions WHERE sku_id = ? AND retailer_id = ? AND quantity_changed = ? AND effective_date = ?"
    data = (transaction_data['sku_id'], transaction_data['retailer_id'], transaction_data['quantity_changed'], transaction_data['effective_date'])
    cursor.execute(sql, data)
    exists = cursor.fetchone()[0] > 0
    conn.close()
    return exists

def insert_transaction(transaction_data):
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    sql = "INSERT INTO transactions (trx_id, sku_id, retailer_id, status, quantity_changed, effective_date, log_timestamp, user_id, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    # Note: Ensure the tuple values are in the exact order of the columns in the SQL statement
    data_tuple = (
        transaction_data['trx_id'], transaction_data['sku_id'], transaction_data['retailer_id'],
        transaction_data['status'], transaction_data['quantity_changed'], transaction_data['effective_date'],
        transaction_data['log_timestamp'], transaction_data['user_id'], transaction_data['source']
    )
    cursor.execute(sql, data_tuple)
    conn.commit()
    conn.close()

def get_all_transactions_as_dataframe():
    if not os.path.isfile(DATABASE_FILE) or os.path.getsize(DATABASE_FILE) == 0:
        return None
    conn = sqlite3.connect(DATABASE_FILE)
    try:
        query = """
            SELECT
                t.trx_id,
                s.product_name,
                r.retailer_name as retailer,
                r.division,
                t.status,
                t.quantity_changed,
                t.effective_date,
                t.log_timestamp,
                t.user_id,
                t.source
            FROM transactions t
            JOIN skus s ON t.sku_id = s.id
            JOIN retailers r ON t.retailer_id = r.id
        """
        df = pd.read_sql_query(query, conn)
    except pd.io.sql.DatabaseError:
        df = None
    finally:
        conn.close()
    return df

def get_recent_transactions(limit=100):
    if not os.path.isfile(DATABASE_FILE) or os.path.getsize(DATABASE_FILE) == 0:
        return None
    conn = sqlite3.connect(DATABASE_FILE)
    try:
        query = """
            SELECT t.log_timestamp, t.effective_date, s.product_name, r.retailer_name as retailer,
                   t.quantity_changed, t.status, t.user_id, t.source
            FROM transactions t
            JOIN skus s ON t.sku_id = s.id
            JOIN retailers r ON t.retailer_id = r.id
            ORDER BY t.log_timestamp DESC
            LIMIT ?
        """
        df = pd.read_sql_query(query, conn, params=(limit,))
    except pd.io.sql.DatabaseError:
        df = None
    finally:
        conn.close()
    return df

def get_total_for_item_by_date(sku_id: int, retailer_id: int, effective_date: str):
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    sql = """
        SELECT SUM(quantity_changed) FROM transactions
        WHERE sku_id = ? AND retailer_id = ? AND effective_date <= ?
    """
    cursor.execute(sql, (sku_id, retailer_id, effective_date))
    result = cursor.fetchone()[0]
    conn.close()
    return result if result is not None else 0
