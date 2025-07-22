# pod_agent/database.py

import os
import sqlite3
import pandas as pd
from datetime import date

DATABASE_FILE = "pods_ledger.db"

def init_db_and_seed():
    initial_skus = {
        # Original SKUs
        "18oz quaker oats": "03000001041", "12oz honey nut cheerios": "01600027526", "12oz cheerios": "01600027525",
        "family size oreos": "04400003327", "10-pack coke zero": "04900003075",
        # New SKUs
        "doritos nacho cheese 9.75oz": "02840009089", "tostitos scoops 10oz": "02840006797",
        "pepsi 12-pack": "01200080994", "gatorade lemon-lime 28oz": "05200033812",
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
        "sprouts": {"retailer": "Sprouts", "division": "National"},
        # MODIFIED: Removed the extra comma from this line
        "7-eleven": {"retailer": "7-Eleven", "division": "Convenience"},
    }

    conn = sqlite3.connect(DATABASE_FILE); cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS transactions ( trx_id TEXT PRIMARY KEY, sku TEXT NOT NULL, product_name TEXT NOT NULL, retailer TEXT NOT NULL, division TEXT, status TEXT NOT NULL, quantity_changed INTEGER NOT NULL, effective_date TEXT NOT NULL, log_timestamp TEXT NOT NULL, user_id TEXT NOT NULL, source TEXT NOT NULL )")
    cursor.execute("CREATE TABLE IF NOT EXISTS skus (product_name TEXT PRIMARY KEY, sku_id TEXT NOT NULL)")
    cursor.execute("CREATE TABLE IF NOT EXISTS retailers (retailer_key TEXT PRIMARY KEY, retailer_name TEXT NOT NULL, division TEXT )")
    try: cursor.execute("ALTER TABLE transactions ADD COLUMN user_id TEXT"); cursor.execute("ALTER TABLE transactions ADD COLUMN source TEXT");
    except sqlite3.OperationalError: pass
    cursor.execute("SELECT COUNT(*) FROM skus");
    if cursor.fetchone()[0] == 0: print("🌱 Seeding SKUs master data..."); sku_data = [(name, sku_id) for name, sku_id in initial_skus.items()]; cursor.executemany("INSERT INTO skus (product_name, sku_id) VALUES (?, ?)", sku_data)
    cursor.execute("SELECT COUNT(*) FROM retailers")
    if cursor.fetchone()[0] == 0: print("🌱 Seeding Retailers master data..."); retailer_data = [(key, data['retailer'], data['division']) for key, data in initial_retailers.items()]; cursor.executemany("INSERT INTO retailers (retailer_key, retailer_name, division) VALUES (?, ?, ?)", retailer_data)
    conn.commit(); conn.close()

# The rest of database.py is unchanged...
def get_master_data_from_db(table_name, key_column):
    conn = sqlite3.connect(DATABASE_FILE); cursor = conn.cursor(); cursor.execute(f"SELECT {key_column} FROM {table_name}"); items = [item[0] for item in cursor.fetchall()]; conn.close(); return items
def check_for_duplicate(transaction_data):
    conn = sqlite3.connect(DATABASE_FILE); cursor = conn.cursor()
    sql = "SELECT COUNT(*) FROM transactions WHERE product_name = ? AND retailer = ? AND quantity_changed = ? AND effective_date = ?"
    data = (transaction_data['product_name'], transaction_data['retailer'], transaction_data['quantity_changed'], transaction_data['effective_date'])
    cursor.execute(sql, data); exists = cursor.fetchone()[0] > 0; conn.close(); return exists
def insert_transaction(transaction_data):
    conn = sqlite3.connect(DATABASE_FILE); cursor = conn.cursor()
    sql = "INSERT INTO transactions (trx_id, sku, product_name, retailer, division, status, quantity_changed, effective_date, log_timestamp, user_id, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    cursor.execute(sql, tuple(transaction_data.values())); conn.commit(); conn.close()
def get_all_transactions_as_dataframe():
    if not os.path.isfile(DATABASE_FILE) or os.path.getsize(DATABASE_FILE) == 0: return None
    conn = sqlite3.connect(DATABASE_FILE)
    try: df = pd.read_sql_query("SELECT * FROM transactions", conn)
    except pd.io.sql.DatabaseError: df = None
    conn.close(); return df
def get_recent_transactions(limit=100):
    if not os.path.isfile(DATABASE_FILE) or os.path.getsize(DATABASE_FILE) == 0: return None
    conn = sqlite3.connect(DATABASE_FILE)
    try:
        query = """
            SELECT log_timestamp, effective_date, product_name, retailer, quantity_changed, status, user_id, source
            FROM transactions
            ORDER BY log_timestamp DESC
            LIMIT ?
        """
        df = pd.read_sql_query(query, conn, params=(limit,))
    except pd.io.sql.DatabaseError: df = None
    finally: conn.close()
    return df
def get_current_total_for_item(product_name, retailer_name):
    conn = sqlite3.connect(DATABASE_FILE); cursor = conn.cursor()
    today_str = date.today().strftime("%Y-%m-%d")
    sql = """
        SELECT SUM(quantity_changed) FROM transactions
        WHERE product_name = ? AND retailer = ? AND effective_date <= ?
    """
    cursor.execute(sql, (product_name, retailer_name, today_str)); result = cursor.fetchone()[0]; conn.close()
    return result if result is not None else 0
