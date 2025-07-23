# pod_agent/database.py

import os
import pandas as pd
from sqlalchemy import create_engine, text, inspect

engine = None

def initialize_database(db_url: str):
    global engine
    if engine is not None: return
    if not db_url: raise ValueError("Database URL cannot be empty.")
    try:
        engine = create_engine(db_url)
        with engine.connect() as conn:
            print("✅ Database engine created and connection successful.")
    except Exception as e:
        print(f"🚨 DATABASE CONNECTION FAILED. Error: {e}")
        engine = None
        raise e

def init_db_and_seed():
    if engine is None: raise ConnectionError("Database not initialized. Call initialize_database() first.")
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    with engine.connect() as conn:
        if 'skus' not in tables:
            conn.execute(text("CREATE TABLE skus (id SERIAL PRIMARY KEY, product_name TEXT NOT NULL UNIQUE, sku_id TEXT NOT NULL UNIQUE)"))
        if 'retailers' not in tables:
            conn.execute(text("CREATE TABLE retailers (id SERIAL PRIMARY KEY, retailer_key TEXT NOT NULL UNIQUE, retailer_name TEXT NOT NULL, division TEXT)"))
        if 'transactions' not in tables:
            conn.execute(text("CREATE TABLE transactions (trx_id TEXT PRIMARY KEY, sku_id INTEGER NOT NULL REFERENCES skus(id), retailer_id INTEGER NOT NULL REFERENCES retailers(id), status TEXT NOT NULL, quantity_changed INTEGER NOT NULL, effective_date DATE NOT NULL, log_timestamp TIMESTAMP NOT NULL, user_id TEXT NOT NULL, source TEXT NOT NULL)"))
        conn.commit()
        if conn.execute(text("SELECT COUNT(*) FROM skus")).scalar() == 0:
            initial_skus = {"18oz quaker oats": "03000001041", "12oz honey nut cheerios": "01600027526", "12oz cheerios": "01600027525", "family size oreos": "04400003327", "10-pack coke zero": "04900003075", "doritos nacho cheese 9.75oz": "02840009089", "tostitos scoops 10oz": "02840006797", "pepsi 12-pack": "01200080994", "gatorade lemon-lime 28oz": "05200033812", "tropicana orange juice 52oz": "04850000574", "starbucks frap vanilla 4-pack": "01200081321", "ben & jerrys chocolate fudge brownie": "07684010129", "haagen-dazs vanilla 14oz": "07457002100", "diGiorno rising crust pepperoni pizza": "07192100613", "tide pods 3-in-1 72ct": "03700087535", "clorox disinfecting wipes 75ct": "04460030623", "colgate total toothpaste 4.8oz": "03500052020", "kraft mac & cheese 7.25oz": "02100065883", "heinz tomato ketchup 32oz": "01300000046", "campbells chicken noodle soup": "05100001251", "barilla spaghetti 1lb": "07680850001", "yoplait strawberry yogurt 6oz": "07047000300", "philadelphia cream cheese 8oz": "02100061221", "kelloggs frosted flakes 13.5oz": "03800020108", "pampers swaddlers diapers size 1": "03700074301"}
            sku_df = pd.DataFrame(initial_skus.items(), columns=['product_name', 'sku_id'])
            sku_df.to_sql('skus', conn, if_exists='append', index=False)
        if conn.execute(text("SELECT COUNT(*) FROM retailers")).scalar() == 0:
            initial_retailers = {"walmart": {"retailer": "Walmart", "division": "National"}, "target": {"retailer": "Target", "division": "National"}, "kroger": {"retailer": "Kroger", "division": "National"}, "costco": {"retailer": "Costco", "division": "National"}, "whole foods": {"retailer": "Whole Foods", "division": "National"}, "aldi": {"retailer": "Aldi", "division": "National"}, "publix": {"retailer": "Publix", "division": "Southeast"}, "h-e-b": {"retailer": "H-E-B", "division": "Southwest"}, "safeway": {"retailer": "Safeway", "division": "West"}, "albertsons": {"retailer": "Albertsons", "division": "West"}, "wegmans": {"retailer": "Wegmans", "division": "Northeast"}, "stop & shop": {"retailer": "Stop & Shop", "division": "Northeast"}, "sprouts": {"retailer": "Sprouts", "division": "National"}, "7-eleven": {"retailer": "7-Eleven", "division": "Convenience"}}
            retailer_list = [(k, v['retailer'], v['division']) for k, v in initial_retailers.items()]
            retailer_df = pd.DataFrame(retailer_list, columns=['retailer_key', 'retailer_name', 'division'])
            retailer_df.to_sql('retailers', conn, if_exists='append', index=False)
        conn.commit()

# (The rest of the functions are unchanged but will now reliably work)
def get_master_data_from_db(table_name, key_column):
    # ... (code is the same)
def get_info_from_names(product_name: str, retailer_key: str):
    # ... (code is the same)
# ... etc for all other functions
