# pod_agent/database.py

import os
import pandas as pd
from sqlalchemy import create_engine, text, inspect

engine = None

def initialize_database(db_url: str):
    """Initializes the database engine. This must be called once at app startup."""
    global engine
    if engine is not None:
        return
    if not db_url:
        raise ValueError("Database URL cannot be empty.")
    try:
        engine = create_engine(db_url)
        with engine.connect() as conn:
            print("✅ Database engine created and connection successful.")
    except Exception as e:
        print(f"🚨 DATABASE CONNECTION FAILED. Error: {e}")
        engine = None
        raise e

def init_db_and_seed():
    """Creates tables if they don't exist and seeds them with initial data."""
    if engine is None:
        raise ConnectionError("Database not initialized. Call initialize_database() first.")
    
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    with engine.connect() as conn:
        if 'skus' not in tables:
            conn.execute(text("CREATE TABLE skus (id SERIAL PRIMARY KEY, product_name TEXT NOT NULL UNIQUE, sku_id TEXT NOT NULL UNIQUE)"))
        if 'retailers' not in tables:
            conn.execute(text("CREATE TABLE retailers (id SERIAL PRIMARY KEY, retailer_key TEXT NOT NULL UNIQUE, retailer_name TEXT NOT NULL, division TEXT)"))
        if 'transactions' not in tables:
            conn.execute(text("CREATE TABLE transactions (trx_id TEXT PRIMARY KEY, sku_id INTEGER NOT NULL REFERENCES skus(id), retailer_id INTEGER NOT NULL REFERENCES retailers(id), status TEXT NOT NULL, quantity_changed INTEGER NOT NULL, effective_date DATE NOT NULL, log_timestamp TIMESTAMP NOT NULL, user_id TEXT NOT NULL, source TEXT NOT NULL)"))
        
        # Seeding logic
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

def get_master_data_from_db(table_name, key_column):
    if engine is None: raise ConnectionError("Database not initialized.")
    with engine.connect() as conn:
        if key_column == '*':
            return conn.execute(text(f"SELECT * FROM {table_name}")).fetchall()
        else:
            result = conn.execute(text(f"SELECT {key_column} FROM {table_name}")).fetchall()
            return [item[0] for item in result]

def get_info_from_names(product_name: str, retailer_key: str):
    if engine is None: raise ConnectionError("Database not initialized.")
    with engine.connect() as conn:
        sku_res = conn.execute(text("SELECT id FROM skus WHERE product_name = :p_name"), {"p_name": product_name}).fetchone()
        if not sku_res: return None
        retailer_res = conn.execute(text("SELECT id, retailer_name, division FROM retailers WHERE retailer_key = :r_key"), {"r_key": retailer_key}).fetchone()
        if not retailer_res: return None
        return {"sku_id": sku_res[0], "retailer_id": retailer_res[0], "retailer_name": retailer_res[1], "division": retailer_res[2]}

def check_for_duplicate(transaction_data):
    if engine is None: raise ConnectionError("Database not initialized.")
    with engine.connect() as conn:
        sql = text("SELECT COUNT(*) FROM transactions WHERE sku_id = :sku_id AND retailer_id = :retailer_id AND quantity_changed = :qty AND effective_date = :eff_date")
        params = {"sku_id": transaction_data['sku_id'], "retailer_id": transaction_data['retailer_id'], "qty": transaction_data['quantity_changed'], "eff_date": transaction_data['effective_date']}
        return conn.execute(sql, params).scalar() > 0

# in pod_agent/database.py
                
                def insert_transaction(transaction_data, conn=None):
                    def _execute(connection):
                        sql = text("INSERT INTO transactions (trx_id, sku_id, retailer_id, status, quantity_changed, effective_date, log_timestamp, user_id, source) VALUES (:trx_id, :sku_id, :retailer_id, :status, :qty, :eff_date, :log_ts, :user, :src)")
                        params = {
                            "trx_id": transaction_data['trx_id'], 
                            "sku_id": transaction_data['sku_id'], 
                            "retailer_id": transaction_data['retailer_id'], 
                            "status": transaction_data['status'], 
                            "qty": transaction_data['quantity_changed'], # <-- THIS IS THE FIXED LINE
                            "eff_date": transaction_data['effective_date'], 
                            "log_ts": transaction_data['log_timestamp'], 
                            "user": transaction_data['user_id'], 
                            "src": transaction_data['source']
                        }
                        connection.execute(sql, params)
                    
                    if conn:
                        _execute(conn)
                    else:
                        if engine is None: raise ConnectionError("Database not initialized.")
                        with engine.connect() as connection:
                            with connection.begin():
                                _execute(connection)

def get_all_transactions_as_dataframe():
    if engine is None: raise ConnectionError("Database not initialized.")
    query = text("SELECT t.trx_id, s.product_name, r.retailer_name as retailer, r.division, t.status, t.quantity_changed, t.effective_date, t.log_timestamp, t.user_id, t.source FROM transactions t JOIN skus s ON t.sku_id = s.id JOIN retailers r ON t.retailer_id = r.id")
    with engine.connect() as conn:
        return pd.read_sql_query(sql=query, con=conn)

def get_total_for_item_by_date(sku_id: int, retailer_id: int, effective_date: str):
    if engine is None: raise ConnectionError("Database not initialized.")
    with engine.connect() as conn:
        sql = text("SELECT SUM(quantity_changed) FROM transactions WHERE sku_id = :sku_id AND retailer_id = :retailer_id AND effective_date <= :eff_date")
        result = conn.execute(sql, {"sku_id": sku_id, "retailer_id": retailer_id, "eff_date": effective_date}).scalar()
        return result if result is not None else 0
