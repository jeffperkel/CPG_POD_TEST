# pod_agent/logic.py

import os
from dotenv import load_dotenv
import json
import time
from openai import OpenAI
import pandas as pd
from datetime import datetime, date
from thefuzz import process
from . import database

load_dotenv()
client = OpenAI()
FUZZY_MATCH_THRESHOLD = 80

def validate_and_enrich_data(parsed_data, user_id, source):
    """Validates a single transaction record."""
    if database.engine is None:
        raise ConnectionError("Database is not connected.")

    product_input = parsed_data.get("product_name")
    retailer_input = parsed_data.get("retailer_name")
    quantity_input = parsed_data.get("quantity")
    intent_input = parsed_data.get("status")
    date_input = parsed_data.get("effective_date")

    if not all([product_input, retailer_input, quantity_input, intent_input, date_input]):
        raise ValueError("Missing one or more required fields.")

    valid_retailer_names = database.get_master_data_from_db('retailers', 'retailer_name')
    matched_retailer_name = find_best_match(retailer_input, valid_retailer_names)
    if not matched_retailer_name:
        raise ValueError(f"Invalid Retailer: '{retailer_input}'.")

    all_retailers = database.get_master_data_from_db('retailers', '*')
    all_retailers_df = pd.DataFrame(all_retailers, columns=['id', 'retailer_key', 'retailer_name', 'division'])
    retailer_row = all_retailers_df[all_retailers_df['retailer_name'] == matched_retailer_name]
    if retailer_row.empty:
        raise ValueError(f"Could not map retailer name '{matched_retailer_name}' to a key.")
    matched_retailer_key = retailer_row.iloc[0]['retailer_key']

    valid_skus = database.get_master_data_from_db('skus', 'product_name')
    matched_product_name = find_best_match(product_input, valid_skus)
    if not matched_product_name:
        raise ValueError(f"Invalid Product: '{product_input}'.")

    db_info = database.get_info_from_names(matched_product_name, matched_retailer_key)
    if not db_info:
        raise ValueError("Could not retrieve IDs for product/retailer combination.")
    
    validated_date = pd.to_datetime(date_input).date()
    intent = str(intent_input).lower()
    quantity = int(quantity_input)
    
    if intent == 'planned':
        quantity_changed = abs(quantity)
        final_status = 'planned' if validated_date > date.today() else 'live'
    elif intent == 'lost':
        quantity_changed = -abs(quantity)
        final_status = 'lost'
    else:
        raise ValueError(f"Invalid status: '{intent}'. Must be 'planned' or 'lost'.")
    
    trx_id = f"{db_info['sku_id']}-{db_info['retailer_id']}-{time.time()}"
    return {
        "trx_id": trx_id, "sku_id": db_info['sku_id'], "retailer_id": db_info['retailer_id'],
        "product_name": matched_product_name, "retailer_name": db_info['retailer_name'],
        "status": final_status, "quantity_changed": quantity_changed,
        "effective_date": validated_date.strftime("%Y-%m-%d"),
        "log_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "user_id": user_id, "source": source
    }

def process_new_transaction(validated_data):
    """Processes a single validated transaction."""
    if database.engine is None:
        raise ConnectionError("Database is not connected.")
        
    if validated_data['quantity_changed'] < 0:
        total = database.get_total_for_item_by_date(
            validated_data['sku_id'], validated_data['retailer_id'], validated_data['effective_date']
        )
        if abs(validated_data['quantity_changed']) > total:
            raise ValueError(f"Cannot lose more PODs than exist. Projected total is {total}.")
            
    if database.check_for_duplicate(validated_data):
        raise ValueError("Duplicate transaction detected.")
    
    database.insert_transaction(validated_data)

def process_bulk_file(file_stream, user_id):
    """Processes a bulk CSV file efficiently."""
    if database.engine is None:
        raise ConnectionError("Database is not connected.")

    try:
        bulk_df = pd.read_csv(file_stream)
        bulk_df.columns = [x.lower().strip() for x in bulk_df.columns]
    except Exception as e:
        raise ValueError(f"Could not parse CSV file: {e}")

    all_skus = pd.read_sql("SELECT id, product_name FROM skus", database.engine)
    all_retailers = pd.read_sql("SELECT id, retailer_name FROM retailers", database.engine)
    sku_lookup = {name.lower(): id for name, id in zip(all_skus['product_name'], all_skus['id'])}
    retailer_lookup = {name.lower(): id for name, id in zip(all_retailers['retailer_name'], all_retailers['id'])}

    enriched_transactions, errors = [], []
    for index, row in bulk_df.iterrows():
        try:
            # Similar validation logic as the single transaction, but using the efficient lookups
            product_input = str(row['product_name']).lower()
            retailer_input = str(row['retailer_name']).lower()

            matched_prod, _ = process.extractOne(product_input, sku_lookup.keys())
            matched_ret, _ = process.extractOne(retailer_input, retailer_lookup.keys())

            validated_date = pd.to_datetime(row['effective_date']).date()
            intent = str(row['status']).lower()
            quantity = int(row['quantity'])

            if intent == 'planned':
                quantity_changed = abs(quantity)
                status = 'planned' if validated_date > date.today() else 'live'
            elif intent == 'lost':
                quantity_changed = -abs(quantity)
                status = 'lost'
            else:
                raise ValueError(f"Invalid status: '{intent}'")
            
            sku_id = sku_lookup[matched_prod]
            retailer_id = retailer_lookup[matched_ret]
            trx_id = f"{sku_id}-{retailer_id}-{time.time()}"
            
            enriched_transactions.append({
                "trx_id": trx_id, "sku_id": sku_id, "retailer_id": retailer_id,
                "product_name": matched_prod.title(), "retailer_name": matched_ret.title(),
                "status": status, "quantity_changed": quantity_changed,
                "effective_date": validated_date.strftime("%Y-%m-%d"),
                "log_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "user_id": user_id, "source": "bulk_upload"
            })
        except Exception as e:
            errors.append(f"Row {index + 2}: {e}")

    if not enriched_transactions: return 0, errors

    # Intra-file consistency check for losses
    enriched_transactions.sort(key=lambda x: (x['effective_date'], x['log_timestamp']))
    temp_state, final_transactions = {}, []
    for trx in enriched_transactions:
        key = (trx['sku_id'], trx['retailer_id'])
        db_total = database.get_total_for_item_by_date(trx['sku_id'], trx['retailer_id'], trx['effective_date'])
        file_total = temp_state.get(key, 0)
        
        if trx['quantity_changed'] < 0 and abs(trx['quantity_changed']) > (db_total + file_total):
            errors.append(f"Row for {trx['product_name']}: Trying to lose {abs(trx['quantity_changed'])}, but projected total is only {db_total + file_total}.")
            continue
        
        temp_state[key] = file_total + trx['quantity_changed']
        final_transactions.append(trx)

    # Final DB insertion in a single transaction
    if final_transactions:
        with database.engine.connect() as conn:
            with conn.begin() as transaction:
                try:
                    for trx_data in final_transactions:
                        database.insert_transaction(trx_data, conn=conn)
                except Exception as e:
                    transaction.rollback()
                    errors.append(f"Database batch insert failed: {e}")
                    return 0, errors
    return len(final_transactions), errors


def generate_query_plan(user_query):
    db_schema_info = {"columns": ["retailer", "product_name", "division", "status", "effective_date"]}
    system_prompt = f"You are a data query planner. Translate a question into JSON with 'filters', 'group_by', and 'include_future_dates' keys. Columns from: {json.dumps(db_schema_info)}. Set `include_future_dates` to `true` for future reporting, `false` for current state."
    response = client.chat.completions.create(model="gpt-4o", messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_query}], response_format={"type": "json_object"}, temperature=0)
    return json.loads(response.choices[0].message.content)

def execute_query_plan(query_plan, include_future_dates_explicit: bool):
    df = database.get_all_transactions_as_dataframe()
    if df is None or df.empty:
        return pd.DataFrame()
    df['effective_date'] = pd.to_datetime(df['effective_date']).dt.date
    results_df = df.copy()
    if not include_future_dates_explicit:
        results_df = results_df[results_df['effective_date'] <= date.today()]
    
    filters = query_plan.get("filters", {})
    for column, value in filters.items():
        if column in results_df.columns and value:
            results_df = results_df[results_df[column].astype(str).str.contains(str(value), case=False, na=False)]
    
    group_by_cols = query_plan.get("group_by", [])
    if group_by_cols:
        valid_cols = [c for c in group_by_cols if c in results_df.columns]
        if not valid_cols:
            return pd.DataFrame({'value': [results_df['quantity_changed'].sum()]})
        return results_df.groupby(valid_cols)['quantity_changed'].sum().reset_index().rename(columns={'quantity_changed': 'value'})
    else:
        return pd.DataFrame({'value': [results_df['quantity_changed'].sum()]})

def get_export_data_for_both_views():
    plan = {"group_by": ["product_name", "retailer"]}
    current_data = execute_query_plan(plan, include_future_dates_explicit=False)
    future_data = execute_query_plan(plan, include_future_dates_explicit=True)
    return _process_for_export(current_data), _process_for_export(future_data)

def _process_for_export(data_df):
    if data_df is None or data_df.empty or 'value' not in data_df.columns:
        return pd.DataFrame()
    try:
        pivot = data_df.pivot(index='product_name', columns='retailer', values='value').fillna(0).astype(int)
        if not pivot.empty:
            pivot['Grand Total'] = pivot.sum(axis=1)
            pivot.loc['Grand Total'] = pivot.sum(axis=0)
        return pivot
    except Exception:
        return data_df # Return unpivoted if pivot fails

def generate_conversational_response(user_query):
    # This function uses the LLM to generate a plan and then data to answer a question.
    query_plan = generate_query_plan(user_query)
    # The logic here would be more complex, involving executing the plan and formatting the results.
    # For now, we'll keep it simple and just show the plan.
    # In a real scenario, you'd call execute_query_plan and then feed the results to another LLM prompt.
    
    data_df = database.get_all_transactions_as_dataframe()
    if data_df is None or data_df.empty:
        return "The database is empty. I have no data to answer your question."

    # A more sophisticated version would use the query plan to get specific data
    # For now, we use a simpler, direct calculation for the conversational response
    
    today = date.today()
    data_df['effective_date'] = pd.to_datetime(data_df['effective_date']).dt.date
    
    current_pods = data_df[data_df['effective_date'] <= today]['quantity_changed'].sum()
    future_changes = data_df[data_df['effective_date'] > today]
    future_net_change = future_changes['quantity_changed'].sum()
    future_total = current_pods + future_net_change

    future_summary_lines = []
    if not future_changes.empty:
        for _, row in future_changes.sort_values('effective_date').head(5).iterrows():
            change = "gain" if row['quantity_changed'] > 0 else "loss"
            future_summary_lines.append(f"- A {change} of {abs(row['quantity_changed'])} for {row['product_name']} at {row['retailer']} on {row['effective_date']}")
    
    context = f"""
    Current total PODs as of today ({today.strftime('%Y-%m-%d')}): {current_pods:,}
    Net change from future-dated transactions: {future_net_change:+,}
    Projected future total PODs: {future_total:,}
    Key upcoming changes:
    {''.join(future_summary_lines) if future_summary_lines else "- None in the near future."}
    """
    
    system_prompt = f"You are a helpful CPG analyst. Based on the data below, answer the user's question concisely. \n\nDATA CONTEXT:\n{context}"
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_query}
        ],
        temperature=0.1
    )
    return response.choices[0].message.content
