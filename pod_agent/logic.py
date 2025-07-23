# pod_agent/logic.py

import os
from dotenv import load_dotenv
import json
import time
import sqlite3
from openai import OpenAI
import pandas as pd
from datetime import datetime, date
from thefuzz import process
from . import database

load_dotenv()
client = OpenAI()
FUZZY_MATCH_THRESHOLD = 80

def classify_intent(user_input):
    command_word = user_input.lower().split()[0]
    if command_word in ['bulk_add', 'export']: return command_word
    system_prompt = "You are an intent classifier. Respond with JSON: {\"intent\": \"log_data\"} for adding/losing PODs, or {\"intent\": \"query_data\"} for questions/summaries."
    response = client.chat.completions.create(model="gpt-4o", messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_input}], response_format={"type": "json_object"}, temperature=0)
    return json.loads(response.choices[0].message.content).get("intent")

def find_best_match(query, valid_choices_list):
    if not query: return None
    best_match, score = process.extractOne(str(query).lower(), valid_choices_list)
    return best_match if score >= FUZZY_MATCH_THRESHOLD else None

def validate_and_enrich_data(parsed_data, user_id, source):
    product_input = parsed_data.get("product_name") or parsed_data.get("Product") or parsed_data.get("sku_name")
    retailer_input = parsed_data.get("retailer_name") or parsed_data.get("Retailer")
    quantity_input = parsed_data.get("quantity") or parsed_data.get("Quantity")
    intent_input = parsed_data.get("status") or parsed_data.get("Status")
    date_input = parsed_data.get("effective_date") or parsed_data.get("Date")

    if not all([product_input, retailer_input, quantity_input, intent_input]):
        raise ValueError("Missing one or more required fields.")

    valid_skus = database.get_master_data_from_db('skus', 'product_name')
    matched_product_name = find_best_match(product_input, valid_skus)
    if not matched_product_name: raise ValueError(f"Invalid Product: '{product_input}'.")

    valid_retailer_keys = database.get_master_data_from_db('retailers', 'retailer_key')
    matched_retailer_key = find_best_match(retailer_input, valid_retailer_keys)
    if not matched_retailer_key: raise ValueError(f"Invalid Retailer: '{retailer_input}'.")

    db_info = database.get_info_from_names(matched_product_name, matched_retailer_key)
    if not db_info: raise ValueError("Could not retrieve IDs for product/retailer combination.")
    
    validated_date = pd.to_datetime(date_input).date() if pd.notna(date_input) else date.today()
    intent = str(intent_input).lower()
    quantity = int(quantity_input)
    final_status = ""
    quantity_changed = 0

    if intent == 'planned':
        quantity_changed = abs(quantity)
        final_status = 'planned' if validated_date > date.today() else 'live'
    elif intent == 'lost':
        quantity_changed = -abs(quantity)
        final_status = 'lost'
    else:
        raise ValueError(f"Invalid action intent: '{intent}'. Must be 'planned' or 'lost'.")
    
    trx_id = f"{db_info['sku_id']}-{db_info['retailer_id']}-{time.time()}"

    # This dictionary now contains everything needed for insertion and for other logic functions
    return {
        "trx_id": trx_id,
        "sku_id": db_info['sku_id'],
        "retailer_id": db_info['retailer_id'],
        "product_name": matched_product_name, # Keep for logging/messages
        "retailer_name": db_info['retailer_name'], # Keep for logging/messages
        "status": final_status,
        "quantity_changed": quantity_changed,
        "effective_date": validated_date.strftime("%Y-%m-%d"),
        "log_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "user_id": user_id,
        "source": source
    }

def process_new_transaction(validated_data):
    if validated_data['quantity_changed'] < 0:
        total_on_effective_date = database.get_total_for_item_by_date(
            sku_id=validated_data['sku_id'],
            retailer_id=validated_data['retailer_id'],
            effective_date=validated_data['effective_date']
        )
        quantity_to_lose = abs(validated_data['quantity_changed'])
        if quantity_to_lose > total_on_effective_date:
            raise ValueError(
                f"Invalid transaction: Cannot lose {quantity_to_lose} PODs on {validated_data['effective_date']}. "
                f"The projected total for '{validated_data['product_name']}' "
                f"at '{validated_data['retailer_name']}' on that date will only be {total_on_effective_date}."
            )
            
    if database.check_for_duplicate(validated_data):
        raise ValueError("Duplicate transaction detected.")
    
    database.insert_transaction(validated_data)
    return True

def process_bulk_file(file_stream, user_id):
    try: bulk_df = pd.read_csv(file_stream)
    except Exception as e: raise ValueError(f"Could not parse CSV file: {e}")

    enriched_transactions = []; errors = []
    for index, row in bulk_df.iterrows():
        try:
            validated_row = validate_and_enrich_data(row.to_dict(), user_id, source="bulk_upload")
            enriched_transactions.append(validated_row)
        except Exception as e: errors.append(f"Row {index + 2}: {e}")

    if not enriched_transactions: return 0, errors

    enriched_transactions.sort(key=lambda x: (x['effective_date'], x['log_timestamp']))
    
    # Check for consistency within the file before committing to DB
    temp_state = {}; final_transactions_to_insert = []
    for trx in enriched_transactions:
        # Use robust IDs for the state key
        key = (trx['sku_id'], trx['retailer_id'])
        current_total = temp_state.get(key, 0)
        
        if trx['quantity_changed'] < 0:
            quantity_to_lose = abs(trx['quantity_changed'])
            if quantity_to_lose > current_total:
                errors.append(f"File consistency error on {trx['effective_date']} for '{trx['product_name']}': Trying to lose {quantity_to_lose}, but simulated total is only {current_total}.")
                continue
        
        temp_state[key] = current_total + trx['quantity_changed']
        final_transactions_to_insert.append(trx)
    
    if final_transactions_to_insert:
        conn = sqlite3.connect(database.DATABASE_FILE)
        cursor = conn.cursor()
        try:
            # Prepare data tuple for the new schema
            insert_data = [
                (t['trx_id'], t['sku_id'], t['retailer_id'], t['status'], t['quantity_changed'],
                 t['effective_date'], t['log_timestamp'], t['user_id'], t['source'])
                for t in final_transactions_to_insert
            ]
            sql = "INSERT INTO transactions (trx_id, sku_id, retailer_id, status, quantity_changed, effective_date, log_timestamp, user_id, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
            cursor.executemany(sql, insert_data)
            conn.commit()
        except Exception as e:
            conn.rollback()
            errors.append(f"Database batch insert failed: {e}")
            final_transactions_to_insert.clear()
        finally:
            conn.close()

    return len(final_transactions_to_insert), errors

# No changes needed from this point down as they rely on `get_all_transactions_as_dataframe` which abstracts away the db changes.
def generate_query_plan(user_query):
    db_schema_info = {"columns": ["retailer", "product_name", "division", "status", "effective_date"], "notes": "Map user synonyms: 'customer'->'retailer', 'item'->'product_name'."}
    system_prompt = f"You are a data query planner. Translate a question into JSON with 'filters', 'group_by', and 'include_future_dates' keys. Column names must be from this schema: {json.dumps(db_schema_info)}. Set `include_future_dates` to `true` for reporting/timelines, `false` for current state."
    response = client.chat.completions.create(model="gpt-4o", messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_query}], response_format={"type": "json_object"}, temperature=0)
    return json.loads(response.choices[0].message.content)

def execute_query_plan(query_plan, include_future_dates_explicit: bool):
    df = database.get_all_transactions_as_dataframe()
    
    def get_empty_multiindex_series(group_by_cols):
        names = group_by_cols if group_by_cols else ['Net PODs']
        if isinstance(names, list) and len(names) > 1:
            empty_index = pd.MultiIndex.from_tuples([], names=names)
        elif isinstance(names, list) and len(names) == 1:
            empty_index = pd.Index([], name=names[0])
        else:
            empty_index = pd.Index([], name='Dimension')
        return pd.Series(dtype=int, index=empty_index)

    if df is None or df.empty:
        return get_empty_multiindex_series(query_plan.get("group_by", []))

    df['effective_date'] = pd.to_datetime(df['effective_date'], errors='coerce')
    df.dropna(subset=['effective_date'], inplace=True)

    results_df = df.copy()

    if not include_future_dates_explicit:
        current_time = datetime.now()
        results_df = results_df[results_df['effective_date'] <= current_time].copy()

    filters = query_plan.get("filters", {})
    if filters:
        for column, value in filters.items():
            if column in results_df.columns and value:
                results_df = results_df[results_df[column].astype(str).str.contains(str(value), case=False, na=False)]

    if results_df.empty:
        return get_empty_multiindex_series(query_plan.get("group_by", []))

    group_by_cols = query_plan.get("group_by", [])
    if group_by_cols:
        missing_cols = [col for col in group_by_cols if col not in results_df.columns]
        if missing_cols:
            return get_empty_multiindex_series(group_by_cols)

        # The column name from the joined DF is `retailer`, not `retailer_name`
        final_results = results_df.groupby(group_by_cols)['quantity_changed'].sum().astype(int)
    else:
        total_sum = results_df['quantity_changed'].sum()
        final_results = pd.Series([total_sum], index=['Net PODs'], dtype=int)
    
    return final_results

def generate_conversational_response(user_query):
    query_plan = generate_query_plan(user_query)
    filters = query_plan.get("filters", {})
    filtered_retailer = filters.get("retailer")
    filtered_product = filters.get("product_name")
    
    df = database.get_all_transactions_as_dataframe()

    current_date_str = date.today().strftime("%Y-%m-%d")
    today = date.today()

    if df is None or df.empty:
        return f"As of {current_date_str}, the database is empty. There is no POD data to analyze."

    df_copy = df.copy()
    df_copy['effective_date'] = pd.to_datetime(df_copy['effective_date'], errors='coerce').dt.date
    df_copy.dropna(subset=['effective_date'], inplace=True)

    if filtered_retailer:
        df_copy = df_copy[df_copy['retailer'].str.contains(filtered_retailer, case=False, na=False)]
    if filtered_product:
        df_copy = df_copy[df_copy['product_name'].str.contains(filtered_product, case=False, na=False)]
    
    if df_copy.empty:
        return f"I couldn't find any POD data matching your query (Retailer: {filtered_retailer or 'any'}, Product: {filtered_product or 'any'}). Please check the names and try again."

    current_mask = df_copy['effective_date'] <= today
    current_df = df_copy[current_mask]
    current_net_pods = current_df['quantity_changed'].sum()

    future_mask = df_copy['effective_date'] > today
    future_df = df_copy[future_mask].copy()
    
    future_changes_list_str = ""
    future_net_change_amount = 0

    if not future_df.empty:
        future_df = future_df.sort_values(by='effective_date', ascending=True)
        future_changes_lines = []
        for _, row in future_df.iterrows():
            change_type = "Loss" if row['quantity_changed'] < 0 else "Gain"
            future_changes_lines.append(
                f"- {change_type} of {abs(row['quantity_changed']):,} {row['product_name']} at {row['retailer']} on {row['effective_date'].strftime('%Y-%m-%d')}"
            )
        future_changes_list_str = "\n".join(future_changes_lines)
        future_net_change_amount = future_df['quantity_changed'].sum()
    else:
        future_changes_list_str = "No specific future changes found for this query."

    overall_future_total = current_net_pods + future_net_change_amount

    system_prompt = f"""You are a helpful, extremely concise, and accurate CPG sales analyst.
You have been provided with precise, pre-calculated data. Your job is to format it into a clear, two-part answer.

User's original query: "{user_query}"

---
[PRE-CALCULATED DATA FOR YOUR RESPONSE]
Current Date: {current_date_str}
Current_Net_PODs_Calculated: {current_net_pods}
Overall_Future_Total_Calculated: {overall_future_total}
Detailed_Future_Changes_List:
{future_changes_list_str}
---

Your response MUST be structured as two parts:

PART 1: As of Today ({current_date_str}), [State the 'Current_Net_PODs_Calculated' clearly. Directly answer the first part of the user's query regarding their current state/totals based on this number].

PART 2: Future State: [Summarize the 'Detailed_Future_Changes_List' concisely. If it contains "No specific future changes", state that. Then, state the 'Overall_Future_Total_Calculated' clearly as the final future total.]

Be professional and direct. Do not add any extra information not requested by the user, and do not repeat context.
"""
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_query}
        ],
        temperature=0.0
    )
    return response.choices[0].message.content

def get_export_data_for_both_views():
    current_export_plan = {"filters": {}, "group_by": ["product_name", "retailer"], "include_future_dates": False}
    current_data_to_export = execute_query_plan(current_export_plan, include_future_dates_explicit=False)
    
    future_export_plan = {"filters": {}, "group_by": ["product_name", "retailer"], "include_future_dates": True}
    future_data_to_export = execute_query_plan(future_export_plan, include_future_dates_explicit=True)

    current_df_processed = None
    if current_data_to_export is not None and not current_data_to_export.empty:
        current_df_processed = _process_for_export(current_data_to_export, "Current Data")
    else:
        current_df_processed = pd.DataFrame()

    future_df_processed = None
    if future_data_to_export is not None and not future_data_to_export.empty:
        future_df_processed = _process_for_export(future_data_to_export, "Future Data")
    else:
        future_df_processed = pd.DataFrame()

    return current_df_processed, future_df_processed

def _process_for_export(data, view_name):
    if data is None or data.empty:
        return pd.DataFrame()

    df = data.to_frame(name='value')
    if not isinstance(df.index, pd.MultiIndex):
        return pd.DataFrame() # Cannot process if not multi-indexed

    # Ensure index names are consistent for unstacking
    df.index.names = ['Product', 'Retailer']
    
    try:
        final_table = df.unstack(level='Retailer').fillna(0)
        # The column names will be a multi-index ('value', retailer_name), flatten it
        final_table.columns = final_table.columns.droplevel(0)
        final_table = final_table.astype(int)

        if not final_table.empty:
            final_table['Grand Total'] = final_table.sum(axis=1)
            final_table.loc['Grand Total'] = final_table.sum(axis=0)
        
        return final_table
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return pd.DataFrame() # Return empty on error

def get_transaction_log():
    log_df = database.get_recent_transactions()
    return log_df
