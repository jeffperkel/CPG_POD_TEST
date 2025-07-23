# pod_agent/api.py

import os
import json
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import pandas as pd
from io import BytesIO
from datetime import datetime

# Import logic module and database initialization
from . import logic, database

# IMPORTANT: To apply the new database schema, you may need to delete the
# 'pods_ledger.db' file before the first run of this updated code.
database.init_db_and_seed()
app = FastAPI(title="CPG POD Tracker Agent API", version="1.1.0") # Version bump!

class NewTransaction(BaseModel):
    product_name: str
    retailer_name: str
    quantity: int
    status: str
    effective_date: str = None

@app.get("/")
def read_root():
    return {"message": "Welcome to the CPG POD Tracker API"}

@app.get("/master_data")
def get_master_data():
    try:
        # Get user-friendly names for the UI dropdowns
        valid_skus = logic.database.get_master_data_from_db('skus', 'product_name')
        valid_retailers = logic.database.get_master_data_from_db('retailers', 'retailer_name')
        return {"skus": valid_skus, "retailers": valid_retailers}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not load master data: {e}")

@app.post("/transactions")
def create_transaction(transaction: NewTransaction, user_id: str = "api_user", source: str = "api"):
    try:
        # The logic layer now handles all the new complexity internally
        validated_data = logic.validate_and_enrich_data(transaction.dict(), user_id, source)
        logic.process_new_transaction(validated_data)
        # The returned data is now more detailed (includes IDs), which is a good thing
        return {"status": "success", "data": validated_data}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An internal error occurred: {e}")

@app.get("/transactions/log")
def get_transactions_log():
    try:
        log_df = logic.get_transaction_log()
        if log_df is None:
            return []
        # Convert NaN to None for proper JSON representation
        log_df = log_df.where(pd.notnull(log_df), None)
        return json.loads(log_df.to_json(orient="records"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve transaction log: {e}")

@app.get("/summary_table_query")
def get_summary_table_query(include_future: bool):
    try:
        query_plan = {"filters": {}, "group_by": ["product_name", "retailer"], "include_future_dates": include_future}
        results = logic.execute_query_plan(query_plan, include_future_dates_explicit=include_future)
        
        if results is None: return {"result": {}}

        if isinstance(results.index, pd.MultiIndex):
            results.index = results.index.map(lambda x: str(x))
        elif isinstance(results.index, pd.Index):
            results.index = results.index.map(str)
            
        return {"result": json.loads(results.to_json(default_handler=str))}
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An internal error occurred during summary table query: {e}")

@app.get("/query")
def query_data(question: str):
    try:
        query_plan = logic.generate_query_plan(question)
        results = logic.execute_query_plan(query_plan, include_future_dates_explicit=query_plan.get("include_future_dates", False))
        if results is None: return {"query": question, "result": {}}

        if isinstance(results.index, pd.MultiIndex):
            results.index = results.index.map(lambda x: str(x))
        elif isinstance(results.index, pd.Index):
            results.index = results.index.map(str)
            
        return {"query": question, "plan": query_plan, "result": json.loads(results.to_json(default_handler=str))}
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An internal error occurred during query: {e}")

@app.get("/chat_query")
def chat_with_data(question: str):
    try:
        answer = logic.generate_conversational_response(question)
        return {"answer": answer}
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An internal error occurred during chat: {e}")

@app.post("/transactions/bulk_upload")
async def bulk_upload_transactions(user_id: str = "api_user", file: UploadFile = File(...)):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Invalid file type. Please upload a CSV.")
    
    try:
        csv_content = await file.read()
        csv_stream = BytesIO(csv_content)
        
        success_count, errors = logic.process_bulk_file(csv_stream, user_id)
        return {"status": "complete", "successful_logs": success_count, "errors": errors}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred during bulk processing: {e}")

@app.get("/export/excel")
def export_to_excel():
    try:
        current_data_df, future_data_df = logic.get_export_data_for_both_views()
        
        if (current_data_df is None or current_data_df.empty) and \
           (future_data_df is None or future_data_df.empty):
            raise HTTPException(status_code=404, detail="No data available for export.")

        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            if current_data_df is not None and not current_data_df.empty:
                current_data_df.to_excel(writer, sheet_name='Current PODs')
            else:
                pd.DataFrame({'Message': ['No current POD data available']}).to_excel(writer, sheet_name='Current PODs', index=False)

            if future_data_df is not None and not future_data_df.empty:
                future_data_df.to_excel(writer, sheet_name='Future PODs')
            else:
                pd.DataFrame({'Message': ['No future POD data available']}).to_excel(writer, sheet_name='Future PODs', index=False)
        
        output.seek(0)
        return StreamingResponse(output, 
                                 media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 
                                 headers={"Content-Disposition": f"attachment; filename=pod_tracker_report_{datetime.now().strftime('%Y%m%d')}.xlsx"})
    except HTTPException as he:
        raise he
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to generate Excel report: {e}")
