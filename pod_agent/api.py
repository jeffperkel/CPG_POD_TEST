# pod_agent/api.py

import os
import json
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import pandas as pd
import io
from datetime import datetime
from . import logic, database

database.init_db_and_seed()
app = FastAPI(title="CPG POD Tracker Agent API", version="1.0.0")

class NewTransaction(BaseModel):
    product_name: str; retailer_name: str; quantity: int; status: str; effective_date: str = None

@app.get("/")
def read_root(): return {"message": "Welcome to the CPG POD Tracker API"}
@app.get("/master_data")
def get_master_data():
    try:
        valid_skus = logic.database.get_master_data_from_db('skus', 'product_name'); valid_retailers = logic.database.get_master_data_from_db('retailers', 'retailer_key')
        return {"skus": valid_skus, "retailers": valid_retailers}
    except Exception as e: raise HTTPException(status_code=500, detail=f"Could not load master data: {e}")
@app.post("/transactions")
def create_transaction(transaction: NewTransaction, user_id: str = "api_user", source: str = "api"):
    try:
        validated_data = logic.validate_and_enrich_data(transaction.dict(), user_id, source)
        logic.process_new_transaction(validated_data); return {"status": "success", "data": validated_data}
    except ValueError as e: raise HTTPException(status_code=400, detail=str(e))
    except Exception as e: raise HTTPException(status_code=500, detail=f"An internal error occurred: {e}")
@app.get("/transactions/log")
def get_transactions_log():
    try:
        log_df = logic.get_transaction_log()
        if log_df is None:
            return []
        return json.loads(log_df.to_json(orient="records"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve transaction log: {e}")

# MODIFIED: New endpoint for dashboard summary - accepts explicit boolean for date filtering
@app.get("/summary_table_query")
def get_summary_table_query(include_future: bool):
    try:
        # Simplified query plan for the dashboard table
        query_plan = {"filters": {}, "group_by": ["product_name", "retailer"]}
        # Pass the explicit boolean directly
        results = logic.execute_query_plan(query_plan, include_future_dates_explicit=include_future)
        
        if results is None: return {"result": {}}

        if isinstance(results.index, pd.MultiIndex):
            results.index = results.index.map(lambda x: str(x))
        elif isinstance(results.index, pd.Index):
            results.index = results.index.map(str)
            
        return {"result": json.loads(results.to_json())}
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An internal error occurred during summary table query: {e}")


# MODIFIED: Original /query endpoint - now only relies on LLM to generate plan
@app.get("/query")
def query_data(question: str):
    try:
        query_plan = logic.generate_query_plan(question) # LLM still generates the plan
        # Pass the LLM's derived include_future_dates to the explicit parameter
        results = logic.execute_query_plan(query_plan, include_future_dates_explicit=query_plan.get("include_future_dates", False))
        if results is None: return {"query": question, "result": {}}

        if isinstance(results.index, pd.MultiIndex):
            results.index = results.index.map(lambda x: str(x))
        elif isinstance(results.index, pd.Index):
            results.index = results.index.map(str)
            
        return {"query": question, "plan": query_plan, "result": json.loads(results.to_json())}
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An internal error occurred during query: {e}")
@app.get("/chat_query")
def chat_with_data(question: str):
    try: answer = logic.generate_conversational_response(question); return {"answer": answer}
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
        csv_stream = io.StringIO(csv_content.decode('utf-8'))
        
        success_count, errors = logic.process_bulk_file(csv_stream, user_id)
        return {"status": "complete", "successful_logs": success_count, "errors": errors}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred during bulk processing: {e}")

@app.get("/export/excel")
def export_to_excel():
    try:
        export_df = logic.generate_export_dataframe()
        if export_df is None: raise HTTPException(status_code=404, detail="No data available to export.")
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer: export_df.to_excel(writer, sheet_name='POD_Tracker')
        output.seek(0)
        return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition": f"attachment; filename=pod_tracker_report_{datetime.now().strftime('%Y%m%d')}.xlsx"})
    except Exception as e: raise HTTPException(status_code=500, detail=f"Failed to generate Excel report: {e}")
