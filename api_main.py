# api_main.py
"""
Main entry point for the CPG POD Tracker's standalone FastAPI service.
"""
import os
import logging
from typing import Dict
from fastapi import FastAPI, HTTPException, Depends, Security, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import pandas as pd
from io import BytesIO
from datetime import datetime

from pod_agent import logic, database

# --- 1. CONFIGURATION & LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_CONNECTION_STRING = os.environ.get("DB_CONNECTION_STRING")
API_KEY = os.environ.get("API_KEY", "dev_secret_key_123") 
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
API_KEY_NAME = "X-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=True)

# --- 2. SECURITY DEPENDENCY ---
async def get_api_key(api_key: str = Security(api_key_header)):
    if api_key == API_KEY:
        return api_key
    else:
        logger.warning("Failed API Key validation attempt.")
        raise HTTPException(status_code=403, detail="Could not validate credentials")

# --- 3. FASTAPI APP INITIALIZATION & LIFECYCLE ---
app = FastAPI(
    title="CPG POD Tracker API",
    version="2.0.1",
    description="Provides secure, programmatic access to CPG Point of Distribution data.",
    dependencies=[Depends(get_api_key)]
)

# Configure CORS
origins = [
    "https://*.streamlit.app",         # Allow any deployed Streamlit app
    "http://localhost:8501",           # For local Streamlit development
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
def startup_event():
    logger.info("API is starting up...")
    if not DB_CONNECTION_STRING:
        raise RuntimeError("FATAL: DB_CONNECTION_STRING environment variable not set.")
    if not OPENAI_API_KEY:
        raise RuntimeError("FATAL: OPENAI_API_KEY environment variable not set.")
    try:
        logger.info("Initializing database connection...")
        database.initialize_database(DB_CONNECTION_STRING)
        database.init_db_and_seed()
        logger.info("Database connection and seeding successful.")
    except Exception as e:
        logger.exception("Failed to initialize database during startup.")
        raise e

# --- 4. Pydantic Models for Request Bodies ---
class NewTransaction(BaseModel):
    product_name: str
    retailer_name: str
    quantity: int
    status: str
    effective_date: str

class ChatQuery(BaseModel):
    question: str

# --- 5. API ENDPOINTS ---
@app.get("/", summary="API Root", dependencies=[])
def read_root():
    return {"message": "Welcome to the CPG POD Tracker API."}

@app.get("/master_data", summary="Get Master Data Lists")
def get_master_data():
    try:
        return {
            "skus": database.get_master_data_from_db('skus', 'product_name'),
            "retailers": database.get_master_data_from_db('retailers', 'retailer_name')
        }
    except Exception as e:
        logger.exception("Could not load master data.")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/transactions", summary="Log a Single Transaction")
def create_transaction(transaction: NewTransaction, user_id: str = "api_user"):
    try:
        validated_data = logic.validate_and_enrich_data(transaction.dict(), user_id, "api_single")
        logic.process_new_transaction(validated_data)
        return {"status": "success", "data": validated_data}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Internal error processing transaction: {transaction.dict()}")
        raise HTTPException(status_code=500, detail="An internal error occurred.")

# THIS IS THE CORRECTED /summary ENDPOINT
@app.get("/summary", summary="Get POD Distribution Matrix")
def get_summary_table(include_future: bool = True):
    try:
        plan = {"group_by": ["product_name", "retailer"]}
        results = logic.execute_query_plan(plan, include_future_dates_explicit=include_future)
        if results is None or results.empty:
            return {"summary_data": {}}
        pivot_df = logic._process_for_export(results)
        return {"summary_data": pivot_df.to_dict(orient='index')}
    except Exception as e:
        logger.exception("Error generating summary table.")
        raise HTTPException(status_code=500, detail=f"Error in summary query: {e}")

@app.post("/chat", summary="Ask a Conversational Question")
def chat_with_data(query: ChatQuery):
    try:
        answer = logic.generate_conversational_response(query.question)
        return {"answer": answer}
    except Exception as e:
        logger.exception(f"Error processing chat query: {query.question}")
        raise HTTPException(status_code=500, detail="An internal error occurred during chat.")

@app.get("/export/excel", summary="Download Full Excel Report")
def export_to_excel():
    try:
        current_df, future_df = logic.get_export_data_for_both_views()
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            current_df.to_excel(writer, sheet_name='Current PODs')
            future_df.to_excel(writer, sheet_name='Future PODs')
        output.seek(0)
        filename = f"pod_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        logger.exception("Failed to generate Excel report.")
        raise HTTPException(status_code=500, detail="Failed to generate Excel report.")

# (We don't need the bulk upload endpoint for the streamlit app, but it can be added back here if needed for other agents)

# (We don't need the `if __name__ == "__main__"` block for Render deployment)
