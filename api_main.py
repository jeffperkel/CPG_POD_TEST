# api_main.py
"""
Main entry point for the CPG POD Tracker's standalone FastAPI service.

This API serves as the programmatic interface for the POD tracker database,
allowing both the Streamlit UI and other automated agents (e.g., a Sales
Planning Agent) to interact with the data in a secure and structured way.

To run locally for development:
1. Set the required environment variables:
   export DB_CONNECTION_STRING="your_supabase_pooler_url"
   export API_KEY="your_secret_key"
2. Run the server:
   python api_main.py
3. Access the interactive documentation at http://127.0.0.1:8000/docs
"""
import os
import logging
from typing import Dict
from fastapi import FastAPI, HTTPException, Depends, Security, UploadFile, File
from fastapi.security import APIKeyHeader
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import pandas as pd
from io import BytesIO
from datetime import datetime

# Import the core logic and database modules from your package
from pod_agent import logic, database

# --- 1. CONFIGURATION & LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load configuration from environment variables for security and portability
DB_CONNECTION_STRING = os.environ.get("DB_CONNECTION_STRING")
# Use a default key for easy local development, but it MUST be replaced in production.
API_KEY = os.environ.get("API_KEY", "dev_secret_key_123") 
API_KEY_NAME = "X-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=True)

# --- 2. SECURITY DEPENDENCY ---
async def get_api_key(api_key: str = Security(api_key_header)):
    """Dependency function to validate the API key from the request header."""
    if api_key == API_KEY:
        return api_key
    else:
        logger.warning("Failed API Key validation attempt.")
        raise HTTPException(
            status_code=403, detail="Could not validate credentials"
        )

# --- 3. FASTAPI APP INITIALIZATION & LIFECYCLE ---
app = FastAPI(
    title="CPG POD Tracker API",
    version="2.0.0",
    description="Provides secure, programmatic access to CPG Point of Distribution data.",
    dependencies=[Depends(get_api_key)] # Apply API key security to all endpoints by default
)

@app.on_event("startup")
def startup_event():
    """Handles application startup logic: connecting to the database."""
    logger.info("API is starting up...")
    if not DB_CONNECTION_STRING:
        logger.critical("FATAL: DB_CONNECTION_STRING environment variable not set.")
        raise RuntimeError("DB_CONNECTION_STRING is not set.")
    try:
        logger.info("Initializing database connection...")
        database.initialize_database(DB_CONNECTION_STRING)
        database.init_db_and_seed()
        logger.info("Database connection and seeding successful.")
    except Exception as e:
        logger.exception("Failed to initialize database during startup.")
        # Raising an exception here will prevent the app from starting
        raise e

# --- 4. Pydantic Models for Request Bodies ---
class NewTransaction(BaseModel):
    product_name: str
    retailer_name: str
    quantity: int
    status: str # "planned" or "lost"
    effective_date: str # "YYYY-MM-DD"

class ChatQuery(BaseModel):
    question: str

# --- 5. API ENDPOINTS ---
@app.get("/", summary="API Root", description="A public endpoint to confirm the API is running.", dependencies=[])
def read_root():
    """A simple endpoint to verify the API is online. No authentication required."""
    return {"message": "Welcome to the CPG POD Tracker API. Access /docs for documentation."}

@app.get("/master_data", summary="Get Master Data Lists")
def get_master_data():
    """Retrieves lists of all valid SKUs and Retailers from the database."""
    try:
        return {
            "skus": database.get_master_data_from_db('skus', 'product_name'),
            "retailers": database.get_master_data_from_db('retailers', 'retailer_name')
        }
    except Exception as e:
        logger.exception("Could not load master data.")
        raise HTTPException(status_code=500, detail=f"Could not load master data: {e}")

@app.post("/transactions", summary="Log a Single Transaction")
def create_transaction(transaction: NewTransaction, user_id: str = "api_user"):
    """Validates and logs a single new POD transaction."""
    try:
        validated_data = logic.validate_and_enrich_data(transaction.dict(), user_id, "api_single")
        logic.process_new_transaction(validated_data)
        return {"status": "success", "data": validated_data}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Internal error processing transaction: {transaction.dict()}")
        raise HTTPException(status_code=500, detail="An internal error occurred.")

@app.post("/transactions/bulk_upload", summary="Upload Transactions from CSV")
async def bulk_upload_transactions(user_id: str = "api_user", file: UploadFile = File(...)):
    """Processes a CSV file to bulk-add transactions."""
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Invalid file type. Please upload a CSV.")
    try:
        # Read file content into a memory stream for the logic function
        csv_stream = BytesIO(await file.read())
        success_count, errors = logic.process_bulk_file(csv_stream, user_id)
        return {"status": "complete", "successful_logs": success_count, "errors": errors}
    except Exception as e:
        logger.exception("Failed during bulk upload processing.")
        raise HTTPException(status_code=500, detail="An error occurred during bulk processing.")

@app.get("/summary", summary="Get POD Distribution Matrix")
        def get_summary_table(include_future: bool = True):
            """
            Returns the main POD summary table, pivoted with products as rows
            and retailers as columns.
            """
            try:
                # Create the plan and execute it using the logic module,
                # just like the old Streamlit app did.
                plan = {"group_by": ["product_name", "retailer"]}
                results = logic.execute_query_plan(plan, include_future_dates_explicit=include_future)
                
                # Check for empty results before processing
                if results is None or results.empty:
                    return {"summary_data": {}}
                
                # Process the results into the final pivot table
                pivot_df = logic._process_for_export(results)
                
                return {"summary_data": pivot_df.to_dict(orient='index')}
            except Exception as e:
                logger.exception("Error generating summary table.")
                raise HTTPException(status_code=500, detail=f"Error in summary query: {e}")

@app.post("/chat", summary="Ask a Conversational Question")
def chat_with_data(query: ChatQuery):
    """
    Processes a natural language question and returns a conversational response
    based on the underlying POD data.
    """
    try:
        answer = logic.generate_conversational_response(query.question)
        return {"answer": answer}
    except Exception as e:
        logger.exception(f"Error processing chat query: {query.question}")
        raise HTTPException(status_code=500, detail="An internal error occurred during chat.")

@app.get("/export/excel", summary="Download Full Excel Report")
def export_to_excel():
    """
    Generates and returns an Excel file with two sheets: one for the current
    state of PODs and one for the future state.
    """
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

# --- 6. DIRECTLY RUNNABLE FOR DEVELOPMENT ---
if __name__ == "__main__":
    import uvicorn
    logger.info("Starting API server for local development...")
    if not DB_CONNECTION_STRING or API_KEY == "dev_secret_key_123":
        logger.warning("-" * 60)
        logger.warning("WARNING: Running with default or missing environment variables.")
        logger.warning("Ensure DB_CONNECTION_STRING and API_KEY are set for proper operation.")
        logger.warning("-" * 60)

    uvicorn.run("api_main:app", host="0.0.0.0", port=8000, reload=True)
