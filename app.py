# app.py

import streamlit as st
import requests
import pandas as pd
from datetime import datetime
import threading
import uvicorn
from io import BytesIO

# --- FastAPI App Setup (Merged) ---
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from pod_agent import logic, database # Import our platform-agnostic modules

# --- DEPENDENCY INJECTION with DETAILED ERROR LOGGING ---
if "db_initialized" not in st.session_state:
    try:
        db_url = st.secrets["DB_CONNECTION_STRING"]
        database.initialize_database(db_url)
        st.session_state.db_initialized = True
    except KeyError:
        st.error("🚨 CRITICAL: DB_CONNECTION_STRING not found in Streamlit secrets.", icon="🔥")
        st.stop()
    except Exception as e:
        # THIS IS THE CRITICAL CHANGE: We now print the raw error.
        st.error("🚨 CRITICAL: Database connection failed. See the detailed error below.", icon="🔥")
        st.error(f"RAW DATABASE ERROR: {e}") # This will show the exact SQLAlchemy error.
        st.stop()

# (The rest of the file is identical to the one you already have)
if database.engine is None:
    st.error("🚨 Database engine could not be created. The application cannot run.", icon="🔥")
    st.stop()

api_app = FastAPI(title="CPG POD Tracker Agent API", version="3.0.0")

class NewTransaction(BaseModel):
    product_name: str
    retailer_name: str
    quantity: int
    status: str
    effective_date: str = None

@api_app.get("/")
def read_root(): return {"message": "Welcome to the CPG POD Tracker API"}

@api_app.get("/master_data")
def get_master_data():
    return {"skus": logic.database.get_master_data_from_db('skus', 'product_name'),
            "retailers": logic.database.get_master_data_from_db('retailers', 'retailer_name')}

@api_app.post("/transactions")
def create_transaction(transaction: NewTransaction, user_id: str = "api_user", source: str = "api"):
    try:
        validated_data = logic.validate_and_enrich_data(transaction.dict(), user_id, source)
        logic.process_new_transaction(validated_data)
        return {"status": "success", "data": validated_data}
    except Exception as e: raise HTTPException(status_code=400, detail=str(e))

@api_app.post("/transactions/bulk_upload")
async def bulk_upload_transactions(user_id: str = "api_user", file: UploadFile = File(...)):
    if not file.filename.endswith('.csv'): raise HTTPException(status_code=400, detail="Invalid file type.")
    try:
        content = await file.read()
        success_count, errors = logic.process_bulk_file(BytesIO(content), user_id)
        return {"status": "complete", "successful_logs": success_count, "errors": errors}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@api_app.get("/export/excel")
def export_to_excel():
    current_df, future_df = logic.get_export_data_for_both_views()
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        current_df.to_excel(writer, sheet_name='Current PODs')
        future_df.to_excel(writer, sheet_name='Future PODs')
    output.seek(0)
    return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 
                             headers={"Content-Disposition": f"attachment; filename=pod_report.xlsx"})

@api_app.get("/summary_table_query")
def get_summary_table_query(include_future: bool):
    plan = {"group_by": ["product_name", "retailer"]}
    results = logic.execute_query_plan(plan, include_future_dates_explicit=include_future)
    pivot = logic._process_for_export(results)
    return {"result": pivot.to_dict(orient='index')}

@api_app.get("/chat_query")
def chat_with_data(question: str):
    return {"answer": logic.generate_conversational_response(question)}

def run_api():
    uvicorn.run(api_app, host="0.0.0.0", port=8000)

if "api_thread_started" not in st.session_state:
    print("Starting FastAPI server in a background thread...")
    database.init_db_and_seed()
    api_thread = threading.Thread(target=run_api, daemon=True)
    api_thread.start()
    st.session_state.api_thread_started = True
    print("FastAPI server thread started.")

st.set_page_config(layout="wide", page_title="POD Tracker Prototype")
api_base_url = "http://localhost:8000"

@st.cache_data(ttl=3600)
def get_master_data():
    response = requests.get(f"{api_base_url}/master_data")
    return response.json()

@st.cache_data(ttl=60)
def get_summary_data(include_future: bool):
    response = requests.get(f"{api_base_url}/summary_table_query", params={"include_future": include_future})
    data = response.json().get('result', {})
    if not data: return pd.DataFrame()
    df = pd.DataFrame.from_dict(data, orient='index')
    df.columns = df.columns.astype(str)
    return df

st.sidebar.markdown("### Actions")
st.sidebar.markdown("<span style='color: #14B8A6; font-weight: bold;'>✅ Backend API is running.</span>", unsafe_allow_html=True)

st.sidebar.header("Log a New Transaction")
master_data = get_master_data()
with st.sidebar.form("transaction_form", clear_on_submit=True):
    product = st.selectbox("Product Name", sorted(master_data.get("skus", [])), index=None, placeholder="Select...")
    retailer = st.selectbox("Retailer", sorted(master_data.get("retailers", [])), index=None, placeholder="Select...")
    quantity = st.number_input("Quantity", min_value=1, step=1)
    action = st.selectbox("Action", ["Planned", "Lost"], index=0)
    effective_date = st.date_input("Effective Date", value=datetime.now())
    submitted = st.form_submit_button("Log Transaction")
    if submitted:
        if not all([product, retailer]):
            st.warning("Please fill out all fields.")
        else:
            payload = {"product_name": product, "retailer_name": retailer, "quantity": quantity, "status": action.lower(), "effective_date": effective_date.strftime("%Y-m-%d")}
            response = requests.post(f"{api_base_url}/transactions", json=payload)
            if response.status_code == 200:
                st.success("Transaction logged!")
                st.cache_data.clear()
                st.rerun()
            else:
                st.error(f"API Error: {response.json().get('detail', 'Unknown error')}")

st.sidebar.divider()
st.sidebar.header("Bulk Upload Transactions")
uploaded_file = st.sidebar.file_uploader("Choose a CSV file", type="csv")
if uploaded_file is not None:
    if st.sidebar.button("Process Bulk File"):
        with st.spinner("Processing file..."):
            files = {'file': (uploaded_file.name, uploaded_file.getvalue(), 'text/csv')}
            response = requests.post(f"{api_base_url}/transactions/bulk_upload", files=files)
            if response.status_code == 200:
                result = response.json()
                st.sidebar.success(f"Bulk add complete! Logged {result['successful_logs']} transactions.")
                if result['errors']:
                    st.sidebar.warning(f"Skipped {len(result['errors'])} transactions:", icon="⚠️")
                    st.sidebar.json(result['errors'], expanded=False)
                st.cache_data.clear()
                st.rerun()
            else:
                st.sidebar.error(f"API Error: {response.json().get('detail', 'Unknown error')}")

st.header("POD Summaries")
view_option = st.radio("Select View:", ("Current PODs", "Future State"), horizontal=True, index=1)
include_future = (view_option == "Future State")

col1, col2 = st.columns([3, 1])
with col1:
    st.subheader("Distribution Matrix")
with col2:
    excel_response = requests.get(f"{api_base_url}/export/excel")
    st.download_button(label="📥 Download Excel Report", data=excel_response.content,
                       file_name="pod_report.xlsx", use_container_width=True)

summary_df = get_summary_data(include_future)
if not summary_df.empty:
    st.dataframe(summary_df.style.format("{:,}"), use_container_width=True)
else:
    st.info("No POD data found.")

st.divider()
st.header("Ask me about PODs")
if "messages" not in st.session_state: st.session_state.messages = []
for msg in st.session_state.messages:
    st.chat_message(msg["role"]).write(msg["content"])

if prompt := st.chat_input():
    st.session_state.messages.append({"role": "user", "content": prompt})
    st.chat_message("user").write(prompt)
    response = requests.get(f"{api_base_url}/chat_query", params={"question": prompt})
    answer = response.json().get("answer", "Sorry, I couldn't get a response.")
    st.session_state.messages.append({"role": "assistant", "content": answer})
    st.chat_message("assistant").write(answer)
