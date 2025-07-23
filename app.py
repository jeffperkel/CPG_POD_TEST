# app.py

import streamlit as st
import requests
import pandas as pd
import ast
from datetime import datetime
import subprocess
import time
import os

# --- Streamlit App Configuration ---
st.set_page_config(layout="wide", page_title="POD Tracker Prototype")

# --- Custom CSS ---
st.markdown("""
<style>
    section[data-testid="stSidebar"] {
        width: 400px !important; 
    }
    .api-status-success { color: #14B8A6; font-weight: bold; }
    .api-status-error { color: #EF4444; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

api_base_url = "http://localhost:8000"

# --- Initialize Session State for Process Handle ---
if "fastapi_process" not in st.session_state:
    st.session_state.fastapi_process = None

# --- Helper functions for managing the FastAPI process ---

def is_process_running(process_obj):
    """Checks if a Popen process object is still running."""
    if process_obj is None:
        return False
    return process_obj.poll() is None

def start_fastapi_server():
    """Starts the FastAPI server and stores the handle in session state."""
    print("Attempting to start FastAPI server...")
    try:
        cmd = ["uvicorn", "pod_agent.api:app", "--host", "0.0.0.0", "--port", "8000"]
        # Use DEVNULL to hide subprocess output from Streamlit logs for cleaner debugging
        process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        st.session_state.fastapi_process = process
        
        # Wait for the API to become responsive
        max_wait_time = 30 # Increased for cloud environment
        for i in range(max_wait_time):
            try:
                response = requests.get(f"{api_base_url}/", timeout=1)
                if response.status_code == 200:
                    print("FastAPI server is running and responsive.")
                    return True
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                print(f"Waiting for API... ({i+1}/{max_wait_time})")
                time.sleep(1)
                
        print("FastAPI server failed to become responsive.")
        stop_fastapi_server()
        return False

    except Exception as e:
        print(f"Error during FastAPI startup: {e}")
        st.session_state.fastapi_process = None
        return False

def stop_fastapi_server():
    """Stops the FastAPI server using the handle from session state."""
    process = st.session_state.get("fastapi_process")
    if is_process_running(process):
        print("Stopping FastAPI server...")
        process.terminate()
        try:
            process.wait(timeout=5)
            print("FastAPI server stopped.")
        except subprocess.TimeoutExpired:
            process.kill()
            print("FastAPI server killed.")
        st.session_state.fastapi_process = None

# --- Main API Status Check at the top of each rerun ---
api_ready = False
if is_process_running(st.session_state.get("fastapi_process")):
    try:
        if requests.get(f"{api_base_url}/", timeout=2).status_code == 200:
            api_ready = True
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        stop_fastapi_server()

if not api_ready:
    if start_fastapi_server():
        api_ready = True
    else:
        st.error("Failed to start the backend API. Please check the app logs for details.", icon="🚨")

# --- Data Caching Functions ---
@st.cache_data(ttl=3600)
def get_master_data():
    if not api_ready: return {"skus": [], "retailers": []}
    try:
        response = requests.get(f"{api_base_url}/master_data")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Could not load master data. API Connection Error: {e}")
        return {"skus": [], "retailers": []}

@st.cache_data(ttl=60)
def get_summary_data(include_future: bool):
    if not api_ready: return pd.DataFrame()
    try:
        response = requests.get(f"{api_base_url}/summary_table_query", params={"include_future": include_future}, timeout=10)
        response.raise_for_status()
        data = response.json().get('result', {})
        if not data: return pd.DataFrame()
        df = pd.DataFrame.from_dict(data, orient='index')
        df.columns = df.columns.astype(str)
        return df
    except (requests.exceptions.RequestException, KeyError, ValueError) as e:
        st.warning(f"Could not fetch or parse summary data. Error: {e}")
        return pd.DataFrame()

# --- Sidebar ---
st.sidebar.header("Actions")

if api_ready:
    st.sidebar.markdown("<span class='api-status-success'>✅ Backend API is running.</span>", unsafe_allow_html=True)
    
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
                payload = {"product_name": product, "retailer_name": retailer, "quantity": quantity, "status": action.lower(), "effective_date": effective_date.strftime("%Y-%m-%d")}
                try:
                    response = requests.post(f"{api_base_url}/transactions", json=payload, timeout=10)
                    if response.status_code == 200:
                        st.success("Transaction logged!")
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error(f"API Error: {response.json().get('detail', 'Unknown error')}")
                except requests.exceptions.RequestException as e:
                    st.error(f"Connection Error: {e}")

    st.sidebar.divider()
    st.sidebar.header("Bulk Upload Transactions")
    uploaded_file = st.sidebar.file_uploader("Choose a CSV file", type="csv")
    if uploaded_file is not None:
        if st.sidebar.button("Process Bulk File"):
            with st.spinner("Processing file..."):
                files = {'file': (uploaded_file.name, uploaded_file.getvalue(), 'text/csv')}
                try:
                    response = requests.post(f"{api_base_url}/transactions/bulk_upload", files=files, timeout=60)
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
                except requests.exceptions.RequestException as e:
                    st.sidebar.error(f"Connection Error: {e}")
else:
    st.sidebar.markdown("<span class='api-status-error'>❌ Backend API is not running.</span>", unsafe_allow_html=True)
    if st.sidebar.button("Retry API Connection"):
        st.rerun()

# --- Main Page Content ---
st.header("POD Summaries")
view_option = st.radio("Select View:", ("Current PODs (As of Today)", "Future State (Including All Plans)"), horizontal=True, index=1)
include_future_data = (view_option == "Future State (Including All Plans)")

col1, col2 = st.columns([3, 1])
with col1:
    st.subheader("Distribution Matrix")
with col2:
    if api_ready:
        try:
            excel_response = requests.get(f"{api_base_url}/export/excel", timeout=30)
            if excel_response.status_code == 200:
                st.download_button(
                    label="📥 Download Excel Report", data=excel_response.content,
                    file_name=f"pod_tracker_report_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
        except requests.exceptions.RequestException:
            st.warning("Export failed: API connection error.")

summary_df = get_summary_data(include_future=include_future_data)

if summary_df is not None and not summary_df.empty:
    st.dataframe(summary_df.style.format("{:,}"), use_container_width=True)
elif api_ready:
    st.info("No POD data found. Add transactions via the sidebar to see the summary.")
else:
    st.info("API is not running. Cannot display summary.")

# --- Chat Interface ---
st.divider()
st.header("Ask me about PODs")
if "messages" not in st.session_state:
    st.session_state.messages = []

chat_container = st.container(height=400)
for message in st.session_state.messages:
    with chat_container.chat_message(message["role"]):
        st.markdown(message["content"])

if prompt := st.chat_input("How many PODs are planned for January 2026?"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with chat_container.chat_message("user"):
        st.markdown(prompt)

    with chat_container.chat_message("assistant"):
        with st.spinner("Thinking..."):
            if api_ready:
                try:
                    response = requests.get(f"{api_base_url}/chat_query", params={"question": prompt}, timeout=60)
                    response.raise_for_status()
                    answer = response.json().get("answer", "Sorry, I couldn't get a valid response.")
                    st.markdown(answer)
                    st.session_state.messages.append({"role": "assistant", "content": answer})
                except requests.exceptions.RequestException as e:
                    error_message = f"Could not connect to the chat API: {e}"
                    st.error(error_message)
                    st.session_state.messages.append({"role": "assistant", "content": error_message})
            else:
                error_message = "Backend API is not running. Cannot process chat queries."
                st.error(error_message)
                st.session_state.messages.append({"role": "assistant", "content": error_message})
