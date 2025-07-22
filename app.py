# app.py

import streamlit as st
import requests
import pandas as pd
import ast
from datetime import datetime
import io
import subprocess # NEW IMPORT
import threading # NEW IMPORT
import time # NEW IMPORT
import os # NEW IMPORT

# Function to start the FastAPI server
def start_fastapi():
    # Change to the directory where your pod_agent folder is
    # Adjust 'pod_agent.api:app' if your folder/file structure is different
    cmd = ["uvicorn", "pod_agent.api:app", "--host", "0.0.0.0", "--port", "8000"]
    # Use subprocess.Popen for non-blocking execution
    process = subprocess.Popen(cmd)
    return process

# Check if FastAPI is already running (e.g., in Streamlit Cloud's reruns)
# Use a simple flag in session_state or check for process.
if 'fastapi_process' not in st.session_state:
    st.session_state.fastapi_process = None

if st.session_state.fastapi_process is None or st.session_state.fastapi_process.poll() is not None:
    # Start FastAPI in a new thread to avoid blocking Streamlit's main thread
    # This is crucial for Streamlit Community Cloud deployment
    # It also implicitly sets API_URL to http://0.0.0.0:8000 which is how containers talk
    threading.Thread(target=start_fastapi, daemon=True).start()
    st.session_state.fastapi_process = True # Set a flag that it's attempting to run
    
    # Give FastAPI a moment to start up
    # This is a hack, but often necessary in containerized environments
    time.sleep(3) # Wait for 3 seconds

# --- Initial API_URL: This will now refer to the internal FastAPI server ---
# Streamlit Cloud runs everything within the same container, so localhost works.
# We must ensure the FastAPI server is configured to listen on 0.0.0.0 (done in start_fastapi)
API_URL = "http://localhost:8000"


st.set_page_config(layout="wide", page_title="POD Tracker Prototype")

# --- Custom CSS to Widen Sidebar ---
st.markdown("""
<style>
    section[data-testid="stSidebar"] {
        width: 400px !important; 
    }
</style>
""", unsafe_allow_html=True)


# --- Constants and API Configuration (API_URL is defined above) ---
VALID_ACTIONS = ["Planned", "Lost"]

# --- Data Caching Functions ---
@st.cache_data(ttl=3600)
def get_master_data():
    try:
        response = requests.get(f"{API_URL}/master_data")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Could not load master data. Error: {e}. Is the API server running?")
        return {"skus": [], "retailers": []}

@st.cache_data(ttl=60)
def get_summary_data(include_future: bool):
    try:
        response = requests.get(f"{API_URL}/summary_table_query", params={"include_future": include_future})
        response.raise_for_status()
        data = response.json()
        result_dict = data.get('result', {})
        if result_dict:
            try:
                index_tuples = []
                for key_str in result_dict.keys():
                    try:
                        index_tuples.append(ast.literal_eval(key_str))
                    except (ValueError, SyntaxError):
                        index_tuples.append((key_str,))
                        
                if index_tuples and isinstance(index_tuples[0], tuple):
                    if len(index_tuples[0]) == 2:
                        multi_index = pd.MultiIndex.from_tuples(index_tuples, names=['Product', 'Retailer'])
                    else:
                        multi_index = pd.Index([t[0] for t in index_tuples], name='Dimension')
                else:
                    multi_index = pd.Index(index_tuples, name='Dimension')

                s = pd.Series(result_dict.values(), index=multi_index)
                
                if isinstance(s.index, pd.MultiIndex) and s.index.nlevels > 1:
                    pivot_df = s.unstack(level='Retailer').fillna(0).astype(int)
                else:
                    pivot_df = pd.DataFrame(s).transpose()

                if not pivot_df.empty:
                    pivot_df['Grand Total'] = pivot_df.sum(axis=1)
                    pivot_df.loc['Grand Total'] = pivot_df.sum(axis=0)
                return pivot_df
            except Exception as e:
                st.error(f"Could not parse pivot data from API: {e}")
                return pd.DataFrame()
        return pd.DataFrame()
    except requests.exceptions.RequestException as e:
        st.warning(f"Could not connect to API to fetch detailed data. Error: {e}")
        return None

# --- Main App ---
st.title("POD tracking Prototype")

# --- Sidebar for Data Entry ---
st.sidebar.header("Log a New Transaction")
master_data = get_master_data()

with st.sidebar.form("transaction_form", clear_on_submit=True):
    st.markdown("Enter details of the POD change.")
    product = st.selectbox("Product Name", sorted(master_data.get("skus", [])), index=None, placeholder="Select...")
    retailer = st.selectbox("Retailer", sorted(master_data.get("retailers", [])), index=None, placeholder="Select...")
    quantity = st.number_input("Quantity", min_value=1, step=1)
    action = st.selectbox(
        "Action", VALID_ACTIONS, index=0,
        help="Select 'Planned' for a gain (becomes 'Live' on its effective date). Select 'Lost' for a discontinuation."
    )
    effective_date = st.date_input("Effective Date", value=datetime.now())
    submitted = st.form_submit_button("Log Transaction")

    if submitted:
        if not all([product, retailer]):
            st.warning("Please fill out all fields.")
        else:
            payload = {
                "product_name": product, "retailer_name": retailer, "quantity": quantity,
                "status": action.lower(), "effective_date": effective_date.strftime("%Y-%m-%d")
            }
            try:
                response = requests.post(f"{API_URL}/transactions", json=payload)
                if response.status_code == 200:
                    st.success("Transaction logged!")
                    st.cache_data.clear()
                else:
                    st.error(f"Error: {response.json().get('detail', 'Unknown error')}")
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
                response = requests.post(f"{API_URL}/transactions/bulk_upload", files=files)
                if response.status_code == 200:
                    result = response.json()
                    st.sidebar.success(f"Bulk add complete! Logged {result['successful_logs']} transactions.")
                    if result['errors']:
                        st.sidebar.warning(f"Skipped {len(result['errors'])} transactions:")
                        st.sidebar.json(result['errors'], expanded=False)
                    st.cache_data.clear()
                else:
                    st.sidebar.error(f"API Error: {response.json().get('detail')}")
            except requests.exceptions.RequestException as e:
                st.sidebar.error(f"Connection Error: {e}")

# --- Main Page Layout ---
st.header("POD Summaries")

view_option = st.radio(
    "Select View:",
    ("Current PODs (As of Today)", "Future State (Including All Plans)"),
    horizontal=True,
    index=1
)
include_future_data = (view_option == "Future State (Including All Plans)")

col1, col2 = st.columns([3, 1])
with col1:
    st.subheader("Distribution Matrix")
with col2:
    try:
        response = requests.get(f"{API_URL}/export/excel")
        if response.status_code == 200:
            st.download_button(
                label="📥 Download Excel Report", data=response.content,
                file_name=f"pod_tracker_report_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
    except requests.exceptions.RequestException as e:
        st.warning(f"API connection failed for Excel export: {e}")

summary_df = get_summary_data(include_future=include_future_data)
if summary_df is not None and not summary_df.empty:
    st.dataframe(summary_df.style.format("{:,}"), use_container_width=True)
elif summary_df is None:
    st.warning("Could not connect to API to fetch detailed data for summary.")
else:
    st.info("No POD data found. Add transactions via the sidebar to see the summary.")

st.divider()

st.header("Ask me about PODs")
chat_container = st.container(height=400)
if "messages" not in st.session_state:
    st.session_state.messages = []
if not st.session_state.messages:
    with chat_container:
        with st.chat_message("assistant"):
            st.write("Hello! I'm your PODs analyst. Ask me anything about your data.")
for message in st.session_state.messages:
    with chat_container.chat_message(message["role"]):
        st.markdown(message["content"])

prompt_placeholder = "How many PODs are planned for January 2026 -- what SKUs and Retailers?"
if prompt := st.chat_input(prompt_placeholder):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with chat_container.chat_message("user"):
        st.markdown(prompt)
    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            try:
                response = requests.get(f"{API_URL}/chat_query", params={"question": prompt})
                response.raise_for_status()
                answer = response.json().get("answer", "Sorry, I couldn't get a valid response from the agent.")
                st.markdown(answer)
                st.session_state.messages.append({"role": "assistant", "content": answer})
            except requests.exceptions.RequestException as e:
                error_message = f"Could not connect to the chat API: {e}"
                st.error(error_message)
                st.session_state.messages.append({"role": "assistant", "content": error_message})
