# app_api_client.py
"""
CPG POD Tracker - Streamlit UI (API Client)

This Streamlit application serves as the user-facing interface for the POD
Tracker. It is a pure API client. It does not connect to the database
or use the logic modules directly. All data operations are performed by making
HTTP requests to the standalone FastAPI service.

To run this application, the FastAPI server (`api_main.py`) must be running
and accessible.
"""

import streamlit as st
import pandas as pd
import requests
from io import BytesIO

# --- 1. CONFIGURATION & INITIALIZATION ---
# These values MUST be set in Streamlit's secrets management.
# Go to your app's settings on Streamlit Community Cloud and add these secrets.
API_BASE_URL = st.secrets.get("API_BASE_URL", "http://127.0.0.1:8000")
API_KEY = st.secrets.get("API_KEY")

st.set_page_config(layout="wide", page_title="POD Tracker (API Client)")

# Check for essential configuration
if not API_KEY:
    st.error("🚨 CRITICAL: API_KEY is not set in Streamlit secrets. The app cannot function.")
    st.stop()
if API_BASE_URL == "http://127.0.0.1:8000":
     st.warning("Running against local API. Ensure `api_main.py` is running.")


# --- 2. API HELPER FUNCTIONS ---
# Use a cached session to reuse the connection and headers
@st.cache_resource
def get_api_session():
    """Creates a requests session with the necessary API key header."""
    session = requests.Session()
    session.headers.update({'X-API-Key': API_KEY})
    return session

@st.cache_data(ttl=3600) # Cache master data for an hour
def get_master_data():
    """Fetches master SKU and Retailer lists from the API."""
    session = get_api_session()
    try:
        response = session.get(f"{API_BASE_URL}/master_data")
        response.raise_for_status() # Raises an HTTPError for bad responses (4xx or 5xx)
        return response.json()
    except requests.RequestException as e:
        st.error(f"Failed to fetch master data from API: {e}")
        return {"skus": [], "retailers": []}

@st.cache_data(ttl=60) # Cache summary data for 60 seconds
def get_summary_data(include_future: bool):
    """Fetches the main summary pivot table from the API."""
    session = get_api_session()
    params = {"include_future": include_future}
    try:
        response = session.get(f"{API_BASE_URL}/summary", params=params)
        response.raise_for_status()
        # The API returns a dict; convert it back to a DataFrame
        return pd.DataFrame.from_dict(response.json().get("summary_data", {}), orient='index')
    except requests.RequestException as e:
        st.error(f"Failed to fetch summary data: {e}")
        return pd.DataFrame()


# --- 3. SIDEBAR ---
st.sidebar.image("https://emojicdn.elk.sh/📦", width=80)
st.sidebar.title("POD Tracker")
st.sidebar.markdown("### Actions")

st.sidebar.header("Log a New Transaction")
master_data = get_master_data()
with st.sidebar.form("transaction_form", clear_on_submit=True):
    product = st.selectbox("Product Name", sorted(master_data.get("skus", [])), index=None)
    retailer = st.selectbox("Retailer", sorted(master_data.get("retailers", [])), index=None)
    quantity = st.number_input("Quantity", min_value=1, step=1)
    action = st.selectbox("Action", ["Planned", "Lost"], index=0)
    effective_date = st.date_input("Effective Date")
    submitted = st.form_submit_button("Log Transaction")

    if submitted:
        if not all([product, retailer]):
            st.warning("Please fill out all fields.")
        else:
            payload = {
                "product_name": product,
                "retailer_name": retailer,
                "quantity": quantity,
                "status": action.lower(),
                "effective_date": effective_date.strftime("%Y-%m-%d")
            }
            try:
                session = get_api_session()
                response = session.post(f"{API_BASE_URL}/transactions", json=payload)
                if response.status_code == 200:
                    st.success("Transaction logged successfully via API!")
                    st.cache_data.clear() # Clear all data caches on change
                else:
                    # Show the specific error message from the API
                    st.error(f"API Error: {response.json().get('detail', 'Unknown error')}")
            except requests.RequestException as e:
                st.error(f"Failed to connect to API: {e}")


st.sidebar.divider()
st.sidebar.header("Bulk Upload Transactions")
uploaded_file = st.sidebar.file_uploader("Choose a CSV file", type="csv")
if uploaded_file is not None:
    if st.sidebar.button("Process Bulk File"):
        with st.spinner("Uploading and processing file via API..."):
            try:
                session = get_api_session()
                files = {'file': (uploaded_file.name, uploaded_file.getvalue(), 'text/csv')}
                response = session.post(f"{API_BASE_URL}/transactions/bulk_upload", files=files)

                if response.status_code == 200:
                    result = response.json()
                    st.sidebar.success(f"Bulk add complete! Logged {result['successful_logs']} transactions.")
                    if result.get("errors"):
                        st.sidebar.warning(f"Skipped {len(result['errors'])} rows:", icon="⚠️")
                        st.sidebar.json(result['errors'], expanded=False)
                    st.cache_data.clear()
                else:
                    st.sidebar.error(f"API Error: {response.json().get('detail')}")
            except requests.RequestException as e:
                st.sidebar.error(f"Failed to connect to API: {e}")

# --- 4. MAIN PAGE ---
st.header("POD Summaries")
view_option = st.radio("Select View:", ("Current PODs", "Future State"), horizontal=True, index=1)
include_future = (view_option == "Future State")

# Download button
try:
    session = get_api_session()
    response = session.get(f"{API_BASE_URL}/export/excel")
    response.raise_for_status()
    excel_bytes = response.content
    st.download_button(
        label="📥 Download Excel Report",
        data=excel_bytes,
        file_name="pod_report.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )
except requests.RequestException as e:
    st.warning(f"Could not generate report from API: {e}")


# Display the summary table
summary_df = get_summary_data(include_future)
if not summary_df.empty:
    st.dataframe(summary_df.style.format("{:,}", na_rep='-'), use_container_width=True)
else:
    st.info("No POD data found for the selected view.")

st.divider()
st.header("Ask me about PODs")
if "messages" not in st.session_state:
    st.session_state.messages = []

for msg in st.session_state.messages:
    st.chat_message(msg["role"]).write(msg["content"])

if prompt := st.chat_input("e.g., How many PODs do we have at Target?"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    st.chat_message("user").write(prompt)
    with st.spinner("Thinking..."):
        try:
            session = get_api_session()
            response = session.post(f"{API_BASE_URL}/chat", json={"question": prompt})
            response.raise_for_status()
            answer = response.json().get("answer", "Sorry, I couldn't get a response.")
            st.session_state.messages.append({"role": "assistant", "content": answer})
            st.chat_message("assistant").write(answer)
        except requests.RequestException as e:
            st.error(f"Error getting response from chat API: {e}")
