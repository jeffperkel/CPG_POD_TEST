# app.py

import streamlit as st
import pandas as pd
from datetime import datetime
from io import BytesIO
from pod_agent import logic, database

st.set_page_config(layout="wide", page_title="POD Tracker Prototype")

# --- INITIALIZATION ---
# This block runs only once per session, ensuring the DB is set up correctly.
@st.cache_resource
def initialize_app():
    try:
        db_url = st.secrets["DB_CONNECTION_STRING"]
        database.initialize_database(db_url)
        database.init_db_and_seed()
        return True
    except Exception as e:
        st.error(f"🚨 CRITICAL: Application failed to initialize: {e}", icon="🔥")
        return False

app_ready = initialize_app()

if not app_ready:
    st.warning("Application is not ready due to an initialization error. Please check the logs.")
    st.stop()


# --- UI HELPER FUNCTIONS (No longer use requests) ---
# We now call logic functions directly. Caching is still important.
@st.cache_data(ttl=3600)
def get_master_data():
    return {
        "skus": logic.database.get_master_data_from_db('skus', 'product_name'),
        "retailers": logic.database.get_master_data_from_db('retailers', 'retailer_name')
    }

@st.cache_data(ttl=60)
def get_summary_data(include_future: bool):
    plan = {"group_by": ["product_name", "retailer"]}
    results = logic.execute_query_plan(plan, include_future_dates_explicit=include_future)
    pivot = logic._process_for_export(results)
    return pivot

# --- SIDEBAR ---
st.sidebar.markdown("### Actions")

st.sidebar.header("Log a New Transaction")
master_data = get_master_data()
with st.sidebar.form("transaction_form", clear_on_submit=True):
    product = st.selectbox("Product Name", sorted(master_data.get("skus", [])), index=None)
    retailer = st.selectbox("Retailer", sorted(master_data.get("retailers", [])), index=None)
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
                validated_data = logic.validate_and_enrich_data(payload, "streamlit_user", "ui_form")
                logic.process_new_transaction(validated_data)
                st.success("Transaction logged!")
                st.cache_data.clear() # Clear cache on data change
            except Exception as e:
                st.error(f"Error: {e}")

st.sidebar.divider()
st.sidebar.header("Bulk Upload Transactions")
uploaded_file = st.sidebar.file_uploader("Choose a CSV file", type="csv")
if uploaded_file is not None:
    if st.sidebar.button("Process Bulk File"):
        with st.spinner("Processing file..."):
            try:
                # We pass the file-like object directly to the logic function
                success_count, errors = logic.process_bulk_file(uploaded_file, "streamlit_user")
                st.sidebar.success(f"Bulk add complete! Logged {success_count} transactions.")
                if errors:
                    st.sidebar.warning(f"Skipped {len(errors)} transactions:", icon="⚠️")
                    st.sidebar.json(errors, expanded=False)
                st.cache_data.clear()
            except Exception as e:
                st.sidebar.error(f"A critical error occurred: {e}")


# --- MAIN PAGE ---
st.header("POD Summaries")
view_option = st.radio("Select View:", ("Current PODs", "Future State"), horizontal=True, index=1)
include_future = (view_option == "Future State")

col1, col2 = st.columns([3, 1])
with col1:
    st.subheader("Distribution Matrix")
with col2:
    try:
        current_df, future_df = logic.get_export_data_for_both_views()
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            current_df.to_excel(writer, sheet_name='Current PODs')
            future_df.to_excel(writer, sheet_name='Future PODs')
        output.seek(0)
        st.download_button(label="📥 Download Excel Report", data=output,
                           file_name="pod_report.xlsx", use_container_width=True)
    except Exception as e:
        st.warning(f"Could not generate report: {e}")

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
    with st.spinner("Thinking..."):
        try:
            answer = logic.generate_conversational_response(prompt)
            st.session_state.messages.append({"role": "assistant", "content": answer})
            st.chat_message("assistant").write(answer)
        except Exception as e:
            st.error(f"Error getting response: {e}")
