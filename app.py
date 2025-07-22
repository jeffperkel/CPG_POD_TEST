import streamlit as st
import requests
import pandas as pd
import ast
from datetime import datetime, date
import io
import subprocess
import threading
import time
import os
# import psutil # Removed psutil to simplify startup

# --- Global variable to hold the FastAPI process ---
# This global variable will store the Popen object.
fastapi_process_handle = None
api_base_url = "http://localhost:8000"

# --- Helper functions for managing the FastAPI process ---

def is_process_running_fallback(process_obj):
    """Checks if a Popen process object is still running."""
    if process_obj is None:
        return False
    return process_obj.poll() is None # Returns None if process is still running

def start_fastapi_server():
    """Attempts to start the FastAPI server and returns True on success."""
    global fastapi_process_handle

    # Clean up stale process handles if they exist and are not running
    if fastapi_process_handle and not is_process_running_fallback(fastapi_process_handle):
        print(f"FastAPI process handle found but process seems dead. Resetting.")
        fastapi_process_handle = None 

    # If process is already running, return True
    if fastapi_process_handle and is_process_running_fallback(fastapi_process_handle):
        print("FastAPI server is already running.")
        return True

    print("Attempting to start FastAPI server...")
    try:
        # Construct the command to run uvicorn
        cmd = ["uvicorn", "pod_agent.api:app", "--host", "0.0.0.0", "--port", "8000"]
        
        # Start the process, suppressing stdout/stderr to keep Streamlit logs clean.
        # We rely on pinging to confirm responsiveness.
        fastapi_process_handle = subprocess.Popen(cmd, 
                                                stdout=subprocess.DEVNULL, 
                                                stderr=subprocess.DEVNULL,
                                                start_new_session=True) # start_new_session can help with detaching
        
        print(f"FastAPI process initiated. PID (if available): {getattr(fastapi_process_handle, 'pid', 'N/A')}")

        # --- Wait for the API to become responsive ---
        wait_time = 0
        max_wait_time = 25 # Increased wait time for startup, adjust if needed
        api_responsive = False
        
        while wait_time < max_wait_time:
            if fastapi_process_handle is None or not is_process_running_fallback(fastapi_process_handle):
                print(f"FastAPI process died during startup or was not started.")
                fastapi_process_handle = None
                return False # Process died unexpectedly
            
            try:
                # Ping the API to check if it's ready
                ping_response = requests.get(f"{api_base_url}/", timeout=3) # Use a short timeout
                if ping_response.status_code == 200:
                    print("FastAPI server is running and responsive.")
                    api_responsive = True
                    break
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                # API not ready yet or timed out, continue waiting
                print(f"Waiting for API to become responsive... (attempt {wait_time+1}/{max_wait_time})")
                pass 
                
            time.sleep(1)
            wait_time += 1
            
        if not api_responsive:
            print(f"FastAPI server failed to become responsive within {max_wait_time} seconds.")
            if fastapi_process_handle and is_process_running_fallback(fastapi_process_handle):
                print(f"Terminating unresponsive FastAPI process.")
                fastapi_process_handle.terminate() # Try graceful termination
            fastapi_process_handle = None
            return False
        
        return True # API started and is responsive

    except Exception as e:
        print(f"Error during FastAPI startup: {e}")
        if fastapi_process_handle and is_process_running_fallback(fastapi_process_handle):
            print(f"Terminating FastAPI process due to error.")
            fastapi_process_handle.terminate()
        fastapi_process_handle = None
        return False

def stop_fastapi_server():
    """Stops the FastAPI server if it's running."""
    global fastapi_process_handle
    if fastapi_process_handle and is_process_running_fallback(fastapi_process_handle):
        print(f"Stopping FastAPI server...")
        try:
            fastapi_process_handle.terminate() # Try graceful termination
            fastapi_process_handle.wait(timeout=5) # Wait up to 5 seconds
            print("FastAPI server stopped.")
        except subprocess.TimeoutExpired:
            print("FastAPI server did not terminate gracefully, killing process.")
            fastapi_process_handle.kill() # Force kill if terminate fails
        except Exception as e:
            print(f"Error stopping FastAPI server: {e}")
        finally:
            fastapi_process_handle = None # Ensure handle is cleared
    else:
        print("FastAPI server was not running or handle was invalid.")
        fastapi_process_handle = None # Ensure handle is cleared if it was stale

def check_and_start_api_if_needed():
    """Checks if API is running, and tries to start it if not. Returns True if ready, False otherwise."""
    global fastapi_process_handle
    
    # Clean up stale process handles on app load/rerun
    if fastapi_process_handle and not is_process_running_fallback(fastapi_process_handle):
        print(f"Detected stale FastAPI process. Resetting handle.")
        fastapi_process_handle = None

    if not fastapi_process_handle: # If we don't have an active handle
        if not start_fastapi_server():
            st.error("Failed to start the backend API. Please check the logs for details. The application may not function correctly.")
            return False # Indicate failure to start
    elif not is_process_running_fallback(fastapi_process_handle):
        # Fallback: If process is not running but handle exists, try to restart
        st.warning("FastAPI process unexpectedly stopped. Attempting to restart.")
        if not start_fastapi_server():
            st.error("Failed to restart the backend API.")
            return False
    
    return True # Indicate success or API was already running

def is_api_healthy():
    """Pings the API to confirm it's healthy."""
    if fastapi_process_handle is None or not is_process_running_fallback(fastapi_process_handle):
        return False
    try:
        response = requests.get(f"{api_base_url}/", timeout=3) # Use a short timeout for health check
        return response.status_code == 200
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        return False

# --- Streamlit App Configuration ---
st.set_page_config(layout="wide", page_title="POD Tracker Prototype")

# --- Custom CSS ---
st.markdown("""
<style>
    section[data-testid="stSidebar"] {
        width: 400px !important; 
    }
    .stChatInputContainer, .stChatInputContainer > div {
        padding: 0.5rem 1rem;
    }
    .stMarkdown {
        font-size: 0.95rem;
    }
    /* Styling for the API status message */
    .api-status-success { color: #14B8A6; font-weight: bold; } /* Tailwind green-500 */
    .api-status-error { color: #EF4444; font-weight: bold; }   /* Tailwind red-500 */
    .api-status-warning { color: #F59E0B; font-weight: bold; } /* Tailwind yellow-500 */
</style>
""", unsafe_allow_html=True)

# --- API Startup and Status Check ---
# Ensure this is called early to establish the API status before rendering UI elements
api_ready = check_and_start_api_if_needed()

# --- Sidebar ---
st.sidebar.header("Actions")

if api_ready:
    st.sidebar.markdown("<span class='api-status-success'>✅ Backend API is running.</span>", unsafe_allow_html=True)
    
    # --- Data Entry Form ---
    st.sidebar.header("Log a New Transaction")
    master_data = get_master_data() # Fetch master data only if API is ready

    with st.sidebar.form("transaction_form", clear_on_submit=True):
        st.markdown("Enter details of the POD change.")
        product = st.selectbox("Product Name", sorted(master_data.get("skus", [])), index=None, placeholder="Select...")
        retailer = st.selectbox("Retailer", sorted(master_data.get("retailers", [])), index=None, placeholder="Select...")
        quantity = st.number_input("Quantity", min_value=1, step=1)
        action = st.selectbox(
            "Action", ["Planned", "Lost"], index=0,
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
                    response = requests.post(f"{api_base_url}/transactions", json=payload, timeout=10)
                    if response.status_code == 200:
                        st.success("Transaction logged!")
                        st.cache_data.clear() # Clear cache on data change
                        st.rerun() # Rerun to reflect changes
                    else:
                        st.error(f"API Error: {response.json().get('detail', 'Unknown error')}")
                except requests.exceptions.RequestException as e:
                    st.error(f"Connection Error: {e}")

    st.sidebar.divider()
    
    # --- Bulk Upload ---
    st.sidebar.header("Bulk Upload Transactions")
    uploaded_file = st.sidebar.file_uploader("Choose a CSV file", type="csv")

    if uploaded_file is not None:
        if st.sidebar.button("Process Bulk File"):
            with st.spinner("Processing file..."):
                files = {'file': (uploaded_file.name, uploaded_file.getvalue(), 'text/csv')}
                try:
                    response = requests.post(f"{api_base_url}/transactions/bulk_upload", files=files, timeout=60) # Longer timeout for bulk
                    if response.status_code == 200:
                        result = response.json()
                        st.sidebar.success(f"Bulk add complete! Logged {result['successful_logs']} transactions.")
                        if result['errors']:
                            st.sidebar.warning(f"Skipped {len(result['errors'])} transactions:")
                            st.sidebar.json(result['errors'], expanded=False)
                        st.cache_data.clear()
                        st.rerun() # Rerun to reflect changes
                    else:
                        st.sidebar.error(f"API Error: {response.json().get('detail')}")
                except requests.exceptions.RequestException as e:
                    st.sidebar.error(f"Connection Error: {e}")
    
    st.sidebar.divider()
    # --- Manual API Control ---
    st.sidebar.header("API Management")
    if st.sidebar.button("Stop API Server"):
        stop_fastapi_server()
        st.rerun()
    if st.sidebar.button("Check API Health"):
        if is_api_healthy():
            st.sidebar.success("API is healthy.")
        else:
            st.sidebar.error("API is not responding.")
else:
    st.sidebar.markdown("<span class='api-status-error'>❌ Backend API is not running.</span>", unsafe_allow_html=True)
    if st.button("Attempt to Start API Server"):
        if check_and_start_api_if_needed():
            st.rerun()
        else:
            st.error("Failed to start API server.")

# --- Main Page Content ---
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
    if api_ready:
        try:
            response = requests.get(f"{api_base_url}/export/excel", timeout=30)
            if response.status_code == 200:
                st.download_button(
                    label="📥 Download Excel Report", data=response.content,
                    file_name=f"pod_tracker_report_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            else:
                st.warning(f"Could not download report. API returned status: {response.status_code}")
        except requests.exceptions.RequestException as e:
            st.warning(f"API connection failed for Excel export: {e}")
    else:
        st.warning("API not running. Cannot download report.")

# Get summary data only if API is ready
summary_df = None
if api_ready:
    summary_df = get_summary_data(include_future=include_future_data)

if summary_df is not None and not summary_df.empty:
    st.dataframe(summary_df.style.format("{:,}"), use_container_width=True)
elif summary_df is None and api_ready: # API was ready but call failed
    st.warning("Could not fetch summary data. Please check API status or logs.")
elif summary_df is not None and summary_df.empty: # API ready, returned empty data
    st.info("No POD data found. Add transactions via the sidebar to see the summary.")
else: # API not ready
    st.info("Please start the API server to view POD summaries.")


st.divider()

# --- Chat Interface ---
st.header("Ask me about PODs")
chat_container = st.container(height=400)
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display previous messages
for message in st.session_state.messages:
    with chat_container.chat_message(message["role"]):
        st.markdown(message["content"])

prompt_placeholder = "How many PODs are planned for January 2026 -- what SKUs and Retailers?"
if prompt := st.chat_input(prompt_placeholder):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with chat_container.chat_message("user"):
        st.markdown(prompt)

    with chat_container.chat_message("assistant"):
        with st.spinner("Thinking..."):
            if api_ready:
                try:
                    response = requests.get(f"{api_base_url}/chat_query", params={"question": prompt}, timeout=60)
                    response.raise_for_status()
                    answer = response.json().get("answer", "Sorry, I couldn't get a valid response from the agent.")
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
