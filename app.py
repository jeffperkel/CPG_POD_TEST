import streamlit as st
from sqlalchemy import create_engine
import os

st.set_page_config(layout="wide", page_title="DB Connection Test")

st.title("Database Connection Diagnostic Test")
st.info("This is a temporary app to get the exact database connection error.")

# --- Step 1: Check if the secret exists ---
st.header("Step 1: Reading Streamlit Secrets")
db_url = None
try:
    db_url = st.secrets["DB_CONNECTION_STRING"]
    st.success("✅ Found `DB_CONNECTION_STRING` in Streamlit secrets.")
    # For security, we only show a part of the URL, not the password
    st.code(f"URL Found: {db_url[:25]}...{db_url[-20:]}", language="text")
except KeyError:
    st.error("🚨 FAILED: `DB_CONNECTION_STRING` not found in Streamlit secrets.")
    st.warning("Please double-check the secret name in your app's 'Settings' panel.")
    st.stop()
except Exception as e:
    st.error(f"🚨 An unexpected error occurred while reading secrets: {e}")
    st.stop()


# --- Step 2: Attempt to connect to the database ---
st.header("Step 2: Attempting to Connect to the Database")
if db_url:
    try:
        st.write("Attempting to create the database engine with `create_engine(db_url)`...")
        engine = create_engine(db_url)
        
        st.write("Engine object created. Now attempting to establish a connection with `engine.connect()`...")
        with engine.connect() as connection:
            st.success("✅✅✅ DATABASE CONNECTION SUCCESSFUL! ✅✅✅")
            st.balloons()
            st.code("A connection to the database was successfully established.", language="text")
            st.info("You can now restore your original app.py. The connection string is correct.")

    except Exception as e:
        st.error("🚨 FAILED: The connection attempt raised a critical exception.")
        st.subheader("RAW ERROR MESSAGE:")
        # This will display the full, raw exception to the user. This is our goal.
        st.exception(e)
        
