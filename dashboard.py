import streamlit as st 
import pandas as pd
import os

from analyzer import load_data_from_db, find_amount_anomalies, find_time_anomalies
from transaction_generator import generate_transactions, save_to_sqlite

DB_FOLDER = "database"
DB_NAME = "transactions.db"
TABLE_NAME = "transactions"
DB_FILE_PATH = os.path.join(DB_FOLDER, DB_NAME)

if not os.path.exists(DB_FILE_PATH):
    st.warning("Database not found. Generating sample data...")
    with st.spinner("Generating transactions... This might take a moment on first run."):
        if not os.path.exists(DB_FOLDER):
            os.makedirs(DB_FOLDER)
        # Generate a smaller set for faster initial load online
        initial_data = generate_transactions(num_transactions=5000, num_customers=200) 
        save_to_sqlite(initial_data, db_name=DB_NAME, table_name="transactions")
    st.success("Sample database generated.")

    st.cache_data.clear()

st.set_page_config(page_title="Transaction Anomaly Detection", layout="wide")
st.title("Transaction Anomaly Detection")
st.write("This dashboard loads transaction data and flags potential anomalies based on amount and time.")

@st.cache_data
def load_cached_data(db_path, table_name):
    df = load_data_from_db(db_path, table_name)
    
    if df is not None:
        try:
            df['Date'] = df['Timestamp'].dt.date
        except AttributeError:
             st.error("Timestamp column not found or not in expected format.")   
             df['Date'] = None 
    return df

with st.spinner("Loading transaction data from database..."):
    transaction_df = load_cached_data(DB_FILE_PATH, TABLE_NAME)

if transaction_df is None:
    st.error("Failed to load data. Please ensure the database file exists and the generator script has run.")
    st.stop() 

st.success(f"Loaded {len(transaction_df)} transactions.")

with st.expander("View Raw Data Sample"):
    st.dataframe(transaction_df.head(100)) 


st.header("Anomaly Detection Results")

col1, col2 = st.columns(2)

with col1:
    st.subheader("High Amount Anomalies")
    with st.spinner("Calculating amount anomalies..."):
        # We need to pass a copy to avoid modifying the cached dataframe
        amount_anomalies = find_amount_anomalies(transaction_df.copy()) 
    if not amount_anomalies.empty:
        st.dataframe(amount_anomalies[['TransactionID', 'Timestamp', 'CustomerID', 'Amount', 'Amount_ZScore']])
        st.metric(label="High Amount Anomalies Found", value=len(amount_anomalies))
    else:
        st.info("No significant amount anomalies found.")

with col2:
    st.subheader("Odd Hour Transactions (2-4 AM)")
    with st.spinner("Checking transaction times..."):
        time_anomalies = find_time_anomalies(transaction_df.copy())
    if not time_anomalies.empty:
        st.dataframe(time_anomalies[['TransactionID', 'Timestamp', 'CustomerID', 'Amount', 'Hour']])
        st.metric(label="Odd Hour Transactions Found", value=len(time_anomalies))
    else:
        st.info("No transactions found during odd hours.")

st.write("---")
st.caption("End of Report")