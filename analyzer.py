import pandas as pd
import sqlite3
import numpy as np
import os

DB_FOLDER = "database"
DB_NAME = "transactions.db"
TABLE_NAME = "transactions"

def load_data_from_db(db_path, table_name):
    if not os.path.exists(db_path):
        print(f"Error: Database file not found at '{db_path}'")
        return None
    
    conn = sqlite3.connect(db_path)
    print(f"Connected to database '{db_path}'.")
    
    try:
        query = f"SELECT * FROM {table_name}"
        print(f"Executing query: {query}")
        
        df = pd.read_sql_query(query, conn, parse_dates=['Timestamp'])
        print(f"Successfully loaded {len(df)} rows from table '{table_name}'.")
        return df
    except Exception as e:
        print(f"Error loading data from SQL: {e}")
        return None
    finally:
        conn.close()
        print("Database connection closed.")
        
def find_amount_anomalies(df, threshold=3.0):
    df['Amount_ZScore'] = np.abs((df["Amount"] - df['Amount'].mean()) / df['Amount'].std())
    
    anomalies = df[df["Amount_ZScore"] > threshold]
    print(f"\nFound {len(anomalies)} potential amount anomalies (Z-score > {threshold}).")
    return anomalies

def find_time_anomalies(df, start_hour=2, end_hour=4):
    df['Hour'] = df['Timestamp'].dt.hour
    
    
    anomalies = df[(df['Hour'] >= start_hour) & (df['Hour'] <= end_hour)]
    print(f"Found {len(anomalies)} transactions between {start_hour}:00 and {end_hour}:59.")
    return anomalies

if __name__ == "__main__":
    
    db_file_path = os.path.join(DB_FOLDER, DB_NAME)
    
    transaction_df = load_data_from_db(db_file_path, TABLE_NAME)
    
    if transaction_df is not None:
        print("\n--- Starting Anomaly Detection ---")
        
        amount_anomalies = find_amount_anomalies(transaction_df.copy()) # Use .copy() to avoid modifying original df
        if not amount_anomalies.empty:
            print("--- High Amount Anomalies (Sample) ---")
            print(amount_anomalies[['TransactionID', 'Timestamp', 'CustomerID', 'Amount', 'Amount_ZScore']].head())
            print("--------------------------------------")
            
        time_anomalies = find_time_anomalies(transaction_df.copy())
        if not time_anomalies.empty:
            print("\n--- Odd Hour Anomalies (Sample) ---")
            print(time_anomalies[['TransactionID', 'Timestamp', 'CustomerID', 'Amount', 'Hour']].head())
            print("-----------------------------------")
            
        print("\n--- Anomaly Detection Complete ---")