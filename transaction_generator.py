import pandas as pd
import numpy as np
import sqlite3
import datetime
import random
import os

def generate_transactions(num_transactions=10000, num_customers=500):
    
    print(f"Generating {num_transactions} transactions for {num_customers} customers...")
    
    customer_ids = [f"C{i:04d}" for i in range(1, num_customers + 1)]
    transaction_types ={
        "Deposit": (10, 5000), 
        "Withdrawal": (5, 2000), 
        "Transfer_Out": (50, 10000),
        "Transfer_In": (50, 10000),
        "Payment": (1, 500)
    }
    locations = ["Frankfurt", "Berlin", "Munich", "Hamburg", "Online", "Mobile App"]
    
    data = []
    start_date = datetime.datetime(2024, 1, 1)
    
    for i in range(num_transactions):
        
        cust_id = random.choice(customer_ids)
        
        trans_type = random.choices(list(transaction_types.keys()), weights=[0.2, 0.3, 0.15, 0.15, 0.2], k=1)[0]
        min_amt, max_amt = transaction_types[trans_type]
        if random.random() < 0.95: # 95% normal
            amount = round(random.uniform(min_amt, max_amt * 0.2), 2) 
        else: # 5% larger
             amount = round(random.uniform(max_amt * 0.2, max_amt), 2)
             
        time_delta = datetime.timedelta(days=random.randint(0, 365), hours=random.randint(0, 23), minutes=random.randint(0,59))
        timestamp = start_date + time_delta
        
        location = random.choice(locations)
        
        trans_id = f"T{i:06d}"
        
        data.append([trans_id, timestamp, cust_id, trans_type, amount, location])
        
    num_anomalies = int(num_transactions * 0.005)
    print(f"Introducing {num_anomalies} anomalies...")
    for _ in range(num_anomalies):
        idx = random.randint(0, num_transactions - 1)
        anomaly_type = random.choice(["amount", "time", "location"])
        
        if anomaly_type == "amount":
            data[idx][3] = random.choice(["Withdrawal", "Payment"])
            data[idx][4] = round(random.uniform(20000, 100000), 2)
        elif anomaly_type == "time":
            original_date = data[idx][1].date()
            odd_time = datetime.time(random.randint(2, 4), random.randint(0, 59))
            data[idx][1] = datetime.datetime.combine(original_date, odd_time)
        elif anomaly_type == "location":
            data[idx][5] = "Unknown/Foreign"
        
    df = pd.DataFrame(data, columns=["TransactionID", "Timestamp", "CustomerID", "TransactionType", "Amount", "Location"])
    print("dataframe created")
    
    return df

def save_to_sqlite(df, db_name="transactions.db", table_name="transactions"):
    if not os.path.exists("database"):
        os.makedirs("database")
    db_path = os.path.join("database", db_name)
    
    conn = sqlite3.connect(db_path)
    print(f"Connected to database '{db_path}'.")  
    
    try: 
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"DataFrame successfully saved to table '{table_name}'.")
    except Exception as e:
        print(f"Error saving DataFrame to SQL: {e}")
    finally:
        # Close the connection
        conn.close()
        print("Database connection closed.")
        
if __name__ == "__main__":
    transaction_data = generate_transactions(num_transactions=20000, num_customers=1000)
    
    print("\n--- Sample Data ---")
    print(transaction_data.head())
    print("-------------------\n")
    
    
    save_to_sqlite(transaction_data)