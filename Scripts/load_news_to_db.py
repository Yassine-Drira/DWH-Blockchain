import pandas as pd
import pyodbc
from datetime import datetime

# Read the news CSV file
print("Reading news_.csv...")
df = pd.read_csv("news_.csv")

# Display basic info
print(f"Total rows to load: {len(df)}")
print(f"Columns: {df.columns.tolist()}")

# Connect to SQL Server (localhost)
try:
    conn = pyodbc.connect(
        'DRIVER={SQL Server};'
        'SERVER=mohsen2;'
        'DATABASE=CryptoSourceDB;'
        'Trusted_Connection=yes'
    )
    cursor = conn.cursor()
    print("✅ Connected to CryptoSourceDB")
except Exception as e:
    print(f"❌ Connection error: {e}")
    exit(1)

# Insert data into News table
inserted_count = 0
failed_count = 0

for idx, row in df.iterrows():
    try:
        # Map CSV columns to table columns
        # Convert date column (format: 2025-10-1) to DATE string for SQL Server
        date_str = row.get('date')
        if date_str and str(date_str).strip() and str(date_str) != 'nan':
            try:
                publish_time = str(pd.to_datetime(date_str, format='%Y-%m-%d', errors='coerce').date())
                if publish_time == 'NaT':
                    publish_time = str(datetime.now().date())
            except Exception as date_err:
                publish_time = str(datetime.now().date())
        else:
            publish_time = str(datetime.now().date())
        
        coin_symbol = row.get('coin_symbol') or row.get('coin')
        title = row.get('title')
        subject = row.get('subject')
        sentiment_class = row.get('sentiment_class') or row.get('sentiment')
        polarity = float(row.get('polarity', 0))
        subjectivity = float(row.get('subjectivity', 0))

        # Insert into database
        cursor.execute("""
            INSERT INTO News (publish_time, coin_symbol, title, subject, sentiment_class, polarity, subjectivity)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, publish_time, coin_symbol, title, subject, sentiment_class, polarity, subjectivity)

        inserted_count += 1

        # Commit every 100 rows for performance
        if inserted_count % 100 == 0:
            conn.commit()
            print(f"Progress: {inserted_count} rows inserted...")

    except Exception as e:
        failed_count += 1
        print(f"Row {idx} error: {e}")
        continue

# Final commit
conn.commit()
cursor.close()
conn.close()

print(f"\n✅ Load complete!")
print(f"Inserted: {inserted_count} rows")
print(f"Failed: {failed_count} rows")
