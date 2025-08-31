import csv
import sqlite3

def csv_to_sql(csv_filepath, db_name):
    # Connect to SQLite database or create it if not exists
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Create table based on CSV file name
    table_name = csv_filepath.split('/')[-1].split('.')[0]
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join([col for col in csv_columns])})")
    
    # Read CSV file and insert data into SQLite table
    with open(csv_filepath, 'r') as f:
        reader = csv.reader(f)
        columns = next(reader)
        
        for row in reader:
            cursor.execute(f"INSERT INTO {table_name} ({', '.join([col for col in columns])}) VALUES ({', '.join(['?' for _ in columns])})", row)
    
    conn.commit()
    conn.close()
