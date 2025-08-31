import sys
import csv
import sqlite3

def csv_to_sql(csv_filepath, db_name):
    # Connect to SQLite database or create it if not exists
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Create table based on CSV file name
    table_name = csv_filepath.split('/')[-1].split('.')[0]

    with open(csv_filepath, 'r') as f:
        reader = csv.reader(f)
        columns = next(reader)   # Get the column names from the CSV file
        
        # Create table in SQLite database based on the column names
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name}  ({', '.join([col + ' TEXT' for col in columns])})")
    
    conn.commit()   # Commit changes to the database

    with open(csv_filepath, 'r') as f:
        reader = csv.reader(f)
        next(reader)   # Skip column names
        
        for row in reader:
            cursor.execute(f"INSERT INTO {table_name}  ({', '.join([col for col in columns])}) VALUES  ({', '.join(['?' for _ in columns])})", row)
    
    conn.commit()   # Commit changes to the database
    conn.close()   # Close connection to the SQLite database

if __name__ == "__main__":
    csv_filepath = sys.argv[1]
    db_name = sys.argv[2]
    csv_to_sql(csv_filepath, db_name)
