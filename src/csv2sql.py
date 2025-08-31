import sys
import getopt
import csv
import sqlite3

def usage():
    print("Usage: python3 csv2sql.py --csvfile <csv_file> [--dbtable <db_table>]")

def csv_to_sql(csv_filepath, db_name):
    # Connect to SQLite database or create it if not exists
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Create table based on CSV file name
    table_name = csv_filepath.split('/')[-1].split('.')[0]

    with open(csv_filepath, 'r') as f:
        reader = csv.reader(f)
        columns = next(reader)  # Get the column names from the CSV file
        
        # Create table in SQLite database based on the column names
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name}   ({', '.join([col + ' TEXT' for col in columns])})")
    
    conn.commit()  # Commit changes to the database

    with open(csv_filepath, 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Skip column names
        
        for row in reader:
            cursor.execute(f"INSERT INTO {table_name}   ({', '.join([col for col in columns])}) VALUES   ({', '.join(['?' for _ in columns])})", row)
    
    conn.commit()  # Commit changes to the database
    conn.close()  # Close connection to the SQLite database

if __name__ == "__main__":
    try:
        opts, args = getopt.getopt(sys.argv[1:], "", ["csvfile=", "dbtable="])
    except getopt.GetoptError as err:
        print(err)
        usage()
        sys.exit(2)
    
    csv_filepath = None
    db_name = 'test_data'
    for opt, arg in opts:
        if opt == "--csvfile":
            csv_filepath = arg
        elif opt == "--dbtable":
            db_name = arg
    
    if not csv_filepath:
        usage()
        sys.exit(2)
        
    csv_to_sql(csv_filepath, db_name)
