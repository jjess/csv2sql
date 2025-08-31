import sys
import getopt
import csv
import pymysql
import yaml

CONFIG_PATH = 'src/etc/config.yml'   # Global variable for config path

def usage():
    print("Usage: python3 csv2sql.py    --csvfile <csv_file> [--dbtable <db_table>]")

def load_config(config_path):
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config['mysql_connection']

def csv_to_sql(csv_filepath, db_name):
    # Load database configuration from YAML file
    config = load_config(CONFIG_PATH)
    
    # Connect to MariaDB database or create it if not exists
    conn = pymysql.connect(host=config['host'], user=config['user'], password=config['password'])
    cursor = conn.cursor()
    
    # Create table based on CSV file name
    table_name = csv_filepath.split('/')[-1].split('.')[0] if not db_name else db_name

    with open(csv_filepath, 'r') as f:
        reader = csv.reader(f)
        columns = next(reader)   # Get the column names from the CSV file
        
        # Create table in MariaDB database based on the column names
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name}  ({', '.join([col + ' TEXT' for col in columns])})")
    
    conn.commit()   # Commit changes to the database

    with open(csv_filepath, 'r') as f:
        reader = csv.reader(f)
        next(reader)   # Skip column names
        
        for row in reader:
            cursor.execute(f"INSERT INTO {table_name}  ({', '.join([col for col in columns])}) VALUES  ({', '.join(['%s' for _ in columns])})", row)
    
    conn.commit()   # Commit changes to the database
    conn.close()   # Close connection to the MariaDB database

if __name__ == "__main__":
    try:
        opts, args = getopt.getopt(sys.argv[1:], "", ["csvfile=", "dbtable="])
    except getopt.GetoptError as err:
        print(err)
        usage()
        sys.exit(2)
    
    csv_filepath = None
    db_name = None
    for opt, arg in opts:
        if opt == "--csvfile":
            csv_filepath = arg
        elif opt == "--dbtable":
            db_name = arg
    
    if not csv_filepath:
        usage()
        sys.exit(2)
        
    csv_to_sql(csv_filepath, db_name)
