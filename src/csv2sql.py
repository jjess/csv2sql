#!/usr/bin/env python3
#
# This script converts CSV data into SQLite database tables.
import sys
import getopt
import csv
import pymysql
import yaml
from datetime import datetime
import logging

CONFIG_PATH = 'src/etc/config.yml'       # Global variable for config path
DB_NAME = 'test'     # Default database name
LOGFILE = "log/csv2sql.log"   # Log file location

# Set up logging to a file
logging.basicConfig(level=logging.INFO, format='%(asctime)s  %(message)s', filename=LOGFILE, filemode='w')
logger = logging.getLogger()

def usage():
    '''Prints the usage of this script'''
    print("Usage: python3 csv2sql.py   --csvfile <csv_file> [--dbtable <db_table>]")

def load_config(config_path):
    '''Loads database configuration from a YAML file'''
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config['mysql_connection']

def normalize_columns(columns):
    '''Normalizes column names to lower case and replaces spaces with underscores'''
    return [col.lower().replace(' ', '_') for col in columns]

def create_table_sql(table_name, columns):
    '''Creates SQL code for creating a table based on the given columns'''
    normalized_columns = normalize_columns(columns)
    sql = f"""DROP TABLE IF EXISTS {DB_NAME}.{table_name}; 
               CREATE TABLE {DB_NAME}.{table_name} (
                   id INT AUTO_INCREMENT PRIMARY KEY,
                   last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                        {', '.join([f"{col} VARCHAR(64)" for col in normalized_columns])}
                  )"""
    logger.info(sql)   # Log the SQL command to create table
    return sql

def csv_to_sql(csv_filepath):
    '''Converts CSV data into SQLite database tables'''
    config = load_config(CONFIG_PATH)
    
    conn = pymysql.connect(host=config['host'], user=config['user'], password=config['password'], db=DB_NAME)
    cursor = conn.cursor()
    
    table_name = csv_filepath.split('/')[-1].split('.')[0]

    with open(csv_filepath, 'r') as f:
        reader = csv.reader(f)
        columns = next(reader)       # Get the column names from the CSV file
        
    create_sql = create_table_sql(table_name, columns)
    
    try:
        cursor.execute(create_sql)
        conn.commit()   # Commit changes to the database
    except Exception as e:
        print(f"Error creating table {table_name}: {str(e)}")
        return
        
    with open(csv_filepath, 'r') as f:
        reader = csv.reader(f)
        next(reader)   # Skip column names
        
        for row in reader:
            cursor.execute(f"INSERT INTO {DB_NAME}.{table_name} ({', '.join([col for col in columns])}) VALUES ({', '.join(['%s' for _ in columns])})", row)
    
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
         
    csv_to_sql(csv_filepath)
