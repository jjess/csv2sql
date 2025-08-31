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
LOGFILE = "log/csv2sql.log"   # Log file location

# Set up logging to a file
#logging.basicConfig(level=logging.INFO, format='%(asctime)s  %(message)s', filename=LOGFILE, filemode='w')
#logger = logging.getLogger()

# Set up logging to both file and standard output                                                                                                 
level    = logging.INFO
format   = '  %(message)s'
handlers = [logging.FileHandler('filename.log'), logging.StreamHandler()]

logging.basicConfig(level = level, format = format, handlers = handlers)
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
    nl=",\n        "
    sql = f"""
    DROP TABLE IF EXISTS {table_name}; 
    CREATE TABLE {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        {f"{nl}".join([f"{col} VARCHAR(64)" for col in normalized_columns])}
       );"""
    logger.info(sql)   # Log the SQL command to create table
    return sql

def csv_to_sql(csv_filepath):
    '''Converts CSV data into SQLite database tables'''
    logger.info(f"   ---> reading config.yml ...")   # Log successful addition of data                                        
    config = load_config(CONFIG_PATH)

    db_name = config['database']   # Get the database name from the configuration                                                                 

    conn = pymysql.connect(host=config['host'], user=config['user'], password=config['password'], db=db_name)
    cursor = conn.cursor()
    
    table_name = csv_filepath.split('/')[-1].split('.')[0]

    logger.info(f"   ---> reading ${csv_filepath} to build the table ... ")   # Log successful addition of data                                        
    with open(csv_filepath, 'r') as f:
        reader = csv.reader(f)
        columns = next(reader)       # Get the column names from the CSV file
        
    create_sql = create_table_sql(table_name, columns)
    
    logger.info(f"   ---> dropping/creating table {table_name} ... ")   # Log successful addition of data                                        
    try:
        for statement in create_sql.split(';'):    # Split on ‘;’ which is SQL command separator                                                
            if not statement.strip():   # Check if the statement is empty or contains only spaces/tabs
                continue

            cursor.execute(statement)     # Execute each part separately                                                                          
        conn.commit()     # Commit changes to the database                     
    except Exception as e:
        print(f"   ---> error creating table {table_name}: {str(e)}")
        logger.error(f"   ---> failed to create table {table_name}. Error: {str(e)}")   # Log error when creating the table fails                         
        return

    logger.info(f"   ---> reading ${csv_filepath} to insert data ... ")   # Log successful addition of data                                        
    with open(csv_filepath, 'r') as f:
        reader = csv.reader(f)
        next(reader)   # Skip column names
        line_count = 0

        for row in reader:
            if not all(row):   # Check if the row is empty or contains only spaces/tabs                                                           
                continue                                                                                                                          

            cursor.execute(f"INSERT INTO {table_name} ({', '.join([col for col in columns])}) VALUES ({', '.join(['%s' for _ in columns])})",
                           row)
            line_count += 1
    
    conn.commit()   # Commit changes to the database
    logger.info(f"   ----> Added {line_count} rows to table {table_name}.")   # Log successful addition of data                                        
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
