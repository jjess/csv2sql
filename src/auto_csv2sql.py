import pymysql
import yaml
from datetime import datetime
import logging
import csv
from typing import List

def load_config(config_path):
    '''Loads database configuration from a YAML file'''
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config['mysql_connection']

def normalize_columns(columns: List[str]) -> List[str]:
    '''Normalizes column names to lower case and replaces spaces with underscores'''
    return [col.lower().replace(' ', '_') for col in columns]

def create_table_sql(table_name: str, columns: List[str]) -> str:
    '''Creates SQL code for creating a table based on the given columns'''
    normalized_columns = normalize_columns(columns)
    nl=",\n        "
    sql = f"""
    DROP TABLE IF EXISTS {table_name}; 
    CREATE TABLE {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        last_update TIMESTAMP NULL DEFAULT NOW() ON UPDATE NOW() ,
        {f"{nl}".join([f"{col} VARCHAR(64)" for col in normalized_columns])}
    );"""
    return sql

def csv_to_sql(csv_filepath: str) -> None:
    '''Converts CSV data into MySQL database tables'''
    config = load_config('src/etc/config.yml')

    db_name = config['database']

    conn = pymysql.connect(host=config['host'], 
                          user=config['user'], 
                          password=config['password'], 
                          db=db_name,
                          local_infile=True)
    cursor = conn.cursor()

    table_name = csv_filepath.split('/')[-1].split('.')[0]

    with open(csv_filepath, 'r') as f:
        reader = csv.reader(f)
        columns = next(reader)    

    create_sql = create_table_sql(table_name, columns)

    header = str(',').join(columns) 
    print(f"columns in file: [{ header }]" )
    print(f"create table command: [ {create_sql} ]")

    try:
        for statement in create_sql.split(';'):    
            if not statement.strip():
                continue

            cursor.execute(statement)     
        conn.commit()     
    except Exception as e:
        print(f"Error creating table {table_name}: {str(e)}")
        conn.close()
        return

    # Execute LOAD DATA INFILE once to load the entire file
    try:
        sql = f"""
        LOAD DATA LOCAL INFILE '{csv_filepath}' 
        INTO TABLE {table_name} 
        FIELDS TERMINATED BY ',' 
        ENCLOSED BY '"' 
        LINES TERMINATED BY '\n' 
        IGNORE 1 ROWS
        (header)
        SET id=NULL, last_update=CURRENT_TIMESTAMP ;
        """
        cursor.execute(sql)         
        conn.commit()
        
        # Get the number of rows inserted
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        line_count = cursor.fetchone()[0]
        
        print(f"Added {line_count} rows to table {table_name}.")
    except Exception as e:
        print(f"Error loading data into table {table_name}: {str(e)}")
    finally:
        conn.close()

    return
