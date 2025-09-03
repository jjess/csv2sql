import sys
import getopt
from auto_csv2sql import csv_to_sql

def usage():
    '''Prints the usage of this script'''
    print("Usage: python3 csv2sql.py --csvfile <csv_file> [--dbtable <db_table>]")

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
