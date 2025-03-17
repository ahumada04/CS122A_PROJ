from pathlib import Path
from datetime import datetime
import pandas as pd
import os
import platform
import sys
import mysql.connector

# SWITCH TO THIS VERSION WHEN SUBMITING TO GRADESCOPE!!!!!!!!!!!!!!!!!!!!
# db = mysql.connect(user = 'test', password = 'password', database = 'cs122a')

# IF YOU ARE HAVING ISSUES CONNECTING ENTER BELOW INT CMD PROMPT:  
# pip install pymysql
db = mysql.connector.connect(host = "127.0.0.1", port = "3306", user="root", password="1234", database = "cs122a")
dbcursor = db.cursor()
functions = ["import_"]
table_names = ['users', 'producers', 'viewers', 'releases', 'movies', 'series', 'videos', 'sessions', 'reviews']


def main():
    if len(sys.argv) < 3:
        print("Usage: python project.py <FUNCTION> <YOUR DATA>")
        sys.exit(1)
    else:
        func_name = sys.argv[1]
        user_data = sys.argv[2]
        if func_name in globals():
            globals()[func_name](user_data)
        else:
            print("Not a function.")
            print(f"Use these instead: {functions}")


def import_(filepath):
    if os.path.exists(filepath): 
        reset_db()
        # dbcursor.execute("SET GLOBAL local_infile = 1;")
        # dbcursor.execute("SET SESSION local_infile = 1;")
        # db.commit()

        for table in table_names:
            file = table + ".csv"
            abs_path = os.path.join(filepath, file)
            abs_path = os.path.abspath(abs_path)
            abs_path = abs_path.replace('\\', '/')  # MySQL syntax
            # Set correct line terminator
            line_terminator = '\r\n' if platform.system() == "Windows" else '\n'
            dataload = (
                f"LOAD DATA LOCAL INFILE '{abs_path}' "
                f"INTO TABLE {table} "
                "FIELDS TERMINATED BY ',' "
                f"LINES TERMINATED BY '{line_terminator}' "
                "IGNORE 1 ROWS;"
            )
            try:
                # dbcursor.execute("SET SESSION local_infile = 1;")
                # dbcursor.execute("SET GLOBAL local_infile = 1;")
                dbcursor.execute(dataload)
                db.commit()
            except mysql.connector.Error as err:
                print(f"Error loading {file}: {err}")
            # print(dataload)
        return True
    else:
        print("Path does not exist")
        return False


def reset_db():
    with open("database_reset.txt", "r", encoding= "utf-8") as file:
        reset_query = file.read()
    try:
        dbcursor.execute(reset_query)
        db.commit()
    except mysql.connector.Error as err:
        print(f'Error reseting the database: {err}')


def insertViewer(viewerInfo):
    insertQ = f"""" 
    INSERT INTO viewers (uid, firstname, lastame, subscription)" \
    VALUES ({viewerInfo});
    """
    dbcursor.execute(insertQ)
    db.commit


if __name__ == "__main__":
    main()
