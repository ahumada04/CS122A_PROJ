from pathlib import Path
from datetime import datetime
import pandas as pd
import os
import csv
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
        print("Usage: python <FUNCTION> <YOUR DATA>")
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

        # file = os.path.join(filepath, "load_data_instructions.txt")
        # with open(file, "r", encoding="utf-8") as f:

    else:
        print("Path does not exist")


# Reversed due to foreign key dependencies
def reset_db():
    with open("database_reset.txt", "r", encoding= "utf-8") as file:
        reset_query = file.read()
    dbcursor.execute(reset_query)








if __name__ == "__main__":
    main()
