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
functions = ["import_", "insertViewer", "addGenre"]
table_names = ['users', 'producers', 'viewers', 'releases', 'movies', 'series', 'videos', 'sessions', 'reviews']


def main():
    if len(sys.argv) < 3:
        print("Usage: python project.py <FUNCTION> <YOUR DATA>")
        sys.exit(1)
    else:
        func_name = sys.argv[1]
        if func_name in functions:
            select_function(func_name)
        else:
            print("Not a function.")
            print(f"Use these instead: {functions}")


def select_function(func_name):
    match func_name:
        case "import_":
            import_(sys.argv[2])
        case "insertViewer":
            #             [uid:int] [email:str] [nickname:str] [street:str] [city:str] [state:str] [zip:str] [genres:str] [joined_date:date] [first:str] [last:str] [subscription:str]
            insertViewer(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[8], sys.argv[9], sys.argv[10], sys.argv[11],  sys.argv[12], sys.argv[13])
        case "addGenre":
            #         [uid:int] [genres:str]
            addGenre(sys.argv[2], sys.argv[3])

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
            dataload = f"""
                LOAD DATA LOCAL INFILE '{abs_path}' 
                INTO TABLE {table} 
                FIELDS TERMINATED BY ',' 
                LINES TERMINATED BY '{line_terminator}' 
                IGNORE 1 ROWS;
            """
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


#EXAMPLE:         1 test@uci.edu awong "1111 1st street" Irvine CA 92616 "romance;comedy" 2020-04-19 Alice Wong yearly
def insertViewer(uid, email, nickname, address, city, state, zip, genres, joined, first, last, sub):
    # TODO
    # NEED TO UPDATE WITH NULL HANDLING
    userQ = f"""
    INSERT INTO users (uid, email, nickname, street, city, state, zip, genres, joined_date) \
    VALUES ({uid}, "{email}", "{nickname}","{address}", "{city}", "{state}", "{zip}", "{genres}", "{joined}");
    """
    viewerQ = f"""
    INSERT INTO viewers (uid, firstname, lastname, subscription) \
    VALUES ({uid}, "{first}", "{last}", "{sub}");
    """
    try:
        dbcursor.execute(userQ)
        dbcursor.execute(viewerQ)
        db.commit()
    except mysql.connector.Error as err:
        print(f'Error inserting into users/viewers tables: {err}')
        return False
    
    return True


def addGenre(uid, genre):
    grabQ = f"""
    SELECT genres 
    FROM users
    WHERE uid = {uid}
    """

    try:
        dbcursor.execute(grabQ)
        currGenres = dbcursor.fetchall()
        if currGenres:
            newGenres = genre.split(';')
            currGenres = currGenres[0][0].split(';')
        else:
            print("uid not found.")
            return False
    except mysql.connector.Error as err:
        print(f'Unexpected Error: {err}')
        return False
    
    updatedGenres = set(newGenres) | set(currGenres)
    deliminator = ";"
    updated = deliminator.join(updatedGenres)
    # TODO
    # ADD LOGIC TO STOP IF GENRES IS SAME AS OLD GENRES

    updateQ = f"""
    UPDATE users 
    SET genres = "{updated}"
    WHERE uid = {uid};
    """
    
    dbcursor.execute(updateQ)
    db.commit()




# translates a list of items to MYSQL syntax
# def SQLtranslate(dataList):
#     for i, item in enumerate(dataList):
#         print(item)
#         if(item) == 'NULL':
#             dataList[i] = None
#         else:
            
#     return dataList


if __name__ == "__main__":
    main()
