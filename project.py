from pathlib import Path
from datetime import datetime
import os
import sys
import mysql.connector

# SWITCH TO THIS VERSION WHEN SUBMITING TO GRADESCOPE!!!!!!!!!!!!!!!!!!!!
db = mysql.connector.connect(user = 'test', password = 'password', database = 'cs122a', allow_local_infile=True)

# IF YOU ARE HAVING ISSUES CONNECTING ENTER BELOW INT CMD PROMPT:  
# pip install pymysql
# db = mysql.connector.connect(host = "127.0.0.1", port = "3306", user="root", password="1234", database = "cs122a", allow_local_infile = True)
dbcursor = db.cursor()
functions = ["import", "insertViewer", "addGenre"]
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
        case "import":
            import_(sys.argv[2])
        case "insertViewer":
            #             [uid:int] [email:str] [nickname:str] [street:str] [city:str] [state:str] [zip:str] [genres:str] [joined_date:date] [first:str] [last:str] [subscription:str]
            insertViewer(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[8], sys.argv[9], sys.argv[10], sys.argv[11],  sys.argv[12], sys.argv[13])
        case "addGenre":
            #         [uid:int] [genres:str]
            addGenre(sys.argv[2], sys.argv[3])

def import_(filepath):
    if not os.path.exists(filepath):
        print(f"Path {filepath} does not exist")
        return False
    try:
        # db = mysql.connector.connect(
        #     host="127.0.0.1", 
        #     port="3306", 
        #     user="root", 
        #     password="1234",
        #     allow_local_infile=True
        # )
        c2 = db.cursor()
        
        # Reset database first - execute without selecting a database
        with open("database_reset.txt", "r", encoding="utf-8") as file:
            reset_script = file.read()
        
        # Split and execute statements individually (handles multiple statements)
        for statement in reset_script.split(';'):
            if statement.strip():
                try:
                    c2.execute(statement)
                    db.commit()
                except mysql.connector.Error as err:
                    print(f"Error executing reset statement: {err}")
                    print(f"Statement: {statement}")
                
        for table in table_names:
            csv_file = os.path.join(filepath, f"{table}.csv")
            abs_path = os.path.abspath(csv_file)
            if not os.path.exists(abs_path):
                print(f"Warning: File {abs_path} does not exist, skipping.")
                continue
            abs_path = abs_path.replace('\\', '/') # MySQL format
            try:
                # Try with Unix-style line endings first
                load_query = f"""
                    LOAD DATA LOCAL INFILE '{abs_path}' 
                    INTO TABLE {table} 
                    FIELDS TERMINATED BY ',' 
                    OPTIONALLY ENCLOSED BY '"'
                    LINES TERMINATED BY '\\n' 
                    IGNORE 1 ROWS;
                """
                c2.execute(load_query)
                db.commit()
                print(f"Successfully imported {table}.csv")
            except mysql.connector.Error as err:
                print(f"Error loading {table}.csv with Unix endings: {err}")
                # try:
                #     # Try with Windows-style line endings
                #     load_query = f"""
                #         LOAD DATA LOCAL INFILE '{abs_path}' 
                #         INTO TABLE {table} 
                #         FIELDS TERMINATED BY ',' 
                #         OPTIONALLY ENCLOSED BY '"'
                #         LINES TERMINATED BY '\\r\\n' 
                #         IGNORE 1 ROWS;
                #     """
                #     cursor.execute(load_query)
                #     connection.commit()
                #     print(f"Successfully imported {table}.csv with Windows line endings")
                # except mysql.connector.Error as second_err:
                #     print(f"Also failed with Windows line endings: {second_err}")
        
        c2.close()
        db.close()
        return True
    
    except Exception as e:
        print(f"Unexpected error during import: {e}")
        return False
    

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
