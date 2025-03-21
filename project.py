from pathlib import Path
from datetime import datetime
import os
import sys
import mysql.connector
import csv

# SWITCH TO THIS VERSION WHEN SUBMITING TO GRADESCOPE!!!!!!!!!!!!!!!!!!!!
db = mysql.connector.connect(user = 'test', password = 'password', database = 'cs122a')

# IF YOU ARE HAVING ISSUES CONNECTING ENTER BELOW INT CMD PROMPT:  
# pip install pymysql
#db = mysql.connector.connect(host = "127.0.0.1", port = "3306", user="root", password="1234", database = "cs122a")
dbcursor = db.cursor()
functions = ["import", "insertViewer", "addGenre", "listReleases", "popularRelease", "releaseTitle", "activeViewer", "videosViewed", "deleteViewer", "insertSession", "updateRelease", "insertMovie"]
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
    passed = None
    match func_name:
        case "import":
            passed = import_(sys.argv[2])
        case "insertViewer":
            #             [uid:int] [email:str] [nickname:str] [street:str] [city:str] [state:str] [zip:str] [genres:str] [joined_date:date] [first:str] [last:str] [subscription:str]
            passed = insertViewer(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[8], sys.argv[9], sys.argv[10], sys.argv[11],  sys.argv[12], sys.argv[13])
        case "addGenre":
            #         [uid:int] [genres:str]
            passed = addGenre(sys.argv[2], sys.argv[3])
        case "listReleases":
            #       [uid:int]
            passed = listReleases(sys.argv[2])
        case "popularRelease":
            #         [n:int]
            passed = popularRelease(sys.argv[2])
        case "releaseTitle":
            #         [sid:int]
            passed = releaseTitle(sys.argv[2])
        case "activeViewer":
            #         [N:int] [start:date] [end:date]
            passed = activeViewer(sys.argv[2], sys.argv[3], sys.argv[4])
        case "videosViewed":
            #         [rid:int]
            passed = videosViewed(sys.argv[2])
        case "insertMovie":  
            passed = insertMovie(sys.argv[2], sys.argv[3])


    if passed:
        print("Success")
    elif passed == False:
        print("Fail")

def import_(filepath) -> bool:
    if not os.path.exists(filepath):
        print(f"Path {filepath} does not exist")
        return False
    
    try:
        cursor = db.cursor()
        # Reset database 
        with open("database_reset.txt", "r", encoding="utf-8") as file:
            reset_script = file.read()
        try:
            for statement in reset_script.split(";"):  # Splitting statements
                statement = statement.strip()
                if statement:  # Ignore empty statements
                    dbcursor.execute(statement)  # Execute each statement
            db.commit()
        except mysql.connector.Error as err:
            print(f"Error executing reset statement: {err}")
        
        for table in table_names:
            csv_file = os.path.join(filepath, f"{table}.csv")
            with open(csv_file, 'r') as file:
                reader = csv.reader(file)
                headers = next(reader)  # Get column names from first row
                headers_str = ', '.join(headers)
                
                for row in reader:
                    values = []
                    for (column, value) in zip(headers, row):
                        if column.endswith('_id'):
                            values.append(value)                            
                        else:
                            values.append(f"\'{value}\'")

                    values_str = ', '.join(values)
                    insert_query = f"INSERT INTO {table} ({headers_str}) VALUES ({values_str});"

                    try:
                        cursor.execute(insert_query)
                        db.commit()
                    except mysql.connector.Error as err:
                        print(f"Error inserting into {table}: {err}")
                        print(f"Failed query: {insert_query}")
        # cursor.close()
        return True
    except Exception as e:
        print(f"Unexpected error during import: {e}")
        return False
    

#EXAMPLE:         1 test@uci.edu awong "1111 1st street" Irvine CA 92616 "romance;comedy" 2020-04-19 Alice Wong yearly
def insertViewer(uid, email, nickname, address, city, state, zip, genres, joined, first, last, sub) -> bool:
    # TODO
    # NEED TO UPDATE WITH NULL HANDLING
    userQ = f"""
    INSERT INTO users (uid, email, nickname, street, city, state, zip, genres, joined_date) \
    VALUES ({uid}, "{email}", "{nickname}","{address}", "{city}", "{state}", "{zip}", "{genres}", "{joined}");
    """
    viewerQ = f"""
    INSERT INTO viewers (uid, first_name, last_name, subscription) \
    VALUES ({uid}, "{first}", "{last}", "{sub}");
    """
    try:
        dbcursor.execute(userQ)
        dbcursor.execute(viewerQ)
        db.commit()
    except mysql.connector.Error as err:
        # print(f'Error inserting into users/viewers tables: {err}')
        return False
    
    return True


def addGenre(uid, genre) -> bool:
    try:
        grabQ = f"""
        SELECT genres 
        FROM users
        WHERE uid = {uid}
        """
        dbcursor.execute(grabQ)
        currGenres = dbcursor.fetchall()
        if currGenres:
            newGenres = genre.split(';')
            currGenres = currGenres[0][0].split(';')
        else:
            # print("uid not found.")
            return False
    except mysql.connector.Error as err:
        # print(f'Unexpected Error: {err}')
        return False
    
    updatedGenres = set(newGenres) | set(currGenres)
    deliminator = ";"

    if updatedGenres == set(currGenres):
        return False
    
    updated = deliminator.join(updatedGenres)
    updateQ = f"""
    UPDATE users 
    SET genres = "{updated}"
    WHERE uid = {uid};
    """
    
    dbcursor.execute(updateQ)
    db.commit()
    return True




# translates a list of items to MYSQL syntax
# def SQLtranslate(dataList):
#     for i, item in enumerate(dataList):
#         print(item)
#         if(item) == 'NULL':
#             dataList[i] = None
#         else:
            
#     return dataList


#4- Delete viewer
def deleteViewer(uid: int) -> bool:
    try:
        # delete_viewer = f"DELETE FROM viewers WHERE uid = {uid};"
        # delete_user = f"DELETE FROM users WHERE uid = {uid};"
        # dbcursor.execute(delete_viewer)
        # dbcursor.execute(delete_user)

        dbcursor.execute(f"DELETE FROM sessions WHERE uid = {uid};")
        dbcursor.execute(f"DELETE FROM reviews WHERE uid = {uid};")

        dbcursor.execute(f"DELETE FROM viewers WHERE uid = {uid};")
        dbcursor.execute(f"DELETE FROM users WHERE uid = {uid};")
        

        db.commit()
        return True
    except mysql.connector.Error as err:
        print(f"Database Error: {err}")
        return False

#5 - insert
def insertMovie(rid: int, website_url: str) -> bool:
    try:
        insert_movie = f"INSERT INTO movies (rid, website_url) VALUES ({rid}, '{website_url}');"
        dbcursor.execute(insert_movie)
        db.commit()
        return True
    except mysql.connector.Error as err:
        return False

#6 - insert session
def insertSession(sid: int, uid: int, rid: int, ep_num: int, initiate_at: str, leave_at: str, quality: str, device: str) -> bool:
    try:
        insert_session = f"""
        INSERT INTO sessions (sid, uid, rid, ep_num, initiate_at, leave_at, quality, device)
        VALUES ({sid}, {uid}, {rid}, {ep_num}, '{initiate_at}', '{leave_at}', '{quality}', '{device}');
        """
        dbcursor.execute(insert_session)
        db.commit()
        return True
    except mysql.connector.Error as err:
        return False

#7 - update release
def updateRelease(rid: int, title:str) -> bool:
    try:
        update_query = f"UPDATE releases SET title = '{title}' WHERE rid = {rid};"
        dbcursor.execute(update_query)
        db.commit()
        return True
    except mysql.connector.Error as err:
        return False



def listReleases(uid):
    '''
    Question 8: given a viewer id, list all the unique releases the viewer has reviewed in ASC order on release title.
    strategy: tables; need Reviews and Releases.
        grab all reviews by the given vid, and replace the rid with the release title. sort asc.
    input: python3 project.py listReleases [uid:int]
    example: python3 project.py listReleases 1
    output: Table - rid, genre, title
    '''
    try:
        grabQ = f"""
        SELECT DISTINCT rid, genre, title
        FROM releases
        WHERE rid IN (SELECT rid 
        FROM reviews
        WHERE uid = {uid})
        ORDER BY title ASC
        """
        dbcursor.execute(grabQ)
        currTitles = dbcursor.fetchall()
        if currTitles:
            tablePrinter(currTitles)
        else:
            # print("uid not found.")
            return False
    except mysql.connector.Error as err:
        # print(f'Unexpected Error: {err}')
        return False

def popularRelease(N):
    '''
    Question 9: List the top N releases that have the most reviews, in DESCENDING order on reviewCount, rid
    strategy: tables; need Reviews and Releases.
        grab rid,title and
        create a reviewCount var (the number of times a release has been reviewed)

    input: python3 project.py popularRelease [N: int]
    	EXAMPLE: python3 project.py popularRelease 10
    output: Table - rid, title, reviewCount
    '''

    N = int(N)
    try:
        grabQ = f"""
        SELECT r.rid, r.title, CAST(COUNT(rv.rid) AS CHAR) AS review_count
        FROM releases r
        LEFT JOIN reviews rv ON r.rid = rv.rid
        GROUP BY r.rid
        ORDER BY review_count DESC;
        """
        dbcursor.execute(grabQ)
        currTitles = dbcursor.fetchall()
        if currTitles:
            tempString = ","
            for row in currTitles:
                if N > 0:
                    print(tempString.join(row))
                    N = N - 1

    except mysql.connector.Error as err:
        #print(f'Unexpected Error: {err}')
        return False

def releaseTitle(sid):
    '''
    Question 10: Given a session ID, find the release associated with the
    video streamed in the session.
    List information on both the release and video,
    in ASCENDING order on release title.

    strategy: tables = sessions, releases, videos

    input: python3 project.py releaseTitle [sid: int]
	EXAMPLE: python3 project.py releaseTitle 123
    output: Table - rid, release_title, genre, video_title, ep_num, length
    '''
    try:
        grabQ = f"""
        SELECT CAST(r.rid AS CHAR) AS rid, r.title AS release_title, r.genre, v.title AS video_title, CAST(s.ep_num AS CHAR) AS ep_num, CAST(v.length AS CHAR) AS length
            FROM     sessions s
            JOIN     releases r ON s.rid = r.rid
            JOIN     videos v ON s.rid = v.rid AND s.ep_num = v.ep_num
            WHERE    s.sid = {sid}
            ORDER BY r.title ASC;
        """
        dbcursor.execute(grabQ)
        currTitles = dbcursor.fetchall()
        if currTitles:
            tablePrinter(currTitles)
        else:
            # print("uid not found.")
            return False
    except mysql.connector.Error as err:
        # print(f'Unexpected Error: {err}')
        return False

def activeViewer(N, start, end):
    '''
    Question 11: Find all active viewers that have started a session more than N times (including N) in a specific time range (including start and end date), in ASCENDING order by uid. N will be at least 1.
    strategy: tables;
    input: python3 project.py activeViewer [N:int] [start:date] [end:date]
	EXAMPLE: python3 project.py activeViewer 5 2023-01-09 2023-03-10
    output: Table - UID, first name, last name
    '''


    start += " 00:00:00"
    end += " 00:00:00"

    start = f'"{start}"'
    end = f'"{end}"'

    try:
        grabQ = f"""
        SELECT CAST(v.uid AS CHAR) AS uid, v.first_name, v.last_name
        FROM     sessions s
        JOIN     viewers v ON s.uid = v.uid
        WHERE    s.initiate_at BETWEEN {start} AND {end}
        GROUP BY     v.uid, v.first_name, v.last_name
        HAVING     COUNT(s.sid) >= {N}
        ORDER BY     v.uid ASC;
        """

        dbcursor.execute(grabQ)
        currTitles = dbcursor.fetchall()
        if currTitles:
            tablePrinter(currTitles)
        else:
            # print("uid not found.")
            return False
    except mysql.connector.Error as err:
        print(f'Unexpected Error: {err}')
        return False

def videosViewed(rid):
    '''
    Question 12: Given a Video rid, count the number of unique viewers that have
    started a session on it. Videos that are not streamed by any viewer should
    have a count of 0 instead of NULL. Return video information along with the
    count in DESCENDING order by rid.

    strategy: tables = sessions, releases, videos

    input: python3 project.py videosViewed [rid: int]
    EXAMPLE: python3 project.py videosViewed 123

    output: Table -
    '''
    try:
        grabQ = f"""
        SELECT  CAST(v.rid AS CHAR) AS rid, 
                CAST(v.ep_num AS CHAR) AS ep_num, 
                v.title, 
                CAST(v.length AS CHAR) AS length, 
                CAST(COALESCE(COUNT(DISTINCT s.uid), 0) AS CHAR) AS viewer_count
        FROM        videos v
        LEFT JOIN   sessions s ON v.rid = s.rid AND v.ep_num = s.ep_num
        WHERE   v.rid = {rid}
        GROUP BY    v.rid, v.ep_num, v.title, v.length
        ORDER BY     v.rid DESC;
        """
        dbcursor.execute(grabQ)
        currTitles = dbcursor.fetchall()
        if currTitles:
            tablePrinter(currTitles)
        else:
            # print("uid not found.")
            return False
    except mysql.connector.Error as err:
        # print(f'Unexpected Error: {err}')
        return False

def tablePrinter(table):
    '''table printing helper function.
     given table (which is a list)
     go through the list, printing the tuples within separated by a single comma.
     newline when done with a tuple.
     '''
    tempString = ","
    for row in table:
        print(tempString.join(row))

if __name__ == "__main__":
    main()
