import sys
import mysql.connector

# SWITCH TO THIS VERSION WHEN SUBMITING TO GRADESCOPE!!!!!!!!!!!!!!!!!!!!
# db = mysql.connect(user = 'test', password = 'password', database = 'cs122a')

# IF YOU ARE HAVING ISSUES CONNECTING ENTER BELOW INT CMD PROMPT:  
# pip install pymysql
db = mysql.connector.connect(host = "127.0.0.1", port = "3306", user="root", password="1234", database = "cs122a")
functions = ["import_"]

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


def import_(user_data):
    db
    print("this is being reached.")


if __name__ == "__main__":
    main()
