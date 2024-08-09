import pandas as pd
import mysql.connector
from mysql.connector import Error
# Configuration for MySQL connection
db_config = {
    'user': 'root',
    'password': 'Anshika1@34',
    'host': '127.0.0.1',
    'port' : '3306',
    'database': 'portfolioManagement'
}

# Load CSV data into a DataFrame
csv_file = r"C:\Users\Anshika\Downloads\anshika-axis-statemetn1.csv"
try:
    df = pd.read_csv(csv_file)
    #df[['DR','CR']] = df[['DR','CR']].fillna('')
    #print(df)
    print("CSV file loaded successfully.")

    # Check the DataFrame's columns
    print("Columns in CSV file:", df.columns)

except FileNotFoundError:
    print(f"Error: The file {csv_file} was not found.")
    exit(1)
except pd.errors.EmptyDataError:
    print("Error: The CSV file is empty.")
    exit(1)
except pd.errors.ParserError:
    print("Error: Error parsing the CSV file.")
    exit(1)

# Establish a connection to the MySQL database
try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    print("Database connection established.")

    # Define the insert query
    insert_query = """
         INSERT INTO axis (SRL_NO,Tran_Date,CHQNO,Particulars,DR,CR,BAL,SOL)
        VALUES (%s, str_to_date(%s,'%Y-%d-%m'),%s,%s,%s,%s,%s,%s)
        """

    # Insert data from DataFrame into MySQL table
    for index, row in df.iterrows():
        try:
            cursor.execute(insert_query, (row['SRL NO'], row['Tran Date'],
                                          row['CHQNO'], row['PARTICULARS'], row['DR'],
                                          row['CR'], row['BAL'], row['SOL']))
        except KeyError as e:
            print(f"Error: Missing column {e} in the CSV file.")
            exit(1)
        except Error as e:
            print(f"Database error: {e}")
            conn.rollback()
            exit(1)

    # Commit the transaction
    conn.commit()
    print("Data inserted successfully.")

except Error as e:
    print(f"Error: {e}")
finally:
    # Close the connection
    if cursor:
        cursor.close()
    if conn:
        conn.close()
