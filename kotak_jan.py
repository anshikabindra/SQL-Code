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
csv_file = r"C:\Users\Anshika\Downloads\anshika kotak-jan 2.csv"
try:
    df = pd.read_csv(csv_file)
    #df['Amount'].str.replace(',','')
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
         INSERT INTO kotak_jan (Sl_No,Transaction_Date,Value_Date,Description,Chq_Ref_No,Amount,DR_CR,Balance,D_C)
        VALUES (%s,str_to_date(%s,'%d/%m/%Y'),str_to_date(%s,'%d/%m/%Y'),%s,%s,%s,%s,%s,%s)
        """
    # Insert data from DataFrame into MySQL table
    for index, row in df.iterrows():
        try:
            cursor.execute(insert_query, (row['Sl. No. '], row['Transaction Date'],
                                          row['Value Date'], row['Description'], row['Chq / Ref No.'],
                                          row['Amount'].replace(',',''), row['Dr / Cr'],row['Balance'].replace(',',''),row['Dr / Cr']))
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
