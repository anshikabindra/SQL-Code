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
csv_file = r"C:\Users\Anshika\Downloads\anshika-equity-zerodha (1).csv"
try:
    df = pd.read_csv(csv_file)
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
        INSERT INTO EqutityZerodha (Symbol, ISIN, Trade_Date, EXCH, Segment, Series,
         Trade_Type,Auction,Price, Quantity, Trade_ID, Order_ID, Order_Execution_Time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s)
        """

    # Insert data from DataFrame into MySQL table
    for index, row in df.iterrows():
        try:
            cursor.execute(insert_query, (row['Symbol'], row['ISIN'], row['Trade Date'],
                                          row['Exchange'], row['Segment'], row['Series'],
                                          row['Trade Type'],row['Auction'],row['Price'],
                                          row['Quantity'], row['Trade ID'], row['Order ID'],
                                          row['Order Execution Time']))
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
