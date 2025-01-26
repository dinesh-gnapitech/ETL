import pandas as pd
import os
import pyodbc

# Database connection details
db_config = {
    'server': 'YOUR_SERVER',
    'database': 'YOUR_DATABASE',
    'username': 'YOUR_USERNAME',
    'password': 'YOUR_PASSWORD',
    'driver': '{ODBC Driver 17 for SQL Server}'  # Ensure you have this driver installed
}

# Paths
excel_file = 'path_to_excel_file.xlsx'  # Replace with the path to your Excel file
output_base_dir = 'output_folder'  # Replace with the desired base output directory

# Chunk size
chunk_size = 1000000  # Number of records to process per chunk

# Function to create a database connection
def create_db_connection(config):
    conn = pyodbc.connect(
        f"DRIVER={config['driver']};"
        f"SERVER={config['server']};"
        f"DATABASE={config['database']};"
        f"UID={config['username']};"
        f"PWD={config['password']}"
    )
    return conn

# Function to get total record count for a table
def get_record_count(table_name, conn):
    query = f"SELECT COUNT(*) AS TotalRecords FROM {table_name}"
    result = pd.read_sql(query, conn)
    return result['TotalRecords'][0]

# Function to export table data
def export_table_to_csv(table_name, conn, output_dir, chunk_size):
    os.makedirs(output_dir, exist_ok=True)  # Create output directory for the table

    # Get the total number of records
    total_records = get_record_count(table_name, conn)
    print(f"Total records in {table_name}: {total_records}")

    if total_records <= chunk_size:
        # Export the entire table if total records are less than or equal to the chunk size
        query = f"SELECT * FROM {table_name}"
        data = pd.read_sql(query, conn)
        output_path = os.path.join(output_dir, f"{table_name}.csv")
        data.to_csv(output_path, index=False)
        print(f"Exported {table_name} to {output_path}")
    else:
        # Export in chunks if total records exceed the chunk size
        offset = 0
        chunk_no = 1

        while offset < total_records:
            query = f"""
                SELECT * 
                FROM {table_name}
                ORDER BY (SELECT NULL)  -- Use an indexed column if possible for better performance
                OFFSET {offset} ROWS FETCH NEXT {chunk_size} ROWS ONLY
            """
            chunk = pd.read_sql(query, conn)

            # Save the chunk to a CSV file
            output_path = os.path.join(output_dir, f"{table_name}_chunk{chunk_no}.csv")
            chunk.to_csv(output_path, index=False)
            print(f"Exported {table_name} - Chunk {chunk_no} to {output_path}")

            # Update offset and chunk number
            offset += chunk_size
            chunk_no += 1

def main():
    # Read table names from the Excel file
    table_data = pd.read_excel(excel_file, sheet_name=0)  # Assuming the first sheet contains table names
    table_names = table_data['TableName']  # Assuming the column is named 'TableName'

    # Create database connection
    conn = create_db_connection(db_config)

    # Export data for each table
    for table_name in table_names:
        print(f"Starting export for table: {table_name}")
        table_output_dir = os.path.join(output_base_dir, table_name)
        export_table_to_csv(table_name, conn, table_output_dir, chunk_size)

    conn.close()
    print("Data export complete.")

if __name__ == "__main__":
    main()
