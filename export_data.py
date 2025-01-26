import pandas as pd
import os
import pyodbc

# Database connection details
db_config = {
    'server': 'YOUR_SERVER',
    'database': 'YOUR_DATABASE',
    'username': 'YOUR_USERNAME',
    'password': 'YOUR_PASSWORD',
    'driver': '{ODBC Driver 17 for SQL Server}'  # Ensure the correct driver is installed
}

# Paths
excel_file = 'path_to_excel_file.xlsx'  # Replace with the actual path to your Excel file
output_base_dir = 'output_folder'  # Replace with the desired base output directory

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

# Function to export table data in chunks
def export_table_to_csv(table_name, conn, output_dir, chunk_size=1000000):
    query = f"SELECT * FROM {table_name}"
    chunk_no = 1
    os.makedirs(output_dir, exist_ok=True)  # Create output directory for the table
    
    for chunk in pd.read_sql(query, conn, chunksize=chunk_size):
        output_path = os.path.join(output_dir, f"{table_name}_chunk{chunk_no}.csv")
        chunk.to_csv(output_path, index=False)
        print(f"Exported {table_name} - Chunk {chunk_no} to {output_path}")
        chunk_no += 1

def main():
    # Read table names from the Excel file
    table_data = pd.read_excel(excel_file, sheet_name=0)  # Assuming first sheet contains table names
    table_names = table_data['TableName']  # Assuming the column is named 'TableName'
    
    # Create database connection
    conn = create_db_connection(db_config)
    
    # Export data for each table
    for table_name in table_names:
        table_output_dir = os.path.join(output_base_dir, table_name)
        export_table_to_csv(table_name, conn, table_output_dir)

    conn.close()
    print("Data export complete.")

if __name__ == "__main__":
    main()
