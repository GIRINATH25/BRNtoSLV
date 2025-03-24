from db.db_connector import DBConnector
import pandas as pd
import gc
import os

# Database connection
db = DBConnector()
engine = db.get_engine("staging")

# File settings
INPUT_EXCEL = "sample.xls"
CHUNK_SIZE = 10  # Process 50 rows at a time
SORT_COLUMN = "Id"  # Sorting column

def process_large_excel(input_excel, sort_column, chunk_size):
    # Step 1: Create a folder named after the Excel file
    folder_name = os.path.splitext(os.path.basename(input_excel))[0]
    os.makedirs(folder_name, exist_ok=True)
    print(f"Folder Created: {folder_name}")

    # Step 2: Read and process Excel in chunks without preloading row count
    print("Processing Excel in chunks...")
    
    start_row = 0
    while True:
        # Read next chunk
        chunk = pd.read_excel(input_excel, skiprows=range(1, start_row), nrows=chunk_size, header=0)
        
        # Stop if no more data
        if chunk.empty:
            break
        
        # Ensure column names are strings
        chunk.columns = chunk.columns.astype(str)

        # Sort chunk
        chunk = chunk.sort_values(by=sort_column)

        # Save chunk as Parquet with a filename corresponding to its starting row number
        output_path = os.path.join(folder_name, f"{start_row}.parquet")
        chunk.to_parquet(output_path, engine="fastparquet", compression="snappy", index=False)

        print(f"Chunk saved: {output_path}")

        # Free memory
        del chunk
        gc.collect()

        # Increment start row for next chunk
        start_row += chunk_size

    print("Process Completed!")

# Run the function
process_large_excel(INPUT_EXCEL, SORT_COLUMN, CHUNK_SIZE)
