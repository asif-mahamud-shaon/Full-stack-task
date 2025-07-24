import os
import pandas as pd # type: ignore

def save_upload_file(upload_file, destination_folder):
    """Save uploaded file to destination folder."""
    os.makedirs(destination_folder, exist_ok=True)
    file_path = os.path.join(destination_folder, upload_file.filename)
    try:
        with open(file_path, 'wb') as buffer:
            buffer.write(upload_file.file.read())
    except Exception as e:
        raise IOError(f"Failed to save file: {e}")
    return file_path

def convert_csv_to_parquet(csv_path, parquet_path):
    """Convert CSV to Parquet and return row count."""
    try:
        df = pd.read_csv(csv_path)
        df.to_parquet(parquet_path, engine='pyarrow')
        return len(df)
    except Exception as e:
        raise IOError(f"Failed to convert CSV to Parquet: {e}")

def get_row_count(csv_path):
    """Get row count of a CSV file."""
    try:
        df = pd.read_csv(csv_path)
        return len(df)
    except Exception as e:
        raise IOError(f"Failed to count rows: {e}") 