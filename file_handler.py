import csv
from typing import List, Dict, Any, Optional
from clickhouse_driver import Client
import os

def get_file_schema(file_path: str, delimiter: str = ",", has_header: bool = True) -> List[str]:
    """Extract column names from a flat file."""
    try:
        with open(file_path, 'r', newline='') as csvfile:
            # Create CSV reader
            reader = csv.reader(csvfile, delimiter=delimiter)
            
            if has_header:
                # First row contains headers
                headers = next(reader)
                return headers
            else:
                # No headers, generate column names
                first_row = next(reader)
                return [f"col_{i}" for i in range(len(first_row))]
    except Exception as e:
        raise Exception(f"Failed to read file schema: {str(e)}")
    finally:
        # Clean up temp file if needed
        pass

def preview_data(
    file_path: str, 
    delimiter: str = ",", 
    has_header: bool = True,
    selected_columns: Optional[List[str]] = None,
    limit: int = 100
) -> List[Dict[str, Any]]:
    """Preview data from a flat file."""
    try:
        with open(file_path, 'r', newline='') as csvfile:
            # Create CSV reader
            reader = csv.reader(csvfile, delimiter=delimiter)
            
            # Read headers
            headers = next(reader) if has_header else [f"col_{i}" for i in range(len(next(reader)))]
            
            # Reset file pointer if no header
            if not has_header:
                csvfile.seek(0)
            
            # Filter columns if specified
            column_indices = None
            if selected_columns:
                column_indices = [headers.index(col) for col in selected_columns if col in headers]
                filtered_headers = [headers[i] for i in column_indices]
            else:
                filtered_headers = headers
            
            # Read data
            results = []
            row_count = 0
            
            for row in reader:
                if row_count >= limit:
                    break
                
                if column_indices:
                    filtered_row = [row[i] for i in column_indices]
                    row_dict = {filtered_headers[i]: filtered_row[i] for i in range(len(filtered_row))}
                else:
                    row_dict = {headers[i]: row[i] for i in range(len(row))}
                
                results.append(row_dict)
                row_count += 1
            
            return results
    except Exception as e:
        raise Exception(f"Failed to preview file data: {str(e)}")

def import_to_clickhouse(
    file_path: str,
    client: Client,
    target_table: str,
    selected_columns: Optional[List[str]] = None,
    delimiter: str = ",",
    has_header: bool = True,
    batch_size: int = 10000
) -> int:
    """Import data from a flat file to ClickHouse."""
    try:
        # Get file schema
        with open(file_path, 'r', newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=delimiter)
            
            # Read headers
            if has_header:
                headers = next(reader)
            else:
                # Read first row to determine number of columns
                first_row = next(reader)
                headers = [f"col_{i}" for i in range(len(first_row))]
                # Reset file pointer
                csvfile.seek(0)
            
            # Filter columns if specified
            if selected_columns:
                column_indices = [headers.index(col) for col in selected_columns if col in headers]
                column_names = [headers[i] for i in column_indices]
            else:
                column_indices = list(range(len(headers)))
                column_names = headers
        
        # Check if table exists and create if needed
        table_exists = False
        try:
            client.execute(f"SELECT 1 FROM {target_table} LIMIT 1")
            table_exists = True
        except:
            pass
        
        if not table_exists:
            # Create table with columns based on file
            column_defs = ", ".join([f"`{col}` String" for col in column_names])
            create_query = f"CREATE TABLE {target_table} ({column_defs}) ENGINE = MergeTree() ORDER BY tuple()"
            client.execute(create_query)
        
        # Read and insert data in batches
        total_rows = 0
        batch = []
        
        with open(file_path, 'r', newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=delimiter)
            
            # Skip header if exists
            if has_header:
                next(reader)
            
            for row in reader:
                if column_indices:
                    filtered_row = [row[i] for i in column_indices]
                else:
                    filtered_row = row
                
                batch.append(filtered_row)
                
                if len(batch) >= batch_size:
                    # Insert batch
                    column_clause = ", ".join([f"`{col}`" for col in column_names])
                    client.execute(f"INSERT INTO {target_table} ({column_clause}) VALUES", batch)
                    total_rows += len(batch)
                    batch = []
            
            # Insert remaining rows
            if batch:
                column_clause = ", ".join([f"`{col}`" for col in column_names])
                client.execute(f"INSERT INTO {target_table} ({column_clause}) VALUES", batch)
                total_rows += len(batch)
        
        return total_rows
    except Exception as e:
        raise Exception(f"Import to ClickHouse failed: {str(e)}")