from clickhouse_driver import Client
from typing import List, Dict, Any, Optional
import csv

def create_client(host: str, port: int, database: str, user: str, jwt_token: str):
    """Create a ClickHouse client connection."""
    try:
        # Use JWT token for authentication
        client = Client(
            host=host,
            port=port,
            database=database,
            user=user,
            password=None,  # Not needed with JWT
            secure=True if port in [9440, 8443] else False,
            verify=False,  # For development only, enable in production
            settings={'use_client_time_zone': True},
            # Pass JWT as connection parameter
            connection_kwargs={'jwt': jwt_token}
        )
        
        # Test connection
        client.execute("SELECT 1")
        return client
    except Exception as e:
        raise Exception(f"ClickHouse connection failed: {str(e)}")

def get_tables(client: Client) -> List[str]:
    """Get list of tables from the connected ClickHouse database."""
    try:
        query = "SHOW TABLES"
        result = client.execute(query)
        return [table[0] for table in result]
    except Exception as e:
        raise Exception(f"Failed to retrieve tables: {str(e)}")

def get_columns(client: Client, table_name: str) -> List[Dict[str, str]]:
    """Get columns for a specific table."""
    try:
        query = f"DESCRIBE TABLE {table_name}"
        result = client.execute(query)
        return [{"name": col[0], "type": col[1]} for col in result]
    except Exception as e:
        raise Exception(f"Failed to retrieve columns for {table_name}: {str(e)}")

def build_query(
    tables: List[str], 
    columns_map: Dict[str, List[str]], 
    join_config: Optional[Dict[str, Any]] = None,
    limit: Optional[int] = None
) -> str:
    """Build a SQL query based on selected tables and columns."""
    # Simple case - single table
    if len(tables) == 1:
        table = tables[0]
        columns = columns_map.get(table, [])
        
        if not columns:
            column_clause = "*"
        else:
            column_clause = ", ".join([f"{table}.{col}" for col in columns])
        
        query = f"SELECT {column_clause} FROM {table}"
        
        if limit:
            query += f" LIMIT {limit}"
            
        return query
    
    # Multi-table case with joins
    if join_config:
        primary_table = tables[0]
        primary_columns = columns_map.get(primary_table, [])
        
        if not primary_columns:
            column_clause = f"{primary_table}.*"
        else:  
            column_clause = ", ".join([f"{primary_table}.{col}" for col in primary_columns])
        
        query = f"SELECT {column_clause}"
        
        # Add columns from other tables
        for table in tables[1:]:
            table_columns = columns_map.get(table, [])
            if table_columns:
                column_clause = ", ".join([f"{table}.{col}" for col in table_columns])
                query += f", {column_clause}"
        
        # Add FROM and JOINs
        query += f" FROM {primary_table}"
        
        # Add join conditions
        for i, table in enumerate(tables[1:]):
            join_type = join_config.get("join_type", "INNER JOIN")
            join_conditions = join_config.get("conditions", [])
            
            if i < len(join_conditions):
                condition = join_conditions[i]
                query += f" {join_type} {table} ON {condition}"
        
        if limit:
            query += f" LIMIT {limit}"
            
        return query
    
    raise Exception("Multiple tables selected but no join configuration provided")

def preview_data(
    client: Client, 
    table: str, 
    columns: List[str], 
    join_config: Optional[Dict[str, Any]] = None,
    limit: int = 100
) -> List[Dict[str, Any]]:
    """Preview data from ClickHouse."""
    try:
        query = build_query([table], {table: columns}, join_config, limit)
        result = client.execute(query, with_column_types=True)
        
        data, column_types = result
        column_names = [col[0] for col in column_types]
        
        # Format results as list of dictionaries
        formatted_data = []
        for row in data:
            row_dict = {}
            for i, value in enumerate(row):
                row_dict[column_names[i]] = value
            formatted_data.append(row_dict)
            
        return formatted_data
    except Exception as e:
        raise Exception(f"Data preview failed: {str(e)}")

def export_to_file(
    client: Client,
    tables: List[str],
    columns_map: Dict[str, List[str]],
    output_file: str,
    delimiter: str = ",",
    join_config: Optional[Dict[str, Any]] = None
) -> int:
    """Export data from ClickHouse to a flat file."""
    try:
        query = build_query(tables, columns_map, join_config)
        
        # Execute query to get results and column info
        result = client.execute(query, with_column_types=True)
        data, column_types = result
        column_names = [col[0] for col in column_types]
        
        # Write data to CSV
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=delimiter)
            # Write header
            writer.writerow(column_names)
            # Write data rows
            for row in data:
                writer.writerow(row)
        
        return len(data)
    except Exception as e:
        raise Exception(f"Export to file failed: {str(e)}")