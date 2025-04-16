from fastapi import FastAPI, UploadFile, File, Form, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import json
import os
from tempfile import NamedTemporaryFile
import csv
import clickhouse
import file_handler
from fastapi.responses import JSONResponse

app = FastAPI(title="ClickHouse-Flat File Data Integration Tool")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify actual origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Models
class ClickHouseConfig(BaseModel):
    host: str
    port: int
    database: str
    user: str
    jwt_token: str

class FlatFileConfig(BaseModel):
    delimiter: str = ","
    has_header: bool = True

class TableInfo(BaseModel):
    name: str
    columns: List[str]

class IngestionConfig(BaseModel):
    source_type: str  # "clickhouse" or "flatfile"
    clickhouse_config: Optional[ClickHouseConfig] = None
    flat_file_config: Optional[FlatFileConfig] = None
    selected_tables: Optional[List[str]] = None
    selected_columns: Dict[str, List[str]] = {}  # table_name -> [column_names]
    join_config: Optional[Dict[str, Any]] = None  # For bonus requirement

# Global state for tracking jobs
ingestion_jobs = {}

@app.get("/")
def read_root():
    return {"status": "running", "service": "ClickHouse-Flat File Integration Tool"}

@app.post("/connect/clickhouse")
async def connect_clickhouse(config: ClickHouseConfig):
    try:
        # Try to connect and fetch tables
        client = clickhouse.create_client(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            jwt_token=config.jwt_token
        )
        
        tables = clickhouse.get_tables(client)
        return {"status": "success", "tables": tables}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Connection failed: {str(e)}")

@app.post("/get-columns")
async def get_columns(config: ClickHouseConfig, table_name: str = Form(...)):
    try:
        client = clickhouse.create_client(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            jwt_token=config.jwt_token
        )
        
        columns = clickhouse.get_columns(client, table_name)
        return {"status": "success", "columns": columns}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to get columns: {str(e)}")

@app.post("/upload-file")
async def upload_file(
    file: UploadFile = File(...),
    delimiter: str = Form(","),
    has_header: bool = Form(True)
):
    try:
        # Save uploaded file to temp location
        with NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
            temp_file_path = temp_file.name
            content = await file.read()
            temp_file.write(content)
        
        # Read file schema
        columns = file_handler.get_file_schema(temp_file_path, delimiter, has_header)
        
        return {
            "status": "success", 
            "filename": file.filename,
            "temp_path": temp_file_path,
            "columns": columns
        }
    except Exception as e:
        if os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
        raise HTTPException(status_code=400, detail=f"File processing failed: {str(e)}")

@app.post("/preview-data")
async def preview_data(background_tasks: BackgroundTasks, config: Dict[str, Any]):
    try:
        source_type = config.get("source_type")
        
        if source_type == "clickhouse":
            ch_config = ClickHouseConfig(**config.get("clickhouse_config", {}))
            selected_tables = config.get("selected_tables", [])
            selected_columns = config.get("selected_columns", {})
            join_config = config.get("join_config")
            
            client = clickhouse.create_client(
                host=ch_config.host,
                port=ch_config.port,
                database=ch_config.database,
                user=ch_config.user,
                jwt_token=ch_config.jwt_token
            )
            
            preview_data = clickhouse.preview_data(
                client, 
                selected_tables[0] if selected_tables else None,
                selected_columns.get(selected_tables[0], []) if selected_tables else [],
                join_config,
                limit=100
            )
            
            return {"status": "success", "preview": preview_data}
            
        elif source_type == "flatfile":
            file_path = config.get("file_path")
            flat_file_config = FlatFileConfig(**config.get("flat_file_config", {}))
            selected_columns = config.get("selected_columns", {}).get("file", [])
            
            preview_data = file_handler.preview_data(
                file_path, 
                flat_file_config.delimiter, 
                flat_file_config.has_header,
                selected_columns,
                limit=100
            )
            
            return {"status": "success", "preview": preview_data}
        
        else:
            raise HTTPException(status_code=400, detail="Invalid source type")
            
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Preview failed: {str(e)}")

@app.post("/start-ingestion")
async def start_ingestion(background_tasks: BackgroundTasks, config: Dict[str, Any]):
    try:
        job_id = str(len(ingestion_jobs) + 1)
        ingestion_jobs[job_id] = {"status": "starting", "records_processed": 0}
        
        background_tasks.add_task(
            process_ingestion,
            job_id=job_id,
            config=config
        )
        
        return {"status": "started", "job_id": job_id}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to start ingestion: {str(e)}")

@app.get("/job-status/{job_id}")
async def get_job_status(job_id: str):
    if job_id not in ingestion_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return ingestion_jobs[job_id]

def process_ingestion(job_id: str, config: Dict[str, Any]):
    try:
        ingestion_jobs[job_id]["status"] = "processing"
        source_type = config.get("source_type")
        
        if source_type == "clickhouse":
            # ClickHouse to Flat File
            ch_config = ClickHouseConfig(**config.get("clickhouse_config", {}))
            selected_tables = config.get("selected_tables", [])
            selected_columns = config.get("selected_columns", {})
            output_file = config.get("output_file", f"export_{job_id}.csv")
            flat_file_config = FlatFileConfig(**config.get("flat_file_config", {}))
            join_config = config.get("join_config")
            
            client = clickhouse.create_client(
                host=ch_config.host,
                port=ch_config.port,
                database=ch_config.database,
                user=ch_config.user,
                jwt_token=ch_config.jwt_token
            )
            
            # Perform the data export
            records_count = clickhouse.export_to_file(
                client,
                selected_tables,
                selected_columns,
                output_file,
                flat_file_config.delimiter,
                join_config
            )
            
            ingestion_jobs[job_id]["records_processed"] = records_count
            ingestion_jobs[job_id]["output_file"] = output_file
            
        elif source_type == "flatfile":
            # Flat File to ClickHouse
            file_path = config.get("file_path")
            ch_config = ClickHouseConfig(**config.get("clickhouse_config", {}))
            selected_columns = config.get("selected_columns", {}).get("file", [])
            target_table = config.get("target_table")
            flat_file_config = FlatFileConfig(**config.get("flat_file_config", {}))
            
            client = clickhouse.create_client(
                host=ch_config.host,
                port=ch_config.port,
                database=ch_config.database,
                user=ch_config.user,
                jwt_token=ch_config.jwt_token
            )
            
            # Perform the data import
            records_count = file_handler.import_to_clickhouse(
                file_path,
                client,
                target_table,
                selected_columns,
                flat_file_config.delimiter,
                flat_file_config.has_header
            )
            
            ingestion_jobs[job_id]["records_processed"] = records_count
        
        ingestion_jobs[job_id]["status"] = "completed"
        
    except Exception as e:
        ingestion_jobs[job_id]["status"] = "failed"
        ingestion_jobs[job_id]["error"] = str(e)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)