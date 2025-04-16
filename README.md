# Bidirectional ClickHouse & Flat File Data Ingestion Tool

A web application that facilitates bidirectional data transfer between ClickHouse databases and flat files (CSV). This tool provides a user-friendly interface for configuring connections, selecting data sources, choosing specific columns, and monitoring data transfer operations.

## Features

- **Bidirectional Data Flow**: Transfer data from ClickHouse to flat files and vice versa
- **JWT Authentication**: Support for ClickHouse JWT token-based authentication
- **Schema Discovery**: Automatically detect and display available tables and columns
- **Column Selection**: Choose specific columns to include in the data transfer
- **Data Preview**: View sample data before starting the ingestion process
- **Progress Monitoring**: Track the status of data transfer operations
- **Completion Reporting**: Display the total number of records processed

## Architecture

The application consists of two main components:

1. **Backend**: A Python FastAPI server that handles database connections, file operations, and data transfer logic
2. **Frontend**: A React-based user interface for configuration and monitoring

## Installation

### Prerequisites

- Python 3.8+ with pip
- Node.js 14+ with npm
- ClickHouse database (local or remote)

### Backend Setup


# Create and navigate to backend directory
mkdir -p clickhouse-file-integration/backend
cd clickhouse-file-integration/backend

# Create a virtual environment
python -m venv venv

# Activate the virtual environment
# On Windows:
venv\Scripts\activate

# On macOS/Linux:
source venv/bin/activate

# Install dependencies
pip install fastapi uvicorn clickhouse-connect python-multipart pydantic



> **Note**: If you encounter issues installing clickhouse-driver, try using clickhouse-connect instead: `pip install clickhouse-connect`

### Frontend Setup


# Navigate back to project root
cd ..

# Create and set up React application
npx create-react-app frontend
cd frontend

# Install additional dependencies
npm install axios react-bootstrap bootstrap


## Running the Application

### Start the Backend Server


# From the backend directory
cd clickhouse-file-integration/backend

# Activate the virtual environment (if not already activated)
source venv/bin/activate  # On macOS/Linux
# or
venv\Scripts\activate  # On Windows

# Start the FastAPI server
uvicorn app:app --reload --host 0.0.0.0 --port 8000


### Start the Frontend Server


# From the frontend directory
cd clickhouse-file-integration/frontend

# Start the React development server
npm start


The application should now be accessible at:
- Backend API: http://localhost:8000
- Frontend UI: http://localhost:3000

## Usage Guide

1. **Select Source & Target**:
   - Choose either ClickHouse or Flat File as your data source
   - The target will automatically be set to the opposite type

2. **Configure Connection**:
   - For ClickHouse: Enter host, port, database, user, and JWT token
   - For Flat File: Upload a CSV file and specify delimiter and header settings

3. **Select Tables and Columns**:
   - Choose the tables you want to transfer data from (ClickHouse)
   - Select specific columns to include in the transfer

4. **Preview Data** (Optional):
   - View a sample of the data before starting the full transfer

5. **Start Ingestion**:
   - Begin the data transfer process
   - Monitor the progress and view the completion report

## Testing

For testing, you can use ClickHouse example datasets such as `uk_price_paid` and `ontime`:


# Run a local ClickHouse instance using Docker
docker run -d --name clickhouse-server -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server

# Download and import example datasets
# See: https://clickhouse.com/docs/getting-started/example-datasets


## Technical Details

- **Backend**: Python FastAPI with clickhouse-connect/clickhouse-driver
- **Frontend**: React with Bootstrap for styling
- **Authentication**: JWT token-based authentication for ClickHouse
- **Data Handling**: Efficient batch processing for large datasets

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

