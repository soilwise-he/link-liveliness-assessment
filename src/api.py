from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv
from databases import Database
from typing import List
from pydantic import BaseModel
from urllib.parse import unquote
import asyncpg
import os

# Load environment variables from .env file
load_dotenv()

# Database connection setup
# Load environment variables securely (replace with your actual variable names)
DATABASE_URL = "postgresql://" + os.environ.get("POSTGRES_USER") + ":" +\
    os.environ.get("POSTGRES_PASSWORD") + "@" + os.environ.get("POSTGRES_HOST") + ":" +\
    os.environ.get("POSTGRES_PORT") + "/" + os.environ.get("POSTGRES_DB")

database = Database(DATABASE_URL)

# FastAPI app instance
rootpath=os.environ.get("ROOTPATH") or "/" 
app = FastAPI(root_path=rootpath)

# Define response model
class StatusResponse(BaseModel):
    id: int  # Example column, adjust based on your actual table schema
    urlname: str
    parentname: str
    valid: str
    warning: str

# Define status lists
REDIRECTION_STATUSES = [
    "301 Moved Permanently",
    "302 Found (Moved Temporarily)",
    "304 Not Modified",
    "307 Temporary Redirect",
    "308 Permanent Redirect"
]

CLIENT_ERROR_STATUSES = [
    "400 Bad Request",
    "401 Unauthorized",
    "403 Forbidden",
    "404 Not Found",
    "405 Method Not Allowed",
    "409 Conflict"
]

SERVER_ERROR_STATUSES = [
    "500 Internal Server Error",
    "501 Not Implemented",
    "503 Service Unavailable",
    "504 Gateway Timeout"
]

# Helper function to execute SQL query and fetch results
async def fetch_data(query: str, values: dict = {}):
    try:
        return await database.fetch_all(query=query, values=values)
    except asyncpg.exceptions.UndefinedTableError:
        raise HTTPException(status_code=500, detail="The specified table does not exist")
    except Exception as e:
        raise HTTPException(status_code=500, detail="Database query failed") from e

@app.get('/Redirection_URLs/3xx', response_model=List[StatusResponse])
async def get_redirection_statuses():
    query = "SELECT DISTINCT * FROM linkchecker_output WHERE valid = ANY(:statuses)"
    data = await fetch_data(query=query, values={'statuses': REDIRECTION_STATUSES})
    return data

# Endpoint to retrieve data with client error statuses
@app.get('/Client_Error_URLs/4xx', response_model=List[StatusResponse])
async def get_client_error_statuses():
    query = "SELECT DISTINCT * FROM linkchecker_output WHERE valid = ANY(:statuses)"
    data = await fetch_data(query=query, values={'statuses': CLIENT_ERROR_STATUSES})
    return data

# Endpoint to retrieve data with server error statuses
@app.get('/Server_Errors_URLs/5xx', response_model=List[StatusResponse])
async def get_server_error_statuses():
    query = "SELECT DISTINCT * FROM linkchecker_output WHERE valid = ANY(:statuses)"
    data = await fetch_data(query=query, values={'statuses': SERVER_ERROR_STATUSES})
    return data

# Endpoint to retrieve data where the warning column is not empty
@app.get('/URLs_Which_Have_Warnings', response_model=List[StatusResponse])
async def get_non_empty_warnings():
    query = "SELECT DISTINCT * FROM linkchecker_output WHERE warning != ''"
    data = await fetch_data(query=query)
    return data

# Endpoint to retrieve data with client error statuses
@app.get('/status/{item:path}', response_model=List[StatusResponse])
async def get_status_for_url(item):
    decoded_item = unquote(item)
    query = "SELECT * FROM linkchecker_output WHERE urlname = :item"
    data = await fetch_data(query=query, values={'item': decoded_item })
    return data

# Start the application
@app.on_event('startup')
async def startup():
    try:
        await database.connect()
    except Exception as e:
        raise HTTPException(status_code=500, detail="Database connection failed") from e

@app.on_event('shutdown')
async def shutdown():
    try:
        await database.disconnect()
    except Exception as e:
        raise HTTPException(status_code=500, detail="Database disconnection failed") from e
