from fastapi import FastAPI, HTTPException, Query
from dotenv import load_dotenv
from databases import Database
from typing import List, Optional
from pydantic import BaseModel
from urllib.parse import unquote
from datetime import datetime
import asyncpg
import logging
import os

# Load environment variables from .env file
load_dotenv()

# Database connection setup
# Load environment variables securely (replace with your actual variable names)
DATABASE_URL = "postgresql://" + os.environ.get("POSTGRES_USER") + ":" +\
    os.environ.get("POSTGRES_PASSWORD") + "@" + os.environ.get("POSTGRES_HOST") + ":" +\
    os.environ.get("POSTGRES_PORT") + "/" + os.environ.get("POSTGRES_DB")

database = Database(DATABASE_URL)
rootpath = os.environ.get("ROOTPATH") or "/"

# FastAPI app instance
app = FastAPI(
    title="Linkchecker-Liveness",
    summary="Evaluate the status of URLs from OGC data catalogues",
    root_path=rootpath
)
logger = logging.getLogger(__name__)


# Define response model
class StatusResponse(BaseModel):
    id: int 
    urlname: Optional[str]
    parentname: Optional[str]
    valid: Optional[str]
    warning: Optional[str]

# Model to get the availability history of a specific url
class URLAvailabilityResponse(BaseModel):
    url: Optional[str]
    perent_url: Optional[str]
    validation_valid: Optional[str]
    result: Optional[str]
    warning: Optional[str]
    lastChecked: Optional[datetime]
    
class DeprecatedUrlsResponse(BaseModel):
    url: Optional[str]
        
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

@app.get("/Single_url_status_history", response_model=List[URLAvailabilityResponse])
async def get_current_url_status_history(
        url: str = Query(..., description="URL to get avalability"),
        limit: int = Query(100, ge=1, le=1000, description="Maximum number of results (default: 100, min: 1, max: 1000)")) -> List[URLAvailabilityResponse]:
    query = """
    SELECT 
        lo.urlname AS url, 
        lo.parentname AS parent_url,
        lo.result AS result,
        lo.warning AS warning, 
        vh.validation_result AS validation_valid, 
        vh.timestamp AS last_checked
    FROM 
        linkchecker_output lo
    JOIN (
        SELECT 
            url, 
            validation_result, 
            timestamp,
            ROW_NUMBER() OVER (PARTITION BY url ORDER BY timestamp DESC) as rn
        FROM 
            validation_history
    ) vh ON lo.urlname = vh.url AND vh.rn = 1
    WHERE (lo.urlname = :url)
    LIMIT :limit
    """

    try:
        results = await fetch_data(query=query, values={'url': url, 'limit': limit})
        logger.info(f"Query returned {len(results)} results.")
        
        response_data = [URLAvailabilityResponse(**dict(row)) for row in results]
        
        return response_data
    except Exception as e:
        logger.error(f"Error occurred: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/All_url_status_history", response_model=List[URLAvailabilityResponse])
async def get_all_url_status_history(
        limit: int = Query(100, ge=1, le=1000, description="Maximum number of results (default: 100, min: 1, max: 1000)")) -> List[URLAvailabilityResponse]:

    query = """
    SELECT
        lo.urlname AS url, 
        lo.parentname AS parent_url,
        lo.result AS result,
        lo.warning AS warning, 
        vh.validation_result AS validation_valid, 
        vh.timestamp AS last_checked
    FROM 
        linkchecker_output lo
    JOIN (
        SELECT 
            url, 
            validation_result, 
            timestamp,
            ROW_NUMBER() OVER (PARTITION BY url ORDER BY timestamp DESC) as rn
        FROM 
            validation_history
    ) vh ON lo.urlname = vh.url AND vh.rn = 1
    ORDER BY 
        vh.timestamp DESC
    LIMIT :limit
    """

    values = {"limit": limit}

    try:
        results = await fetch_data(query=query, values=values)
        logging.info(f"Query returned {len(results)} results.")

        response_data = [URLAvailabilityResponse(**row) for row in results]

        return response_data
    except Exception as e:
        logging.error(f"Error occurred: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get('/Deprecated URLs', response_model=List[DeprecatedUrlsResponse])
async def get_deprecated_urls():
    query = """
    SELECT
        us.url AS url
    FROM 
        url_status us
    WHERE us.deprecated = TRUE
    """
    try:
        data = await fetch_data(query=query)
        return data
    except Exception as e:
        logging.error(f"Error occurred: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

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
