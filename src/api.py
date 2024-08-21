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
    parent_urls: Optional[List[str]]
    status: Optional[str]
    result: Optional[str]
    info: Optional[str]
    warning: Optional[str]
    deprecated: Optional[bool] = None

# Model to get the availability history of a specific url
class URLAvailabilityResponse(BaseModel):
    urlname: Optional[str] = None
    status: Optional[str] = None
    result: Optional[str] = None
    info: Optional[str] = None
    warning: Optional[str] = None
    validation_valid: Optional[str] = None
    last_checked: datetime
        
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
        logging.error("The specified table does not exist", exc_info=True)
        raise HTTPException(status_code=500, detail="The specified table does not exist")
    except Exception as e:
        logging.error(f"Database query failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Database query failed")
    
# Endpoint to retrieve data with redirection statuses
@app.get('/Redirection_URLs/3xx', response_model=List[StatusResponse])
async def get_redirection_statuses():
    query = """
        SELECT 
            l.id_link AS id, 
            l.urlname, 
            l.status,
            l.warning,
            l.result,
            l.info,
            array_remove(array_agg(DISTINCT p.parentname), NULL) AS parent_urls
        FROM 
            links l
        LEFT JOIN 
            parent p ON l.id_link = p.fk_link
        WHERE 
            l.status ILIKE ANY (:statuses)
        GROUP BY
            l.id_link, l.urlname, l.status, l.warning, result, info
    """
    data = await fetch_data(query=query, values={'statuses': REDIRECTION_STATUSES})
    return data

# Endpoint to retrieve data with client error statuses
@app.get('/Client_Error_URLs/4xx', response_model=List[StatusResponse])
async def get_client_error_statuses():
    query = """
        SELECT 
            l.id_link AS id, 
            l.urlname, 
            l.status,
            l.warning,
            l.result,
            l.info,
            array_remove(array_agg(DISTINCT p.parentname), NULL) AS parent_urls
        FROM 
            links l
        LEFT JOIN 
            parent p ON l.id_link = p.fk_link
        WHERE 
            l.status ILIKE ANY (:statuses)
        GROUP BY
            l.id_link, l.urlname, l.status, l.warning, result, info
    """
    data = await fetch_data(query=query, values={'statuses': CLIENT_ERROR_STATUSES})
    return data

# Endpoint to retrieve data with server error statuses
@app.get('/Server_Errors_URLs/5xx', response_model=List[StatusResponse])
async def get_server_error_statuses():
    query = """
        SELECT 
            l.id_link AS id, 
            l.urlname, 
            l.status,
            l.warning,
            l.result,
            l.info,
            array_remove(array_agg(DISTINCT p.parentname), NULL) AS parent_urls
        FROM 
            links l
        LEFT JOIN 
            parent p ON l.id_link = p.fk_link
        WHERE 
            l.status ILIKE ANY (:statuses)
        GROUP BY
            l.id_link, l.urlname, l.status, l.warning, result, info
    """
    data = await fetch_data(query=query, values={'statuses': SERVER_ERROR_STATUSES})
    return data

# Endpoint to retrieve data with client error statuses
@app.get('/status/{item:path}', response_model=List[StatusResponse])
async def get_status_for_url(item):
    query = """
        SELECT 
            l.id_link AS id, 
            l.urlname, 
            l.status,
            l.warning,
            l.result,
            l.info,
            array_remove(array_agg(DISTINCT p.parentname), NULL) AS parent_urls
        FROM 
            links l
        LEFT JOIN 
            parent p ON l.id_link = p.fk_link
        WHERE 
            l.urlname = :item
        GROUP BY
            l.id_link, l.urlname, l.status, l.warning, result, info
    """
    data = await fetch_data(query=query, values={'item': item})
    return data

# Endpoint to retrieve URLs that that timed out. Timeout is set to 5 seconds currently  
@app.get('/Timeout_URLs', response_model=List[StatusResponse])
async def get_timeout_urls():
    query = """
        SELECT 
            l.id_link AS id, 
            l.urlname, 
            l.status,
            l.warning,
            l.result,
            l.info,
            array_remove(array_agg(DISTINCT p.parentname), NULL) AS parent_urls
        FROM 
            links l
        LEFT JOIN 
            parent p ON l.id_link = p.fk_link
        WHERE 
            l.status LIKE '%ReadTimeout%' OR l.status LIKE '%ConnectTimeout%'
        GROUP BY
            l.id_link, l.urlname, l.status, l.warning, result, info
    """
    data = await fetch_data(query=query)
    return data

@app.get('/Deprecated URLs', response_model=List[StatusResponse])
async def get_deprecated_urls():
    query = """
        SELECT 
            l.id_link AS id, 
            l.urlname, 
            l.status,
            l.warning,
            l.result,
            l.info,
            l.deprecated,
            array_remove(array_agg(DISTINCT p.parentname), NULL) AS parent_urls
        FROM 
            links l
        LEFT JOIN 
            parent p ON l.id_link = p.fk_link
        WHERE l.deprecated IS TRUE
        GROUP BY
            l.id_link, l.urlname, l.status, l.warning, result, info
    """
    data = await fetch_data(query=query)
    return data

@app.get("/URL_status_history", response_model=List[URLAvailabilityResponse])
async def get_url_status_history(
    url: str = Query(..., description="URL to get availability history"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results (default: 100, min: 1, max: 1000)")
) -> List[URLAvailabilityResponse]:
    query = """
        SELECT 
            l.urlname,
            l.status,
            l.result,
            l.info,
            l.warning,
            vh.validation_result AS validation_valid,
            vh.timestamp AS last_checked,
            array_agg(DISTINCT p.parentname) AS parent_urls
        FROM 
            links l
        LEFT JOIN 
            parent p ON l.id_link = p.fk_link
        LEFT JOIN 
            validation_history vh ON l.id_link = vh.fk_link
        WHERE 
            l.urlname = :url
        GROUP BY
            l.urlname, l.status, l.result, l.info, l.warning, vh.validation_result, vh.timestamp
        ORDER BY 
            vh.timestamp DESC
        LIMIT :limit
    """

    try:
        results = await fetch_data(query=query, values={'url': url, 'limit': limit})
        logger.info(f"Query returned {len(results)} results for URL: {url}")
        
        response_data = [URLAvailabilityResponse(**dict(row)) for row in results]
        
        return response_data
    except Exception as e:
        logger.error(f"Error occurred while fetching URL status history: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch URL status history")

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
