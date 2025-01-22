from fastapi import FastAPI, HTTPException, Query
from dotenv import load_dotenv
from databases import Database
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime
import asyncpg
import logging
import os

# Load environment variables from .env file
load_dotenv()

# Database connection setup
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

# Define response models
class LinkResponse(BaseModel):
    id_link: int 
    urlname: str
    deprecated: Optional[bool] = None
    consecutive_failures: Optional[int] = None
    link_type: Optional[str] = None  
    link_size: Optional[int] = None  

class StatusResponse(LinkResponse):
    status_code: Optional[int] = None
    record_id: Optional[str] = None 
    is_redirect: Optional[bool] = None  
    error_message: Optional[str] = None
    timestamp: datetime

class TimeoutResponse(LinkResponse):
    status_code: Optional[int] = None  # Make status_code optional for timeout cases
    final_url: Optional[str] = None
    record_id: Optional[str] = None 
    is_redirect: Optional[bool] = None
    error_message: Optional[str] = None
    timestamp: datetime
    
# Define status lists
REDIRECTION_STATUSES = [301, 302, 304, 307, 308]
CLIENT_ERROR_STATUSES = [400, 401, 403, 404, 405, 409]
SERVER_ERROR_STATUSES = [500, 501, 503, 504]

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
        SELECT l.id_link, l.urlname, l.deprecated, l.consecutive_failures, l.link_type, l.link_size,
               r.record_id, vh.status_code, vh.is_redirect, vh.error_message, vh.timestamp
        FROM links l
        JOIN records r ON l.fk_record = r.id
        JOIN validation_history vh ON l.id_link = vh.fk_link
        WHERE vh.status_code = ANY(:statuses)
        AND vh.timestamp = (
            SELECT MAX(timestamp)
            FROM validation_history
            WHERE fk_link = l.id_link
        )
    """
    data = await fetch_data(query=query, values={'statuses': REDIRECTION_STATUSES})
    return data

# Endpoint to retrieve data with client error statuses
@app.get('/Client_Error_URLs/4xx', response_model=List[StatusResponse])
async def get_client_error_statuses():
    query = """
        SELECT l.id_link, l.urlname, l.deprecated, l.consecutive_failures, l.link_type, l.link_size,
               r.record_id, vh.status_code, vh.is_redirect, vh.error_message, vh.timestamp
        FROM links l
        JOIN records r ON l.fk_record = r.id
        JOIN validation_history vh ON l.id_link = vh.fk_link
        WHERE vh.status_code = ANY(:statuses)
        AND vh.timestamp = (
            SELECT MAX(timestamp)
            FROM validation_history
            WHERE fk_link = l.id_link
        )
    """
    data = await fetch_data(query=query, values={'statuses': CLIENT_ERROR_STATUSES})
    return data

# Endpoint to retrieve data with server error statuses
@app.get('/Server_Errors_URLs/5xx', response_model=List[StatusResponse])
async def get_server_error_statuses():
    query = """
        SELECT l.id_link, l.urlname, l.deprecated, l.consecutive_failures, l.link_type, l.link_size,
               r.record_id, vh.status_code, vh.is_redirect, vh.error_message, vh.timestamp
        FROM links l
        JOIN records r ON l.fk_record = r.id
        JOIN validation_history vh ON l.id_link = vh.fk_link
        WHERE vh.status_code = ANY(:statuses)
        AND vh.timestamp = (
            SELECT MAX(timestamp)
            FROM validation_history
            WHERE fk_link = l.id_link
        )
    """
    data = await fetch_data(query=query, values={'statuses': SERVER_ERROR_STATUSES})
    return data

# Endpoint to retrieve data for a specific URL
@app.get('/status/{item:path}', response_model=List[StatusResponse])
async def get_status_for_url(item):
    query = """
        SELECT l.id_link, l.urlname, l.deprecated, l.consecutive_failures, l.link_type, l.link_size,
               r.record_id, vh.status_code, vh.is_redirect, vh.error_message, vh.timestamp
        FROM links l
        JOIN records r ON l.fk_record = r.id
        JOIN validation_history vh ON l.id_link = vh.fk_link
        WHERE l.urlname = :item
        AND vh.timestamp = (
            SELECT MAX(timestamp)
            FROM validation_history
            WHERE fk_link = l.id_link
        )
    """
    data = await fetch_data(query=query, values={'item': item})
    return data

# Update the timeout endpoint to match other query structures
@app.get('/Timeout_URLs', response_model=List[TimeoutResponse])
async def get_timeout_urls():
    query = """
        SELECT l.id_link, l.urlname, l.deprecated, l.consecutive_failures, l.link_type, l.link_size,
               r.record_id, vh.status_code, vh.is_redirect, vh.error_message, vh.timestamp
        FROM links l
        JOIN records r ON l.fk_record = r.id
        JOIN validation_history vh ON l.id_link = vh.fk_link
        WHERE (vh.error_message LIKE '%ReadTimeout%' OR vh.error_message LIKE '%ConnectTimeout%')
        AND vh.timestamp = (
            SELECT MAX(timestamp)
            FROM validation_history
            WHERE fk_link = l.id_link
        )
    """
    data = await fetch_data(query=query)
    return data

@app.get('/Deprecated_URLs', response_model=List[LinkResponse])
async def get_deprecated_urls():
    query = """
        SELECT id_link, urlname, r.record_id, deprecated, consecutive_failures, link_type, link_size
        FROM links
        JOIN records r ON l.fk_record = r.id
        WHERE deprecated IS TRUE
    """
    data = await fetch_data(query=query)
    return data

@app.get("/URL_status_history", response_model=List[StatusResponse])
async def get_url_status_history(
    url: str = Query(..., description="URL to get availability history"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results (default: 100, min: 1, max: 1000)")
) -> List[StatusResponse]:
    query = """
        SELECT 
            l.id_link,
            l.urlname,
            l.deprecated,
            r.record_id,
            l.consecutive_failures,
            l.link_type, 
            l.link_size,
            vh.status_code,
            vh.is_redirect,
            vh.error_message,
            vh.timestamp
        FROM 
            links l
        JOIN records r ON l.fk_record = r.id
        JOIN validation_history vh ON l.id_link = vh.fk_link
        WHERE 
            l.urlname = :url
        ORDER BY 
            vh.timestamp DESC
        LIMIT :limit
    """

    try:
        results = await fetch_data(query=query, values={'url': url, 'limit': limit})
        logger.info(f"Query returned {len(results)} results for URL: {url}")
        
        response_data = [StatusResponse(**dict(row)) for row in results]
        
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