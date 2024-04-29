from fastapi import FastAPI, APIRouter, Query
import pandas as pd

app = FastAPI(
    title="SOILWISE SERVICE PROJECT",
    description="API that Retrieves EJPSOIL Catalogue URLs status",
)

# Define a route group
urls_router = APIRouter(tags=["Retrieve URLs Info"])

redirection_statuses = [
    "301 Moved Permanently",
    "302 Found (Moved Temporarily)",
    "304 Not Modified",
    "307 Temporary Redirect",
    "308 Permanent Redirect"
]

client_error_statuses = [
    "400 Bad Request",
    "401 Unauthorized",
    "403 Forbidden",
    "404 Not Found",
    "405 Method Not Allowed",
    "409 Conflict"
]

server_error_statuses = [
    "500 Internal Server Error",
    "501 Not Implemented",
    "503 Service Unavailable",
    "504 Gateway Timeout"
]

data = pd.read_csv("soil_catalogue_link.csv")
data = data.fillna('')

def paginate_data(data_frame: pd.DataFrame, skip: int = 0, limit: int = 10):
    """
    Paginates the result from DataFrame
    Args:
        data_frame: The DataFrame to paginate.
        skip: The number of records to skip (default: 0).
        limit: The maximum number of records to return per page (default: 10). 
    """
    return data_frame.iloc[skip: skip + limit]

def get_urls_by_category(category_statuses, column_to_check="valid"):
    """
    Filters URL from the DataFrame based on the provided status code list
    The column containing the values to check ("default valid")
    """
    filtered_data = data[data[column_to_check].isin(category_statuses)]
    filtered_rows = filtered_data.to_dict(orient='records')
    return filtered_rows

@urls_router.get("/Redirection_URLs/3xx", name="Get Redirection URLs 3xx")
async def get_redirection_urls():
    """
    Retrieve URLs from the CSV classified as status code 3xx'
    """
    urls = get_urls_by_category(redirection_statuses)
    return {"category": "3xx Redirection", "urls": urls}

@urls_router.get("/Client_Error_URLs/4xx", name="Get Client Error URLs 4xx")
async def get_client_error_urls():
    """
    Retrieves URLs from the CSV classified as status code 4xx
    """
    urls = get_urls_by_category(client_error_statuses)
    return {"category": "4xx Client Error", "urls": urls}

@urls_router.get("/Server_Error_URLs/5xx", name="Get Server Error URLs 5xx")
async def get_server_error_urls():
    """
    Retrieves URLs from the CSV classified as status code 5xx
    """
    urls = get_urls_by_category(server_error_statuses)
    return {"category": "5xx Server Error", "urls": urls}

@urls_router.get("/URLs_Which_Have_Warnings", name="Get URLs that contain warnings")
async def get_warning_urls(skip: int = Query(0, ge=0), limit: int = Query(10, ge=1)):
    """
    Retrieves URLs from the CSV that contain warnings
    """
    filtered_data = data[data['warning'] != '']
    paginated_data = paginate_data(filtered_data, skip=skip, limit=limit)
    return {"category": "Has Warnings", "urls": paginated_data.to_dict(orient='records')}

# Include the router in the main app
app.include_router(urls_router)
