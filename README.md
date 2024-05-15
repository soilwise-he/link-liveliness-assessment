# OGC API - Records; link liveliness assessment

A component which evaluates for a set of metadata records (describing either data or knowledge sources), if:

- the links to external sources are valid
- the links within the repository are valid
- link metadata represents accurately the resource

The component either returns a http status: 200 (ok), 403 (non autorized), 404 (not found), 500 (error), ...
Status 302 is forwarded to new location and the test is repeated.

The component runs an evaluation for a single resource at request, or runs tests at intervals providing a history of availability 

A link either points to:

- another metadata record
- a downloadable instance (pdf/zip/sqlite) of the resource
- an API

If endpoint is API, some sanity checks can be performed on the API:

- Identify if the API adopted any API-standard
- IF an API standard is adopted, does the API support basic operations of that API

The benefit of latter is that it provides more information then a simple ping to the index page of the API, typical examples of standardised API's are SOAP, GraphQL, SPARQL, OpenAPI, WMS, WFS

## OGC API - records

OGC is in the process of adopting the [OGC API - Records](https://github.com/opengeospatial/ogcapi-records) specification. A standardised API to interact with Catalogues. The specification includes a datamodel for metadata. This tool assesses the linkage section of any record in an OGC API - Records.

## Source Code Brief Desrciption

The source code leverages the [linkchecker](https://linkchecker.github.io/linkchecker/index.html) tool in order to check weather a link 
froom the [EJP Soil Catalogue](https://catalogue.ejpsoil.eu/collections/metadata:main/items?offset=0)
The [JSON](https://catalogue.ejpsoil.eu/collections/metadata:main/items?f=json) file is used in order to retrieve details about the pagination.
A string is created for each page.For every url python [requests](https://pypi.org/project/requests/) library is used in order to retrieve all urls for each page.
Linkchecker command:  
 * subprocess.Popen(["docker", "run", "--rm", "-i", "-u", "1000:1000", "ghcr.io/linkchecker/linkchecker:latest", 
    "--verbose", "--check-extern", "--recursion-level=1",  "--output=csv", url + "?f=html"])
runs a container with the LinkChecker tool and instructs it to check the links in verbose mode, follow external links up to one level deep, and output the results in a CSV file format.

A FastAPI is created to provide endpoints based on the statuses of links, including those with status codes 300, 400, and 500, as well as those containing warnings.
Command to run the FastAPI
* python -m uvicorn api:app --reload --host 0.0.0.0 --port 8000 
  
To view the running FastAPI navigate on: [http://127.0.0.1:8000/docs]

## CI/CD
This workflow is designed to run as a cron job at midnight every Sunday.
The execution time takes about 80 minutes to complete and more than 12.000 urls are checked.
Currently the workflow is commented.

## Known issues
Attempting to write LinkChecker's output directly to a PostgreSQL database causes crashes due to encountering invalid characters and missing values within the data.


## Roadmap

### Report about results

Stats are currently saved as CSV. Stats should be ingested into a format which can be used to create reports in a platform like Apache Superset

### GeoHealthCheck integration

[GeoHealthCheck](https://GeoHealthCheck.org) is a component to monitor livelyhood of typical OGC services (WMS, WFS, WCS, CSW). It is based on the [owslib](https://owslib.readthedocs.io/en/latest/) library, which provides a python implementation of various OGC services clients.

## Soilwise-he project

This work has been initiated as part of the [Soilwise-he project](https://soilwise-he.eu/).
The project receives funding from the European Union’s HORIZON Innovation Actions 2022 under grant agreement No. 101112838.
