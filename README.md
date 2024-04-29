# link-liveliness-assessment

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

## GeoHealthCheck

[GeoHealthCheck](https://GeoHealthCheck.org) is a component to monitor livelyhood of typical OGC services (WMS, WFS, WCS, CSW). It is based on the [owslib](https://owslib.readthedocs.io/en/latest/) library, which provides a python implementation of various OGC services clients.

## Source Code Brief Desrciption

Running the linkchecker.py will utilize the requests library from python to get the relevant EJP Soil Catalogue source.
Run the command below
* python linkchecker.py
The urls selected from the requests will passed to linkchecker using the proper options.
The output will be written on a csv file. 
Writing the output to PostgrSQL database causes program to crash since various invalid characters and missing values occur at 
various places.

## API
The api.py file creates a FastAPI in order to retrieve links statuses. 
Run the command below
* python -m uvicorn api:app --reload --host 0.0.0.0 --port 8000 
To view the service of the FastAPI on [http://127.0.0.1:8000/docs]

## CI/CD
A workflow is provided in order to run it as a cronological job once per week every Sunday Midnight
(However currently it is commemended to save running minutes since it takes about 80 minutes to complete)
