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
