

# SoilWise-he Link Liveliness Assessment

The SoilWise-he project aims to develop an open access knowledge and data metadata catalogue to safeguard soils. This repository contains the Link Liveliness Assessment (LLA) component, which monitors and validates the links contained within metadata harvested for the SoilWise Repository (SWR).

## Features
- **Link validation** — Checks each link for availability and collects metadata such as file format, size, and last modification date
- **Broken link categorization** — Classifies broken links by error type: Redirection, Client, or Server errors
- **Deprecated link handling** — Marks links as deprecated after 10 consecutive failures
- **Polite crawling** — Links are grouped per site; each site is checked one link at a time, respecting its `robots.txt` crawl-delay (default 1s, max 10s). `429 Too Many Requests` pauses the site (Retry-After, max 60s) and is not counted as a failure
- **Lookup services** — DOI, Handle and w3id links are checked at the resolver (its 3xx answer) without following the redirect
- **OWS service support** — Detects OGC web services (WMS, WMTS, WFS, WCS, OGC API) and fetches their GetCapabilities every 30 days, or when a plain HEAD/GET fails, so working services are not reported as broken
- **On-demand validation** — Supports ad-hoc link checks that return instant results without writing to the database
- **Availability history** — Builds a status history for each URL when run periodically

## Installation

### Using Docker (recommended)

Run the api (set db connection details in environment)
```
docker run -p 8000:8000 ghcr.io/soilwise-he/link-liveliness-assessment
```

Run the task (set db connection details in environment)
```
docker run --rm --entrypoint python3 ghcr.io/soilwise-he/link-liveliness-assessment linkcheck/linkchecker.py
```

### Local setup
```bash
pip install -r requirements.txt
```
Then set up your `.env` file and ensure PostgreSQL is running and accessible.

### Configuration (`.env`)

| Variable | Default | Description |
|---|---|---|
| `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` | — | Database connection (required) |
| `POSTGRES_SCHEMA` | `public` | Schema for the LLA tables |
| `STOREINDB` | `true` | Save results to the database |
| `FULL_REFRESH` | `false` | `true` empties `links` and `validation_history` before the run. Use for a single run, then set back to `false` |

Data is kept between runs, so each run builds on the previous one. Existing databases created by an older version need:
```sql
ALTER TABLE links ADD COLUMN caps_last_checked TIMESTAMP;
```

## Usage

The LLA component runs automatically as a **weekly CI/CD pipeline**. It can also be triggered manually or used via its FastAPI endpoints.

### Available endpoints

**Check a specific URL on-demand** (no database storage):
```bash
curl -X POST http://<host>:<port>:/check-url \
  -H "Content-Type: application/json" \
  -d '{"url": "https://example.com/dataset", "check_ogc_capabilities": false}'
```
Returns status code, content type, file size, redirect info, and a diagnostic message. Private/internal addresses (localhost, `10.x`, `192.168.x`, ...) are refused.

**Query broken links by error type:**
```bash
curl http://<host>:<port>:/Redirection_URLs/3xx
curl http://<host>:<port>:/Client_Error_URLs/4xx
curl http://<host>:<port>:/Server_Errors_URLs/5xx
curl http://<host>:<port>:/Timeout_URLs
```
4xx and 5xx only list links that are currently failing.

**Number of links per site** (total and deprecated, largest first):
```bash
curl "http://<host>:<port>:/Links_per_site?limit=100"
```

**Check the status of a specific URL:**
```bash
curl http://<host>:<port>:/status/https://example.com
```

**View the full validation history of a URL:**
```bash
curl "http://<host>:<port>:/URL_status_history?url=https://example.com/dataset&limit=100"
```

**List all deprecated URLs:**
```bash
curl http://<host>:<port>:/Deprecated_URLs
```

The response includes status code, content metadata, redirect information, and diagnostic messages.
### API fields

| Field | Description |
|---|---|
| `link_type` | File format of the resource (e.g., `image/jpeg`, `application/pdf`) |
| `link_size` | Size of the resource in bytes |
| `last_modified` | Timestamp of the resource's last modification |
| `gis_capabilities` | GetCapabilities summary for OGC services (layers, bbox, CRS, ...) |
| `caps_last_checked` | When `gis_capabilities` was last fetched successfully |

### Main Component Diagram
```mermaid
flowchart LR
    H["Harvester"]-- "writes" -->MR[("Record Table")]
    MR-- "reads" -->LAA["Link Liveliness Assessment"]
    MR-- "reads" -->CA["Catalogue"]
    LAA-- "writes" -->LLAL[("Links Table")]
    LAA-- "writes" -->LLAVH[("Validation History Table")]
    CA-- "reads" -->API["API"]
    LLAL-- "writes" -->API
    LLAVH-- "writes" -->API
```

### Database design
```mermaid
classDiagram
    Links <|-- Validation_history
    Links <|-- Records
    Links : +Int ID
    Links : +Int fk_records
    Links : +String Urlname
    Links : +String deprecated
    Links : +String link_type
    Links : +Int link_size
    Links : +DateTime last_modified
    Links : +String Consecutive_failures
    Links : +JSONB gis_capabilities
    Links : +DateTime caps_last_checked
    class Records{
    +Int ID
    +String Records
    }
    class Validation_history{
      +Int ID
      +Int fk_link
      +String Statuscode
     +String isRedirect
     +String Errormessage
     +Date Timestamp
    }
```

## Additional Information

### Architecture

The LLA component is built with the following stack:

| Technology | Role |
|---|---|
| Python | Linkchecker integration, API development, database interactions |
| PostgreSQL | Primary database for links and validation history |
| FastAPI | REST API with auto-generated Swagger documentation |
| Docker | Containerized deployment |
| CI/CD | Automated weekly pipeline runs |

### Database Design

**Links table** — stores URL metadata per record: `ID`, `fk_records`, `Urlname`, `deprecated`, `link_type`, `link_size`, `last_modified`, `Consecutive_failures`, `gis_capabilities`, `caps_last_checked`

**Validation_history table** — stores per-check results: `ID`, `fk_link`, `Statuscode`, `isRedirect`, `Errormessage`, `Timestamp`

**Records table** — source metadata records: `ID`, `Records`

### Key Design Decisions

- Only links in the `ogc-api:records` links section are tested (not links embedded in abstracts) to avoid redundant checks across pages.
- OGC services are handled with a dedicated script that appends required parameters before validation.
- DOI, Handle and w3id links are validated at the resolver only; a 3xx answer means the identifier is registered.
- Links that fail repeatedly are marked as deprecated and no longer added to the validation history.
- Each link is associated with the record(s) that reference it, enabling targeted notifications when a broken link is found.
- A front-end widget displays the status of each link giving users directl feedback on resource availability.
---
## SoilWise-he Project

This work has been initiated as part of the [SoilWise-he](https://soilwise-he.eu) project. The project receives funding from the European Union's HORIZON Innovation Actions 2022 under grant agreement No. 101112838. Views and opinions expressed are however those of the author(s) only and do not necessarily reflect those of the European Union or Research Executive Agency. Neither the European Union nor the granting authority can be held responsible for them.
