
FROM python:3.8-slim-buster
LABEL maintainer="genuchten@yahoo.com"

RUN apt-get update && apt-get install --yes \
        ca-certificates libexpat1 \
    && rm -rf /var/lib/apt/lists/*

RUN adduser --uid 1000 --gecos '' --disabled-password linky

ENV OGCAPI_URL=http://localhost
ENV OGCAPI_COLLECTION=metadata:main
ENV ROOTPATH=/
ENV POSTGRES_HOST=host.docker.internal
ENV POSTGRES_PORT=5432
ENV POSTGRES_DB=postgres
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=******

WORKDIR /home/link-liveliness-assessment

RUN chown --recursive linky:linky .

# initially copy only the requirements files
COPY --chown=linky \
    requirements.txt \
    ./

RUN pip install -U pip && \
    python3 -m pip install \
    -r requirements.txt \
    psycopg2-binary  

COPY --chown=linky . .

WORKDIR /home/link-liveliness-assessment/src

EXPOSE 8000

USER linky

ENTRYPOINT [ "python3", "-m", "uvicorn", "api:app", "--reload", "--host", "0.0.0.0", "--port", "8000" ]