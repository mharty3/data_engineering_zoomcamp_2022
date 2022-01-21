# When this image is run, it will use the entry point 
# python pipeline.py to run the pipeline.py script
# when the container is run

FROM python:3.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py" ]
