# When this image is run, it will use the entry point 
# python pipeline.py to run the pipeline.py script
# when the container is run

FROM python:3.9

RUN pip install pandas

WORKDIR /app
COPY pipeline.py pipeline.py

ENTRYPOINT [ "python", "pipeline.py" ]
