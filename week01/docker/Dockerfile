# When this image is run, it will open with the bash prompt in 
# the /app directory. The pipline.py script can be run via bash
# by running `python pipeline.py`. 

FROM python:3.9

RUN pip install pandas

WORKDIR /app
COPY pipeline.py pipeline.py

ENTRYPOINT [ "bash" ]
