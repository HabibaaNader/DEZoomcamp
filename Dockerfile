From python:3.9

WORKDIR /app
Run pip install pandas sqlalchemy psycopg2
COPY load_green_tripdata.py load_green_tripdata.py

ENTRYPOINT [ "python", "load_green_tripdata.py"]
