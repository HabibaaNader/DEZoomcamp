import pandas as pd
from sqlalchemy import create_engine
import argparse

def main(params):

    pg_user = params.user
    pg_password = params.password
    pg_host = params.host
    pg_port=params.port
    pg_db=params.db
    csv = params.csv
    pg_table = params.table

    engine = create_engine(f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}")
    con = engine.connect()

    df_iter = pd.read_csv(csv, iterator=True, chunksize=100000)
    df = next(df_iter)

    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

    df.head(0).to_sql(con = con, name = pg_table, if_exists="replace")

    df.to_sql(con = con, name = pg_table, if_exists="append")

    try:
        while True:
            # Fetch the next chunk of data
            df = next(df_iter)

            # Convert datetime columns
            df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
            df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

            # Append the chunk to the SQL table
            df.to_sql(con=con, name=pg_table, if_exists="append", index=False)

    except StopIteration:
        print("Data loading complete.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__" :
    parser = argparse.ArgumentParser(description='Ingesting data into postgres database')

    parser.add_argument('--user') 
    parser.add_argument('--password')
    parser.add_argument('--host')
    parser.add_argument('--port')
    parser.add_argument('--db')
    parser.add_argument('--csv')
    parser.add_argument('--table')
    
    params = parser.parse_args()
    main(params)






