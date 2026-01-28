#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from tqdm.auto import tqdm
from sqlalchemy import create_engine
import click

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

@click.command()
@click.option('--year', type=int, default=2021, help='Year of the data')
@click.option('--month', type=int, default=1, help='Month of the data')
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-pw', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', type=int, default=5432, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
@click.option('--chunk-size', type=int, default=100000, help='Chunk size for reading data')
def run(year, month, pg_user, pg_pw, pg_host, pg_port, pg_db, target_table, chunk_size):
    """Ingest NYC taxi data into PostgreSQL database"""
    prefix="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
    url=f"{prefix}/yellow_tripdata_{year:04d}-{month:02d}.csv.gz"

    engine = create_engine(f'postgresql://{pg_user}:{pg_pw}@{pg_host}:{pg_port}/{pg_db}')

    df_iter=pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunk_size
    )
    for i, df_chunk in enumerate(tqdm(df_iter)):
        if_exists = 'replace' if i == 0 else 'append'
        df_chunk.to_sql(name=target_table,con=engine,if_exists=if_exists)
        print(f"Inserted chunk {i+1} into the database.") 

if __name__ == "__main__":
    run()
