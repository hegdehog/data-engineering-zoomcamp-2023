import pandas as pd
from sqlalchemy import create_engine
import argparse
import os
import pyarrow

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    source_file = 'output.parquet'
    
    # download parquet 
    os.system(f'wget ' + url + ' -o ' + source_file)

    conexion = 'postgresql://' + user + ':' + password + '@' + host + ':' + port + '/' + db
   
    engine = create_engine(conexion)

    # "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"
    df = pd.read_parquet(url)

    df.to_sql(name=table_name, con=engine, if_exists="append")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest Script')
    parser.add_argument('--user', help='User por Postgre SQL')
    parser.add_argument('--password', help='Pass por Postgre SQL')
    parser.add_argument('--host', help='Host por Postgre SQL')
    parser.add_argument('--port', help='Port por Postgre SQL')
    parser.add_argument('--db', help='Database por Postgre SQL')
    parser.add_argument('--table_name', help='Table por Postgre SQL')
    parser.add_argument('--url', help='Url for CSV file')

    args = parser.parse_args()

    main(args)
