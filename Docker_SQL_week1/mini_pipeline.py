import pandas as pd
from sqlalchemy import create_engine

def main():
    df_taxi_zone = pd.read_csv('taxi_zone_lookup.csv')
    df_tripdata = pd.read_parquet("green_tripdata_2025-11.parquet", )

    pg_connection_string = "postgresql://postgres:postgres@db:5432/ny_taxi"
    engine = create_engine(pg_connection_string)
    engine.connect()
    
    print("Connected to the database successfully.")

    df_taxi_zone.to_sql(name='taxi_zone', con=engine, if_exists='replace')
    print("Taxi zone data inserted successfully.")
    df_tripdata.to_sql(name='tripdata', con=engine, if_exists='replace')
    print("Trip data inserted successfully.")




if __name__ == "__main__":
    main()
