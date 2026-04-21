import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

logging.basicConfig( # set up how the structure of the login will look like
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a",
)

# create a connection to the database (MySQL)
username = "root"
password = "MySQLpassword"
# host = "127.0.0.1"
host = "localhost"
port = "port#"
database = "vendors_performance"

engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}", 
                       pool_size=10, max_overflow=20, pool_pre_ping=True)

def ingest_db(df, table_name, engine):
    '''
    This function takes in a DataFrane into database table'''
    df.to_sql(table_name, con=engine, if_exists='replace', index=False, method='multi', chunksize=10000)
    
def load_raw_data():
    '''
    Load raw data from CSV files and ingest it into the database.
    '''
    start = time.time()
    for file in os.listdir('data'): # loop through files in the data directory
        if '.csv' in file:
            df = pd.read_csv(os.path.join('data', file))
            logging.info(f"Ingesting {file} with shape {df.shape} into the database.")
            ingest_db(df, file[:-4], engine)
            
    end = time.time()
    total_time = (end - start)/60
    logging.info("All files have been ingested into the database.")
    logging.info("=====================================================")
    
    logging.info(f"\nTotal time taken for ingestion is {total_time:.2f} minutes")
    
if __name__ == "__main__":
    load_raw_data()
