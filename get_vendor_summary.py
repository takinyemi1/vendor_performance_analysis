'''
Description: Performs task of creating vendor_sales_summary table.
run ingestion_db.py first and then get_vendor_summary.py
'''

import pandas as pd
import logging
from sqlalchemy import create_engine
from ingestion_db import ingest_db
import numpy as np

logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a",
)

# create a connection to the database (MySQL)
username = "root"
password = "msql_1851?Y!45"
host = "localhost"
port = "3306"
database = "vendors_performance"

engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}", 
                       pool_size=10, max_overflow=20, pool_pre_ping=True)

# create function called 'create_vendor_summary' taking the connection value as a variable input
def create_vendor_summary(connection):
    # this function will merge the different tables to get the overall vendor summary and adding new columns in the resultant data
    vendor_sales_summary = pd.read_sql_query("""WITH FreightSummary AS (
        SELECT
            VendorNumber,
            SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),
    
    PurchaseSummary AS (
        SELECT
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Price AS ActualPrice,
            pp.Volume,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars
        FROM purchases p
        JOIN purchase_prices pp
            ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Volume, pp.Price
        ),
        
        SalesSummary AS (
            SELECT
                VendorNo,
                Brand,
                SUM(SalesQuantity) AS TotalSalesQuantity,
                SUM(SalesDollars) AS TotalSalesDollars,
                SUM(SalesPrice) AS TotalSalesPrice,
                SUM(ExciseTax) AS TotalExciseTax
            FROM sales
            GROUP BY VendorNo, Brand
        )
        
        SELECT
            ps.VendorNumber,
            ps.VendorName,
            ps.Brand,
            ps.Description,
            ps.PurchasePrice,
            ps.ActualPrice,
            ps.Volume,
            ps.TotalPurchaseQuantity,
            ps.TotalPurchaseDollars,
            ss.TotalSalesQuantity,
            ss.TotalSalesDollars,
            ss.TotalSalesPrice,
            ss.TotalExciseTax,
            fs.FreightCost
        FROM PurchaseSummary ps
        LEFT JOIN SalesSummary ss
            ON ps.VendorNumber = ss.VendorNo
            AND ps.Brand = ss.Brand
        LEFT JOIN FreightSummary fs
            ON ps.VendorNumber = fs.VendorNumber
        ORDER BY ps.TotalPurchaseDollars DESC""", connection)
    
    return vendor_sales_summary

def clean_data(df):
    # this function will clean the data
    df['Volume'] = df['Volume'].astype('float') # change datatype to float
    
    # fill in the missing value(s) with 0
    df.fillna(0, inplace=True)
    
    # remove the whitespaces from categorical columns
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()
    
    # creating new columns for better analysis
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars']) * 100
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity']
    df['SalestoPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars']
    
    # Replace infinite values with 0 (MySQL doesn't support inf)
    df.replace([np.inf, -np.inf], 0, inplace=True)
    
    return df

if __name__ == '__main__':
    # create database connection
    with engine.connect() as connection:
        logging.info('Creating Vendor Summary Table...')
        summary_df = create_vendor_summary(connection)
        logging.info(summary_df.head())
        
        logging.info('Cleaning Data...')
        clean_df = clean_data(summary_df)
        logging.info(clean_df.head())
    
    logging.info('Ingesting data...')
    ingest_db(clean_df, 'vendor_sales_summary', engine)
    logging.info('Completed')
    