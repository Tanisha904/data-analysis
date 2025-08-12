import sqlite3
import pandas as pd
import logging
from ingestion_db import ingest_db

logging.basicConfig(
    filename = "log/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - (message)s",
    filemode="a"
)

def create_vendor_summary(conn):
    vendor_sales_summary=pd.read_sql_query("""WITH freightSummary AS(
    SELECT
        VendorNumber,
        SUM(freight)AS FreightCost
    from vendor_invoice
    GROUP BY VendorNumber
    ),
    
  
purchases_Summary AS(
SELECT
    purchases.VendorNumber,
    purchases.VendorName,
    purchases.Brand,
    purchases.Description,
    purchases.PurchasePrice,
    purchase_prices.Volume,
    purchase_prices.Price as ActualPrice,
    SUM(purchases.Quantity)as TotalPurchaseQuantity,
    SUM(purchases.Dollars)as TotalPurchaseDollars
    FROM purchases 
    JOIN purchase_prices 
    ON purchases.Brand = purchase_prices.Brand 
    WHERE purchases.PurchasePrice > 0
    GROUP BY purchases.VendorNumber,purchases.VendorName,purchases.Brand, purchases.Description, purchase_prices.Price, purchase_prices.Volume
    ),

Sales_Summary AS(
    SELECT
        VendorNo,
        Brand,
        SUM(SalesQuantity) AS TotalSalesQuantity,
        SUM(SalesDollars) AS TotalSalesDollars,
        SUM(SalesPrice) AS TotalSalesPrice,
        SUM(ExciseTax) AS TotalExciseTax
        FROM sales
        GROUP BY VendorNo ,Brand
)
   SELECT
          purchases_Summary.VendorNumber,
          purchases_Summary.VendorName,
          purchases_Summary.Brand,
          purchases_Summary.Description,
          purchases_Summary.PurchasePrice,
          purchases_Summary.ActualPrice,
          purchases_Summary.Volume,
          purchases_Summary.TotalPurchaseQuantity,
          purchases_Summary.TotalPurchaseDollars,
          Sales_Summary.TotalSalesQuantity,
          Sales_Summary.TotalSalesDollars,
          Sales_Summary.TotalSalesPrice,
          Sales_Summary.TotalExciseTax,
          FreightSummary.FreightCost
        FROM purchases_Summary 
        LEFT JOIN Sales_Summary 
          ON purchases_Summary.VendorNumber = Sales_Summary. VendorNo
          AND purchases_Summary.Brand = Sales_summary.Brand
        LEFT JOIN FreightSummary 
           ON purchases_Summary.VendorNumber = FreightSummary.VendorNumber
        ORDER BY purchases_Summary.TotalPurchaseDollars DESC""",conn)

    return vendor_sales_summary

def clean_data(df):
    df['Volume'] = df['Volume'].astype('float')

    df.fillna(0,inplace =True)

    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()

    vendor_sales_summary['grossProfit'] = vendor_sales_summary['TotalSalesDollars']- vendor_sales_summary['TotalPurchaseDollars']
    vendor_sales_summary['ProfitMargin'] = (vendor_sales_summary['GrossProfit']/vendor_sales_summary['TotalSalesDollars'])-100
    vendor_sales_summary['StockTurnover'] = vendor_sales_summary['TotalSalesQuantity']/ vendor_sales_summary['TotalPurchaseQuantity']
    vendor_sales_summary['SalesToPurchaseRatio'] = vendor_sales_summary['TotalSalesDollars']/ vendor_sales_summary['TotalPurchaseDollars']

    return df

if __name__ == '__main__':
    conn = sqlite3.connect('inventory.db')

    logging.info('creating Vendor Summary Table.....')
    summary_df= create_vendor_summary(conn)
    logging.info(summary_df.head())

    logging.info('cleaning Data.....')
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info('ingesting data.....')
    ingest_db(clean_df,'Vendor_sales_summary',conn)
    logging.info('completed')