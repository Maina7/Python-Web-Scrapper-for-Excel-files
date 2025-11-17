from bs4 import BeautifulSoup
import requests
import pandas as pd
from dotenv import load_dotenv
import os
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import logging
import re

logging.basicConfig(
    level = logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scrapper.log'),
        logging.StreamHandler()
    ]
)

class MarketPriceScrapper:
    def __init__(self):
        load_dotenv()
        self.url = os.getenv("SAMPLE_LINK")
        self.db_config = {
            'host':os.getenv("DB_HOST"),
            'dbname':os.getenv("DB_NAME"),
            'user':os.getenv("DB_USER"),
            'password': os.getenv("DB_PASSWORD"),
            'port': os.getenv("PORT")
        }
        self.conn = None
        self.cur = None

    def connect_db(self):
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cur = self.conn.cursor()
            logging.info("Database connection established")
        
        except Exception as e:
            logging.error(f"Database Connection failed: {e}")
            raise
    
    def close_db(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        logging.info("Database connection closed")

    def scrap_data(self):
        try:
            response = requests.get(self.url, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.text,"html.parser")
            table = soup.find("table")

            if not table:
                raise Exception("No table found on the page")
            headers = [th.text.strip() for th in table.find_all("th")]

            rows= []
            for tr in table.find_all("tr")[1:]:
                columns = [td.text.strip() for td in tr.find_all("td")]
                if columns:
                    rows.append(columns)
            df = pd.DataFrame(rows, columns=headers)

            return df
        except Exception as e:
            logging.error(f"Scrapping failed: {e}")
            raise

    def clean_data(self,df):
        try:
            df["wholesale_price"] = df["Wholesale"]
            df["retail_price"] = df["Retail"]
            df["supply_volume"]= df["Supply Volume"]
            df["county"] = df["County"]
            df["commodity"] = df["Commodity"]

            df["price_date"] = pd.to_datetime(df["Date"])

            df_clean = df[[
                'Commodity', 'Classification', 'Grade', 'Sex', 
                'Market', 'wholesale_price','retail_price', 'supply_volume','county','price_date'
            ]].copy()

            df_clean.columns = [
                'commodity', 'classification', 'grade', 'sex', 
                'market', 'wholesale_price','retail_price', 'supply_volume','county','price_date'
            ]
            df_clean = df_clean.dropna(subset = ["price_date"])
            return df_clean
            
        except Exception as e:
            logging.error(f"{e}")

    def save_to_db(self,df):
        try:
            records = [
                (
                    row['commodity'],
                    row['classification'],
                    row['grade'],
                    row['sex'],
                    row['market'],
                    row['wholesale_price'],
                    row['retail_price'],
                    row['supply_volume'],
                    row['county'],
                    row['price_date']
                )
                for _, row in df.iterrows()
            ]
            insert_query = """INSERT INTO market_prices(commodity,classification,grade,sex,market,wholesale_price,retail_price,supply_volume,county,price_date) VALUES %s ON CONFLICT DO NOTHING"""
            execute_values(self.cur,insert_query,records)
            inserted = self.cur.rowcount
            self.conn.commit()

            logging.info(f"{inserted}")

            return inserted
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Database insertion failed {e}")
        
        
    def run(self):
        try:
            self.connect_db()

            df_raw = self.scrap_data()
            df_clean = self.clean_data(df_raw)

            inserted = self.save_to_db(df_clean)

            return True
        except Exception as e:
            logging.error(f"{e}")
            return False
        finally:
            self.close_db()

if __name__ == "__main__":
    scrapper = MarketPriceScrapper()
    success = scrapper.run()
    exit(0 if success else 1)

            

            


