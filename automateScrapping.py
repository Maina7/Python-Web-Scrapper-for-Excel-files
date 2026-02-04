from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import pandas as pd
from dotenv import load_dotenv
import os
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import logging
import time
import glob

logging.basicConfig(
    level = logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("AutomatedScrapper.log"),
        logging.StreamHandler()
    ]
)

class EnhancedMarketPriceScrapper:
    def __init__(self):
        load_dotenv()
        self.url = os.getenv("SAMPLE_LINK")
        self.db_config = {
            "host":os.getenv("DB_HOST"),
            'dbname': os.getenv("DB_NAME"),
            'user': os.getenv("DB_USER"),
            'password': os.getenv("DB_PASSWORD"),
            'port': os.getenv("PORT")
        }
        self.products = [
            "Dry Maize",
            "Beans Red Haricot(Wairimu)",
            "Beans(Yellow-Green)",
            "Dolichos lablab(Njahi)",
            "Red Irish potato",
            "Cabbages",
            "Sweet potatoes",
            "Carrots",
            "Tomatoes",
            "Beans Rosecoco",
            "Eggs",
            "Meat Indiginous Chicken",
            "Avocado",
            "Mangoes",
            "Green Maize",
            "Spinach",
            "White Irish Potatoes",
            "Chillies",
            "Sheep",
            "Goat",
            "Donkey",
            "Pumkin",
            "Capsicums",
            "Cucumber",
            "Rabbit",
            "Rabbit Meat",
            "Pigs",
            "Chicken",
            "Dry Peas"
        ]
        self.conn = None
        self.cur = None
        self.driver = None
        self.download_dir = os.path.join(os.getcwd(),"downloads")

        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir)

    def setup_driver(self):
        try:
            chrome_options = Options()
            prefs = {
                "download.default_directory": self.download_dir,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled":True
            }
            chrome_options.add_experimental_option("prefs",prefs)

            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("disable-dev-shm-usage")

            self.driver = webdriver.Chrome(options=chrome_options)
            logging.info("Chrome driver initialized successfully")
        except Exception as e:
            logging.error(f"Failed to initialize Chrome driver: {e}")
            raise
    def connect_db(self):
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cur = self.conn.cursor()
            logging.info("Database connection established")
        except Exception as e:
            logging.error(f"Database connection failed: {e}")
            raise

    def close_db(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        logging.info("Database connection closed")
    
    def cleanup_downloads(self):
        try:
            files = glob.glob(os.path.join(self.download_dir, "*.xlsx"))
            for f in files:
                os.remove(f)
            logging.info(f"Cleaned up {len(files)} downloaded files")
        except Exception as e:
            logging.error(f"Error cleaning up downloads: {e}")
        
    def wait_for_download(self,timeout=30):
        seconds = 0
        while seconds< timeout:
            time.sleep(1)

            if not glob.glob(os.path.join(self.download_dir,"*.crdownload")):
                xlsx_files = glob.glob(os.path.join(self.download_dir, "*.xlsx"))
                if xlsx_files:
                    return xlsx_files[0]
            seconds += 1
        raise TimeoutError("Download did not finish within timeout period")
    
    def scrape_produce(self,produce_name):
        try:
            logging.info(f"Starting to scrape: {produce_name}")
            self.driver.get(self.url)
            wait = WebDriverWait(self.driver,20)
            product_dropdown = wait.until(
                EC.presence_of_all_elements_located((By.ID, "productname"))
            )
            select_product = Select(product_dropdown)
            select_product.select_by_visible_text(produce_name)
            logging.info(f"Selected product: {produce_name}")

            time.sleep(2)

            try:
                entries_dropdown = wait.until(
                    EC.presence_of_all_elements_located((By.NAME, "table_length"))
                )
                select_entries = Select(entries_dropdown)
                select_entries.select_by_visible_text("3000")
                logging.info("Set entries to 3000")
                time.sleep(2)
            except Exception as e:
                logging.warning(f"Could not set entries to 3000: {e}")

            export_button = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Export to Excel')]"))                
            )
            export_button.click()
            logging.info("Clicked Export to Excel button")

            downloaded_file = self.wait_for_download()
            logging.info(f"Download file: {downloaded_file}")

            df = pd.read_excel(downloaded_file)
            logging.info(f"read {len(df)} rows from Excel for {produce_name}")

            os.remove(downloaded_file)
            return df
        
        except Exception as e:
            logging.error(f"Failed to scrape {produce_name}: {e}")
            return None
        
    def clean_data(self,df):
        try:
            df_clean = df.copy()

            column_mapping = {
                'Commodity': 'commodity',
                'Classification': 'classification',
                'Grade': 'grade',
                'Sex': 'sex',
                'Market': 'market',
                'Wholesale': 'wholesale_price',
                'Retail': 'retail_price',
                'Supply Volume': 'supply_volume',
                'County': 'county',
                'Date': 'price_date'
            }
            df_clean = df_clean.rename(columns = column_mapping)

            columns_to_keep = [
                'commodity', 'classification', 'grade', 'sex', 
                'market', 'wholesale_price', 'retail_price', 
                'supply_volume', 'county', 'price_date'
            ]
            existing_columns = [col for col in columns_to_keep if col in df_clean.columns]
            df_clean = df_clean-existing_columns

            if 'price_date' in df_clean.columns:
                df_clean['price_date'] = pd.to_datetime(df_clean["price_date"], errors="coerce")
                df_clean = df_clean.dropna(subset=["price_date"])

            for col in columns_to_keep:
                if col not in df_clean.columns:
                    df_clean[col] = None
            logging.info(f"cleaned data: {len(df_clean)} rows remaining")
            return df_clean
        except Exception as e:
            logging.error(f"Data cleaning failed: {e}")
            raise
    
    def save_to_db(self,df):
        try:
            if df is None or len(df)==0:
                logging.warning("No data to save")
                return 0
            records = [(
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
            ) for _, row in df.interrows()]
            
            insert_query = """INSERT INTO market_prices(commodity, classification, grade, sex, market,wholesale_price, retail_price, supply_volume, county, price_date) VALUES %s ON CONFLICT DO NOTHING"""
            execute_values(self.cur, insert_query, records)
            inserted = self.cur.rowCount
            self.conn.commit()

            logging.info(f"Inserted {inserted} new records into database")
            return inserted
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Database insertion failed: {e}")
            raise
    def run(self):
        try:
            self.setup_driver()
            self.connect_db()
            self.cleanup_downloads()
            
            total_inserted = 0
            successful_products =0
            failed_products =[]

            for product in self.products:
                try:
                    logging.info(f"\n{'='*60}")
                    logging.info(f"Processing: {product}")
                    logging.info(f"{'='*60}")

                    df_raw = self.scrape_produce(product)

                    if df_raw is not None and len(df_raw)>0:
                        df_clean = self.clean_data(df_raw)

                        inserted = self.save_to_db(df_clean)
                        total_inserted += inserted
                        successful_products += 1

                        logging.info(f"Successfully processed {product}: {inserted} records inserted")
                    else:
                        failed_products.append(product)
                        logging.warning(f"No data retrived for {product}")

                    time.sleep(3)
                    
                except Exception as e:
                    failed_products.append(product)
                    logging.error(f"Failed to process {product}: {e}")
                    continue
            logging.info(f"\n{'='*60}")
            logging.info("SCRAPING SUMMARY")
            logging.info(f"{'='*60}")
            logging.info(f"Total products attempted: {len(self.products)}")
            logging.info(f"Successful: {successful_products}")
            logging.info(f"Failed: {len(failed_products)}")
            logging.info(f"Total records inserted: {total_inserted}")
            
            if failed_products:
                logging.info(f"Failed products: {', '.join(failed_products)}")
            
            return True
        except Exception as e:
            logging.error(f"Scrapping process failed: {e}")
            return False
        finally:
            if self.driver:
                self.driver.quit()
                logging.info("Chrome driver closed")
            self.close_db()
            self.cleanup_downloads()

if __name__ == "__main__":
    scrapper = EnhancedMarketPriceScrapper()
    success = scrapper.run()
    exit(0 if success else 1)




