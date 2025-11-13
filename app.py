from bs4 import BeautifulSoup
import requests
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

url = os.getenv("SAMPLE_LINK")
page = requests.get(url)
soup = BeautifulSoup(page.text,"html")

table = soup.find("table")
table_titles = soup.find_all("th")
table_title_text = [title.text for title in table_titles]

df = pd.DataFrame(columns=table_title_text)
column_daata = table.find_all("tr")

for row in column_daata[1:]:
    row_data = row.find_all("td")
    individual_raw_data = [data.text for data in row_data]

    length = len(df)
    df.loc[length] = individual_raw_data

df.to_csv(os.getenv("STORE_PATH"), index=False)


