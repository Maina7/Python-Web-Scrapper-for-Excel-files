from fastapi import FastAPI, HTTPException,Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
from datetime import date,datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import os

load_dotenv()

#initializing fasstapi
app = FastAPI(
    title="Agricultural Market Prices API",
    description="Public API for querying agricultural commodity prices in Kenya",
    version="1.0.0"
)
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "dbname":os.getenv("DB_NAME"),
    "user":os.getenv("DB_USER"),
    "password":os.getenv("DB_PASSWORD"),
    "port":os.getenv("PORT")
}
class PriceRecord(BaseModel):
    id: int
    commodity:str
    classification: Optional[str]
    grade: Optional[str]
    sex:Optional[str]
    market:str
    wholesale_price: Optional[str]
    retail_price: Optional[str]
    supply_volume: Optional[str]
    county: str
    price_date:date
    created_at: datetime

class PriceStats(BaseModel):
    commodity: str
    county: Optional[str]
    avg_wholesale: Optional[str]
    avg_retail: Optional[str]
    min_wholesale: Optional[str]
    max_wholesale: Optional[str]
    min_retail: Optional[str]
    max_retail: Optional[str]
    total_volume: Optional[str]
    record_count: int

def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")
    

@app.get("/")
def read_root():
    return {
        "message": "Agricultural Market Prices API",
        "version": "1.0.0",
        "endpoints": {
            "prices": "/api/prices",
            "latest": "/api/prices/latest",
            "stats": "/api/stats",
            "commodities": "/api/markets",
            "counties": "/api/counties"
        }
    }
@app.get("/api/prices", response_model=List[PriceRecord])
def get_prices(
    commodity: Optional[str] = Query(None, description="Filter by commodity name"),
    market: Optional[str] = Query(None, description="Filter by market name"),
    county: Optional[str] = Query(None, description="Filter by county"),
    start_date: Optional[str] = Query(None, description="start date (DD-MM-YYYY)"),
    end_date: Optional[str] = Query(None, description="End date (DD-MM-YYYY)"),
    limit: int = Query(100, ge=1, le=3000, description="Number of records to return"),
    offset: int = Query(0,ge=0,description="Number of records to skip")):
    
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        query = "SELECT * FROM market_prices WHERE 1=1"
        params = []

        if commodity:
            query += " AND market ILIKE %s"
            params.append(f"%{market}")
        if market:
            query += "AND market ILIKE %s"
            params.append(f"%{market}")
        if county:
            query += "AND county ILIKE %s"
            params.append(f"%{county}")
        if start_date:
            query += " AND price_date >= %s"
            params.append(start_date)        
        if end_date:
            query += " AND price_date <= %s"
            params.append(end_date)
        query += "ORDER BY price_date DESC, id DESC LIMIT %s OFFSET %s"
        params.extend([limit,offset])

        cur.execute(query,params)
        results = cur.fetchall()

        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()
        conn.close()
@app.get("/api/prices/latest", response_model=List[PriceRecord])
def get_latest_prices(
    commodity: Optional[str] = Query(None, description="Filter by commodity name"),
    
):

