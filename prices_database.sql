CREATE TABLE IF NOT EXISTS market_prices (
id SERIAL PRIMARY KEY,
commodity VARCHAR(255) NOT NULL,
classification VARCHAR(100),
grade VARCHAR(100),
sex VARCHAR(50),
market VARCHAR(255) NOT NULL,
wholesale_price VARCHAR(100),
retail_price VARCHAR(100),
supply_volume VARCHAR(100),
county VARCHAR(100),
price_date DATE NOT NULL,
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_commodity ON market_prices(commodity);
CREATE INDEX idx_market ON market_prices(market);
CREATE INDEX idx_county ON market_prices(county);
CREATE INDEX idx_price_date ON market_prices(price_date);
CREATE INDEX idx_commodity_date ON market_prices(commodity,price_date);

CREATE OR REPLACE VIEW latest_prices AS SELECT DISTINCT ON (commodity,market,county) * FROM market_prices ORDER BY commodity, market,county,price_date DESC;