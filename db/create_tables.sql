
CREATE TABLE stock (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    name TEXT NOT NULL,
    exchange TEXT NOT NULL,
    is_etf BOOLEAN NOT NULL
);


CREATE TABLE etf_holding (
    etf_id INTEGER NOT NULL, 
    holding_id INTEGER NOT NULL,
    dt DATE NOT NULL, 
    shares NUMERIC,
    weight NUMERIC, 
    PRIMARY KEY (etf_id, holding_id, dt),
    CONSTRAINT fk_etf FOREIGN KEY (etf_id) REFERENCES stock (id),
    CONSTRAINT fk_holding FOREIGN KEY (holding_id) REFERENCES stock (id)
);

CREATE TABLE stock_price (
    stock_id INTEGER NOT NULL,
    dt TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    open NUMERIC (6, 2) NOT NULL, 
    high NUMERIC (6, 2) NOT NULL,
    low NUMERIC (6, 2) NOT NULL,
    close NUMERIC (6, 2) NOT NULL, 
    volume NUMERIC NOT NULL,
    PRIMARY KEY (stock_id, dt),
    CONSTRAINT fk_stock FOREIGN KEY (stock_id) REFERENCES stock (id)
);

CREATE INDEX ON stock_price (stock_id, dt DESC);

SELECT create_hypertable('stock_price', 'dt');


CREATE MATERIALIZED VIEW daily_bars
WITH (timescaledb.continuous) AS
SELECT stock_id,
       time_bucket(INTERVAL '1 day', dt) AS day,
       first(open, dt) as open,
       max(high) as high,
       min(low) as low,
       last(close, dt) as close,
       sum(volume) as volume
FROM stock_price
GROUP BY stock_id, day;


CREATE MATERIALIZED VIEW hourly_bars
WITH (timescaledb.continuous) AS
SELECT stock_id,
       time_bucket(INTERVAL '1 hour', dt) AS hour,
       first(open, dt) as open,
       max(high) as high,
       min(low) as low,
       last(close, dt) as close,
       sum(volume) as volume
FROM stock_price
GROUP BY stock_id, hour;