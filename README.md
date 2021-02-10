# TimeScale Queries, aiohttp and asyncpg tutorials

## asyncpg and aiohttp video

https://www.youtube.com/watch?v=2utibYV3oxA

## TimeScale and PostgreSQL analytics and aggregates video

https://www.youtube.com/watch?v=BrYe-4QWjdc

## Queries

### Find a Stock We are Interested In

select * from stock where symbol = 'TWTR';

### Look at the Price Data We Retrieved, observe that we have 5 minute 

select * from stock_price where stock_id = 15117;
select * from stock_price where stock_id = 15117 and date(dt) = '2021-01-25';

## Delete pre-market and after hours

delete from stock_price where dt::timestamp::time < '06:30:00' or dt::timestamp::time >= '13:00:00';

## What was the high and low for Twitter in our database?

select max(high)
from stock_price
where stock_id = 15117;

select min(low)
from stock_price
where stock_id = 15117;

## What was the price at when it was added to ETF database?

select first(open, dt)
from stock_price
where stock_id = 15117
and date(dt) = '2021-01-26';

## What did it close at?

select last(close, dt)
from stock_price
where stock_id = 15117;

## What are some low volume stocks held by ARK ETFs?

select stock_id, symbol, sum(volume) as total_volume
from stock_price join stock on stock.id = stock_price.stock_id
where date(dt) = '2021-01-29'
group by stock_id, symbol
order by total_volume asc LIMIT 10;

## Histogram - How many times did Twitter close below 50, above 52, or anywhere in between?

SELECT histogram(close, 50, 52, 4)
FROM stock_price
WHERE stock_id = 15117;

          histogram
-----------------------------
 {8481,428,272,439,256,2138}

select count(*) from stock_price where stock_id = 15117 and close < 50;
select count(*) from stock_price where close >= 50 and close < 50.5 and stock_id = 15117;
select count(*) from stock_price where stock_id = 15117 and close > 52;

## Time bucketing functions - get hourly bars 

select time_bucket(INTERVAL '1 hour', dt) AS bucket, first(open, dt), max(high), min(low), last(close, dt)
from stock_price 
where stock_id = 15117
group by bucket
order by bucket desc;

select time_bucket(INTERVAL '20 minute', dt) AS bucket, first(open, dt), max(high), min(low), last(close, dt)
from stock_price 
where stock_id = 15117
group by bucket
order by bucket desc;

## Filling Gaps / Missing Prices

SELECT time_bucket_gapfill('5 min', dt, now() - INTERVAL '5 day', now()) AS bar, avg(close) as close
FROM stock_price
WHERE stock_id = 7502 and dt > now () - INTERVAL '5 day'
group by bar, stock_id
order by bar;

## Last Observation Carried Forward

SELECT time_bucket_gapfill('5 min', dt, now() - INTERVAL '5 day', now()) AS bar, locf(avg(close)) as close2
FROM stock_price
WHERE stock_id = 7502 and dt > now () - INTERVAL '5 day'
group by bar, stock_id
order by bar;

## Materialized Views

CREATE MATERIALIZED VIEW hourly_bars
WITH (timescaledb.continuous) AS
SELECT stock_id,
       time_bucket(INTERVAL '1 hour', dt) AS day,
       first(open, dt) as open,
       MAX(high) as high,
       MIN(low) as low,
       last(close, dt) as close,
       SUM(volume) as volume
FROM stock_price
GROUP BY stock_id, day;

CREATE MATERIALIZED VIEW daily_bars
WITH (timescaledb.continuous) AS
SELECT stock_id,
       time_bucket(INTERVAL '1 day', dt) AS day,
       first(open, dt) as open,
       MAX(high) as high,
       MIN(low) as low,
       last(close, dt) as close,
        SUM(volume) as volume
FROM stock_price
GROUP BY stock_id, day;

## Show hourly bars for a particular stock

SELECT * 
FROM hourly_bars
WHERE stock_id = 15117
ORDER BY hour desc;

## Show daily bars for last 21 days

SELECT * 
FROM daily_bars
WHERE day > (now() - interval '21 days')
AND stock_id = 15117
ORDER BY day;

# 20 Day Moving Average

SELECT avg(close) 
FROM (
  SELECT * FROM daily_bars WHERE stock_id = 15117 ORDER BY day DESC LIMIT 20
) a;

## Window Function - performs calculation across a set of table rows that are related to the current row

Moving average with Window Functions

SELECT day, AVG(close) OVER (ORDER BY day ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS sma_20
  FROM daily_bars
  WHERE stock_id = 15117
  ORDER BY day DESC;

## highest daily returns

WITH prev_day_closing AS (
  SELECT stock_id, day, close,
    LEAD(close) OVER (PARTITION BY stock_id ORDER BY day DESC) AS prev_day_closing_price
  FROM daily_bars
), daily_factor AS (
  SELECT stock_id, day, close / prev_day_closing_price AS daily_factor
  FROM prev_day_closing
)
SELECT day, LAST(stock_id, daily_factor) AS stock_id,
   MAX(daily_factor) AS max_daily_factor
FROM
   daily_factor JOIN stock ON stock.id = daily_factor.stock_id
GROUP BY
   day
ORDER BY day DESC, max_daily_factor DESC;

## select all closes for Friday vs. Previous Close

SELECT stock_id, symbol, day, close, LAG (close,1) OVER (ORDER BY close ASC) AS previous_close 
FROM daily_bars JOIN stock ON stock.id = daily_bars.stock_id
WHERE date(day) = '2021-02-05'
ORDER BY stock_id, day;

## Stocks that went down yesterday

SELECT * FROM (
  SELECT stock_id, day, close, 
    LEAD (close,1) OVER (PARTITION BY stock_id ORDER BY day DESC) AS previous_close
  FROM daily_bars
) a
WHERE close < previous_close 
AND date(day) = '2021-02-05';

## Stocks that sold off into the close (last 5 minutes of trading)

SELECT * FROM (
  SELECT stock_id, dt, close, 
    LEAD (close,1) OVER (PARTITION BY stock_id ORDER BY dt DESC) AS previous_close
  FROM stock_price
) a
WHERE close < previous_close 
AND dt = '2021-02-05 12:55:00';

## Bullish Engulfing Pattern

SELECT * FROM (
  SELECT
      day, open, close, stock_id,
      LAG(close, 1) OVER (
          PARTITION BY stock_id
          ORDER BY day
      ) previous_close,
      LAG(open, 1) OVER (
          PARTITION BY stock_id
          ORDER BY day
      ) previous_open
  FROM
      daily_bars
) a
WHERE previous_close < previous_open 
AND close > previous_open AND open < previous_close  
AND day = '2021-02-05';

#### Add Gamestop, Refresh Continous Aggregate

CALL refresh_continuous_aggregate('hourly_bars', '2020-10-01', '2021-03-01');
CALL refresh_continuous_aggregate('daily_bars', '2020-10-01', '2021-03-01');

### Closed higher 3 times on higher volume

SELECT * FROM (
  SELECT
      day, 
      close,
      volume,
      stock_id,
      LAG(close, 1) OVER (
          PARTITION BY stock_id
          ORDER BY day
      ) previous_close,
      LAG(volume, 1) OVER (
          PARTITION BY stock_id
          ORDER BY day
      ) previous_volume,
      LAG(close, 2) OVER (
          PARTITION BY stock_id
          ORDER BY day
      ) previous_previous_close,
      LAG(volume, 2) OVER (
          PARTITION BY stock_id
          ORDER BY day
      ) previous_previous_volume
  FROM
      daily_bars
) a
WHERE close > previous_close AND previous_close > previous_previous_close 
AND volume > previous_volume AND previous_volume > previous_previous_volume 
AND day = '2021-01-22';

## Gamestonk Query

SELECT * FROM (
  SELECT
      day, 
      close,
      volume,
      stock_id,
      LAG(close, 1) OVER (
          PARTITION BY stock_id
          ORDER BY day
      ) previous_close,
      LAG(volume, 1) OVER (
          PARTITION BY stock_id
          ORDER BY day
      ) previous_volume,
      LAG(close, 2) OVER (
          PARTITION BY stock_id
          ORDER BY day
      ) previous_previous_close,
      LAG(volume, 2) OVER (
          PARTITION BY stock_id
          ORDER BY day
      ) previous_previous_volume,
      LAG(close, 3) OVER (
          PARTITION BY stock_id
          ORDER BY day
      ) previous_previous_previous_close,
      LAG(volume, 3) OVER (
          PARTITION BY stock_id
          ORDER BY day
      ) previous_previous_previous_volume
  FROM
      daily_bars
) a
WHERE close > previous_close AND previous_close > previous_previous_close AND previous_previous_close > previous_previous_previous_close
AND volume > previous_volume AND previous_volume > previous_previous_volume AND previous_previous_volume > previous_previous_previous_volume
AND day = '2021-02-05';


## Three Bar Breakout

SELECT * FROM (
  SELECT
      day, 
      close,
      volume,
      stock_id,
      LAG(close, 1) OVER (
          PARTITION BY stock_id
          ORDER BY day
      ) previous_close,
      LAG(volume, 1) OVER (
          PARTITION BY stock_id
          ORDER BY day
      ) previous_volume,
      LAG(close, 2) OVER (
          PARTITION BY stock_id
          ORDER BY day
      ) previous_previous_close,
      LAG(volume, 2) OVER (
          PARTITION BY stock_id
          ORDER BY day
      ) previous_previous_volume,
      LAG(close, 3) OVER (
          PARTITION BY stock_id
          ORDER BY day
      ) previous_previous_previous_close,
      LAG(volume, 3) OVER (
          PARTITION BY stock_id
          ORDER BY day
      ) previous_previous_previous_volume
  FROM
      daily_bars
) a
WHERE close > previous_previous_previous_close and previous_close < previous_previous_close and previous_close < previous_previous_previous_close
AND volume > previous_volume and previous_volume < previous_previous_volume and previous_previous_volume < previous_previous_previous_volume
AND day = '2021-02-05';
