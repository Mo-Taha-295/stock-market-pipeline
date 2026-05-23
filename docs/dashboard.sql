-- 1. Close Price Over Time
SELECT timestamp, close
FROM public.stock_prices_dw
ORDER BY timestamp;

-- 2. Daily Return %
SELECT 
    timestamp,
    ROUND(((close - open) / open * 100)::numeric, 2) as daily_return_pct
FROM public.stock_prices_dw
ORDER BY timestamp;

-- 3. 7D Moving Average
SELECT
    timestamp,
    close,
    ROUND(AVG(close) OVER (
        ORDER BY timestamp
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )::numeric, 2) as moving_avg_7d
FROM public.stock_prices_dw
ORDER BY timestamp;