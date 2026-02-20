-- =========================================
-- Hive Schemas for Financial Data Lakehouse
-- =========================================

-- Create database
CREATE DATABASE IF NOT EXISTS financial_lakehouse
COMMENT 'Financial data lakehouse for stock market analysis'
LOCATION '/user/hive/warehouse/financial_lakehouse.db';

USE financial_lakehouse;

-- =========================================
-- RAW DATA TABLES (External Tables on HDFS)
-- =========================================

-- Stock Price Data (Polygon/Alpha Vantage)
CREATE EXTERNAL TABLE IF NOT EXISTS raw_stock_prices (
    ticker STRING COMMENT 'Stock ticker symbol',
    date DATE COMMENT 'Trading date',
    open DOUBLE COMMENT 'Opening price',
    high DOUBLE COMMENT 'Highest price',
    low DOUBLE COMMENT 'Lowest price',
    close DOUBLE COMMENT 'Closing  price',
    volume BIGINT COMMENT 'Trading volume',
    vwap DOUBLE COMMENT 'Volume weighted average price',
    num_transactions INT COMMENT 'Number of transactions',
    price_range DOUBLE COMMENT 'High - Low',
    price_change DOUBLE COMMENT 'Close - Open',
    price_change_pct DOUBLE COMMENT 'Price change percentage',
    timestamp BIGINT COMMENT 'Unix timestamp',
    extracted_at TIMESTAMP COMMENT 'Data extraction timestamp'
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET
LOCATION '/data/processed/polygon'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- News Sentiment Data
CREATE EXTERNAL TABLE IF NOT EXISTS raw_news_sentiment (
    ticker STRING COMMENT 'Stock ticker symbol',
    title STRING COMMENT 'News article title',
    url STRING COMMENT 'Article URL',
    published_at TIMESTAMP COMMENT 'Publication timestamp',
    summary STRING COMMENT 'Article summary',
    sentiment_score DOUBLE COMMENT 'Sentiment score',
    sentiment_label STRING COMMENT 'Sentiment label (Bullish/Bearish/Neutral)',
    source STRING COMMENT 'News source',
    date DATE COMMENT 'Publication date',
    extracted_at TIMESTAMP COMMENT 'Data extraction timestamp'
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION '/data/processed/news_sentiment'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- SEC Filing Data
CREATE EXTERNAL TABLE IF NOT EXISTS raw_sec_filings (
    ticker STRING COMMENT 'Stock ticker symbol',
    cik STRING COMMENT 'Central Index Key',
    accession_number STRING COMMENT 'Filing accession number',
    filing_date DATE COMMENT 'Filing date',
    report_date DATE COMMENT 'Report date',
    form_type STRING COMMENT 'Form type (10-K, 10-Q, 8-K, etc.)',
    primary_document STRING COMMENT 'Primary document filename',
    document_description STRING COMMENT 'Document description',
    extracted_at TIMESTAMP COMMENT 'Data extraction timestamp'
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION '/data/processed/sec'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- =========================================
-- PROCESSED DATA TABLES
-- =========================================

-- Stock Prices with Technical Indicators
CREATE TABLE IF NOT EXISTS stock_prices_with_indicators (
    ticker STRING,
    date DATE,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume BIGINT,
    vwap DOUBLE,
    price_change_pct DOUBLE,
    sma_7 DOUBLE COMMENT '7-day simple moving average',
    sma_20 DOUBLE COMMENT '20-day simple moving average',
    sma_50 DOUBLE COMMENT '50-day simple moving average',
    sma_200 DOUBLE COMMENT '200-day simple moving average',
    ema_12 DOUBLE COMMENT '12-day exponential moving average',
    ema_26 DOUBLE COMMENT '26-day exponential moving average',
    bb_upper DOUBLE COMMENT 'Bollinger band upper',
    bb_middle DOUBLE COMMENT 'Bollinger band middle',
    bb_lower DOUBLE COMMENT 'Bollinger band lower',
    volume_sma_20 DOUBLE COMMENT '20-day volume average',
    momentum_1d DOUBLE COMMENT '1-day momentum',
    momentum_5d DOUBLE COMMENT '5-day momentum',
    momentum_20d DOUBLE COMMENT '20-day momentum',
    volatility_20d DOUBLE COMMENT '20-day volatility',
    relative_volume DOUBLE COMMENT 'Relative to 20-day average'
)
PARTITIONED BY (year INT, month INT)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB', 'orc.bloom.filter.columns'='ticker,date');

-- Aggregated Daily Sentiment
CREATE TABLE IF NOT EXISTS daily_sentiment_aggregates (
    ticker STRING,
    date DATE,
    news_count INT COMMENT 'Number of news articles',
    avg_sentiment DOUBLE COMMENT 'Average sentiment score',
    min_sentiment DOUBLE,
    max_sentiment DOUBLE,
    sentiment_stddev DOUBLE COMMENT 'Sentiment standard deviation',
    bullish_count INT,
    bearish_count INT,
    neutral_count INT,
    bullish_ratio DOUBLE,
    bearish_ratio DOUBLE,
    weighted_avg_sentiment DOUBLE COMMENT 'Time-weighted sentiment'
)
PARTITIONED BY (year INT, month INT)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');

-- Master Dataset (All data combined)
CREATE TABLE IF NOT EXISTS master_dataset (
    ticker STRING,
    date DATE,
    -- Price data
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume BIGINT,
    vwap DOUBLE,
    price_change_pct DOUBLE,
    -- Technical indicators
    sma_20 DOUBLE,
    sma_50 DOUBLE,
    bb_upper DOUBLE,
    bb_lower DOUBLE,
    momentum_1d DOUBLE,
    momentum_5d DOUBLE,
    volatility_20d DOUBLE,
    relative_volume DOUBLE,
    -- Sentiment data
    news_count INT,
    avg_sentiment DOUBLE,
    weighted_avg_sentiment DOUBLE,
    bullish_ratio DOUBLE,
    bearish_ratio DOUBLE,
    -- SEC data
    filing_count INT,
    is_filing_date INT,
    -- Metadata
    day_of_week INT
)
PARTITIONED BY (year INT, month INT)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB', 'orc.bloom.filter.columns'='ticker,date');

-- =========================================
-- ANALYSIS RESULTS TABLES
-- =========================================

-- Sentiment-Price Correlation by Ticker
CREATE TABLE IF NOT EXISTS sentiment_correlation_by_ticker (
    ticker STRING,
    sample_size BIGINT,
    sentiment_correlation DOUBLE,
    weighted_sentiment_correlation DOUBLE,
    bullish_ratio_correlation DOUBLE,
    news_volume_correlation DOUBLE,
    avg_sentiment DOUBLE,
    avg_next_day_change DOUBLE,
    analysis_date TIMESTAMP
)
STORED AS ORC;

-- Overall Sentiment-Price Correlation
CREATE TABLE IF NOT EXISTS sentiment_correlation_overall (
    total_samples BIGINT,
    overall_sentiment_correlation DOUBLE,
    overall_weighted_correlation DOUBLE,
    overall_bullish_correlation DOUBLE,
    analysis_date TIMESTAMP
)
STORED AS ORC;

-- Sentiment-Driven Events
CREATE TABLE IF NOT EXISTS sentiment_driven_events (
    ticker STRING,
    date DATE,
    close DOUBLE,
    news_count INT,
    avg_sentiment DOUBLE,
    weighted_avg_sentiment DOUBLE,
    bullish_ratio DOUBLE,
    bearish_ratio DOUBLE,
    price_change_pct DOUBLE,
    next_day_price_change DOUBLE,
    intraday_volatility DOUBLE,
    volume BIGINT,
    relative_volume DOUBLE,
    filing_count INT,
    event_type STRING,
    sentiment_price_alignment INT
)
PARTITIONED BY (ticker STRING)
STORED AS ORC;

-- Sentiment Summary Statistics by Ticker
CREATE TABLE IF NOT EXISTS sentiment_summary_by_ticker (
    ticker STRING,
    days_with_news INT,
    total_news INT,
    avg_news_per_day DOUBLE,
    avg_sentiment DOUBLE,
    sentiment_volatility DOUBLE,
    avg_bullish_ratio DOUBLE,
    avg_bearish_ratio DOUBLE,
    avg_price_change DOUBLE,
    same_day_correlation DOUBLE,
    generated_at TIMESTAMP
)
STORED AS ORC;

-- =========================================
-- VIEWS FOR COMMON QUERIES
-- =========================================

-- Recent Stock Performance (Last 30 days)
CREATE VIEW IF NOT EXISTS v_recent_stock_performance AS
SELECT 
    ticker,
    date,
    close,
    price_change_pct,
    volume,
    relative_volume,
    sma_20,
    sma_50,
    avg_sentiment,
    news_count
FROM master_dataset
WHERE date >= DATE_SUB(CURRENT_DATE(), 30)
ORDER BY ticker, date DESC;

-- Top Sentiment Movers
CREATE VIEW IF NOT EXISTS v_top_sentiment_movers AS
SELECT 
    ticker,
    date,
    avg_sentiment,
    news_count,
    price_change_pct,
    next_day_price_change
FROM sentiment_driven_events
WHERE abs(avg_sentiment) > 0.5 AND news_count >= 10
ORDER BY abs(avg_sentiment) DESC, news_count DESC
LIMIT 100;

-- Stocks with Strong Bullish Sentiment
CREATE VIEW IF NOT EXISTS v_bullish_sentiment_stocks AS
SELECT 
    ticker,
    date,
    close,
    avg_sentiment,
    bullish_ratio,
    news_count,
    price_change_pct
FROM master_dataset
WHERE avg_sentiment > 0.3 AND news_count >= 5
    AND date >= DATE_SUB(CURRENT_DATE(), 7)
ORDER BY avg_sentiment DESC, news_count DESC;

-- Daily Market Summary
CREATE VIEW IF NOT EXISTS v_daily_market_summary AS
SELECT 
    date,
    COUNT(DISTINCT ticker) as num_stocks,
    AVG(price_change_pct) as avg_price_change,
    AVG(volume) as avg_volume,
    AVG(avg_sentiment) as market_sentiment,
    SUM(news_count) as total_news,
    SUM(filing_count) as total_filings
FROM master_dataset
WHERE date >= DATE_SUB(CURRENT_DATE(), 90)
GROUP BY date
ORDER BY date DESC;

-- =========================================
-- Repair partitions (run after data load)
-- =========================================

-- Run these commands to discover partitions automatically:
-- MSCK REPAIR TABLE raw_stock_prices;
-- MSCK REPAIR TABLE raw_news_sentiment;
-- MSCK REPAIR TABLE raw_sec_filings;
-- MSCK REPAIR TABLE stock_prices_with_indicators;
-- MSCK REPAIR TABLE daily_sentiment_aggregates;
-- MSCK REPAIR TABLE master_dataset;

-- =========================================
-- Optimize tables for better query performance
-- =========================================

-- ANALYZE TABLE master_dataset COMPUTE STATISTICS;
-- ANALYZE TABLE master_dataset COMPUTE STATISTICS FOR COLUMNS;
-- ANALYZE TABLE sentiment_driven_events COMPUTE STATISTICS;
