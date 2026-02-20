-- =========================================
-- PostgreSQL Schemas for Financial Data Analytics
-- =========================================

-- Create schema for financial analytics
CREATE SCHEMA IF NOT EXISTS financial_analytics;

-- =========================================
-- DIMENSION TABLES
-- =========================================

-- Stock Ticker Dimension
CREATE TABLE IF NOT EXISTS financial_analytics.dim_stock (
    stock_id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) UNIQUE NOT NULL,
    company_name VARCHAR(255),
    sector VARCHAR(100),
    industry VARCHAR(100),
    market_cap NUMERIC,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Date Dimension
CREATE TABLE IF NOT EXISTS financial_analytics.dim_date (
    date_id SERIAL PRIMARY KEY,
    date DATE UNIQUE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    week INT NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =========================================
-- FACT TABLES
-- =========================================

-- Daily Stock Facts
CREATE TABLE IF NOT EXISTS financial_analytics.fact_daily_stock (
    fact_id BIGSERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open NUMERIC(12,4),
    high NUMERIC(12,4),
    low NUMERIC(12,4),
    close NUMERIC(12,4),
    volume BIGINT,
    vwap NUMERIC(12,4),
    price_change_pct NUMERIC(8,4),
    sma_20 NUMERIC(12,4),
    sma_50 NUMERIC(12,4),
    momentum_5d NUMERIC(8,4),
    volatility_20d NUMERIC(8,4),
    relative_volume NUMERIC(8,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_stock_date UNIQUE (ticker, date)
);

CREATE INDEX idx_fact_daily_stock_ticker ON financial_analytics.fact_daily_stock(ticker);
CREATE INDEX idx_fact_daily_stock_date ON financial_analytics.fact_daily_stock(date);

-- Daily Sentiment Facts
CREATE TABLE IF NOT EXISTS financial_analytics.fact_daily_sentiment (
    fact_id BIGSERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    news_count INT NOT NULL DEFAULT 0,
    avg_sentiment NUMERIC(6,4),
    weighted_avg_sentiment NUMERIC(6,4),
    bullish_ratio NUMERIC(6,4),
    bearish_ratio NUMERIC(6,4),
    bullish_count INT,
    bearish_count INT,
    neutral_count INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_sentiment_stock_date UNIQUE (ticker, date)
);

CREATE INDEX idx_fact_daily_sentiment_ticker ON financial_analytics.fact_daily_sentiment(ticker);
CREATE INDEX idx_fact_daily_sentiment_date ON financial_analytics.fact_daily_sentiment(date);

-- =========================================
-- AGGREGATE TABLES
-- =========================================

-- Sentiment-Price Correlation Summary
CREATE TABLE IF NOT EXISTS financial_analytics.agg_sentiment_correlation (
    agg_id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    analysis_period VARCHAR(20) NOT NULL, -- 'monthly', 'quarterly', 'yearly'
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    sample_size INT NOT NULL,
    sentiment_correlation NUMERIC(6,4),
    weighted_sentiment_correlation NUMERIC(6,4),
    bullish_ratio_correlation NUMERIC(6,4),
    avg_sentiment NUMERIC(6,4),
    avg_price_change NUMERIC(8,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_correlation_period UNIQUE (ticker, analysis_period, period_start)
);

CREATE INDEX idx_agg_correlation_ticker ON financial_analytics.agg_sentiment_correlation(ticker);
CREATE INDEX idx_agg_correlation_period ON financial_analytics.agg_sentiment_correlation(period_start, period_end);

-- Stock Performance Summary
CREATE TABLE IF NOT EXISTS financial_analytics.agg_stock_performance (
    agg_id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    period_type VARCHAR(20) NOT NULL, -- 'daily', 'weekly', 'monthly', 'quarterly'
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    open_price NUMERIC(12,4),
    close_price NUMERIC(12,4),
    high_price NUMERIC(12,4),
    low_price NUMERIC(12,4),
    avg_price NUMERIC(12,4),
    total_volume BIGINT,
    avg_volume BIGINT,
    price_change_pct NUMERIC(8,4),
    volatility NUMERIC(8,4),
    trading_days INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_perf_period UNIQUE (ticker, period_type, period_start)
);

CREATE INDEX idx_agg_performance_ticker ON financial_analytics.agg_stock_performance(ticker);
CREATE INDEX idx_agg_performance_period ON financial_analytics.agg_stock_performance(period_type, period_start);

-- =========================================
-- EVENT TABLES
-- =========================================

-- Sentiment-Driven Events
CREATE TABLE IF NOT EXISTS financial_analytics.event_sentiment_driven (
    event_id BIGSERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    event_date DATE NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    news_count INT NOT NULL,
    avg_sentiment NUMERIC(6,4) NOT NULL,
    price_at_event NUMERIC(12,4),
    price_change_same_day NUMERIC(8,4),
    price_change_next_day NUMERIC(8,4),
    price_change_5d NUMERIC(8,4),
    intraday_volatility NUMERIC(8,4),
    volume BIGINT,
    relative_volume NUMERIC(8,4),
    sentiment_price_alignment INT,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_event_sentiment_ticker ON financial_analytics.event_sentiment_driven(ticker);
CREATE INDEX idx_event_sentiment_date ON financial_analytics.event_sentiment_driven(event_date);
CREATE INDEX idx_event_sentiment_type ON financial_analytics.event_sentiment_driven(event_type);

-- SEC Filing Events
CREATE TABLE IF NOT EXISTS financial_analytics.event_sec_filing (
    event_id BIGSERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    filing_date DATE NOT NULL,
    form_type VARCHAR(20) NOT NULL,
    price_at_filing NUMERIC(12,4),
    price_change_1d NUMERIC(8,4),
    price_change_5d NUMERIC(8,4),
    avg_sentiment_on_date NUMERIC(6,4),
    news_count_on_date INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_event_filing_ticker ON financial_analytics.event_sec_filing(ticker);
CREATE INDEX idx_event_filing_date ON financial_analytics.event_sec_filing(filing_date);
CREATE INDEX idx_event_filing_form ON financial_analytics.event_sec_filing(form_type);

-- =========================================
-- ANALYSIS RESULTS TABLES
-- =========================================

-- Overall Market Sentiment
CREATE TABLE IF NOT EXISTS financial_analytics.analysis_market_sentiment (
    analysis_id SERIAL PRIMARY KEY,
    analysis_date DATE NOT NULL UNIQUE,
    num_stocks INT NOT NULL,
    avg_sentiment NUMERIC(6,4),
    bullish_stocks_count INT,
    bearish_stocks_count INT,
    neutral_stocks_count INT,
    avg_price_change NUMERIC(8,4),
    total_news_count INT,
    market_volatility NUMERIC(8,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_analysis_market_date ON financial_analytics.analysis_market_sentiment(analysis_date);

-- Pipeline Execution Log
CREATE TABLE IF NOT EXISTS financial_analytics.pipeline_execution_log (
    log_id BIGSERIAL PRIMARY KEY,
    execution_date TIMESTAMP NOT NULL,
    pipeline_name VARCHAR(100) NOT NULL,
    task_name VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL, -- 'success', 'failed', 'running'
    records_processed INT,
    execution_time_seconds INT,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_pipeline_log_date ON financial_analytics.pipeline_execution_log(execution_date);
CREATE INDEX idx_pipeline_log_status ON financial_analytics.pipeline_execution_log(status);

-- =========================================
-- VIEWS FOR REPORTING
-- =========================================

-- Recent Stock Performance with Sentiment
CREATE OR REPLACE VIEW financial_analytics.v_recent_performance AS
SELECT 
    s.ticker,
    s.date,
    s.close,
    s.price_change_pct,
    s.volume,
    s.sma_20,
    s.sma_50,
    se.avg_sentiment,
    se.news_count,
    se.bullish_ratio
FROM financial_analytics.fact_daily_stock s
LEFT JOIN financial_analytics.fact_daily_sentiment se 
    ON s.ticker = se.ticker AND s.date = se.date
WHERE s.date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY s.ticker, s.date DESC;

-- Top Sentiment Movers
CREATE OR REPLACE VIEW financial_analytics.v_top_sentiment_movers AS
SELECT 
    ticker,
    event_date,
    event_type,
    news_count,
    avg_sentiment,
    price_change_same_day,
    price_change_next_day,
    price_change_5d
FROM financial_analytics.event_sentiment_driven
WHERE abs(avg_sentiment) > 0.5 
ORDER BY abs(avg_sentiment) DESC, news_count DESC
LIMIT 50;

-- Strong Correlation Stocks
CREATE OR REPLACE VIEW financial_analytics.v_strong_correlation_stocks AS
SELECT 
    ticker,
    sentiment_correlation,
    sample_size,
    avg_sentiment,
    avg_price_change
FROM financial_analytics.agg_sentiment_correlation
WHERE analysis_period = 'monthly'
    AND abs(sentiment_correlation) > 0.5
    AND sample_size >= 20
ORDER BY abs(sentiment_correlation) DESC;

-- =========================================
-- FUNCTIONS
-- =========================================

-- Function to calculate date dimension
CREATE OR REPLACE FUNCTION financial_analytics.populate_date_dimension(
    start_date DATE,
    end_date DATE
) RETURNS VOID AS $$
DECLARE
    current_date DATE := start_date;
BEGIN
    WHILE current_date <= end_date LOOP
        INSERT INTO financial_analytics.dim_date (
            date, year, quarter, month, week, day, day_of_week, day_name, is_weekend
        )
        VALUES (
            current_date,
            EXTRACT(YEAR FROM current_date),
            EXTRACT(QUARTER FROM current_date),
            EXTRACT(MONTH FROM current_date),
            EXTRACT(WEEK FROM current_date),
            EXTRACT(DAY FROM current_date),
            EXTRACT(DOW FROM current_date),
            TO_CHAR(current_date, 'Day'),
            EXTRACT(DOW FROM current_date) IN (0, 6)
        )
        ON CONFLICT (date) DO NOTHING;
        
        current_date := current_date + INTERVAL '1 day';
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Populate date dimension for 5 years
SELECT financial_analytics.populate_date_dimension(
    CURRENT_DATE - INTERVAL '2 years',
    CURRENT_DATE + INTERVAL '3 years'
);

-- =========================================
-- GRANTS (adjust as needed)
-- =========================================

-- Grant permissions to airflow user
GRANT USAGE ON SCHEMA financial_analytics TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA financial_analytics TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA financial_analytics TO airflow;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA financial_analytics TO airflow;
