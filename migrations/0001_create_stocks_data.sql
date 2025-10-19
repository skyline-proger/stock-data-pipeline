CREATE TABLE IF NOT EXISTS stocks_data (
    date DATE NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    ticker TEXT NOT NULL,
    return_pct DOUBLE PRECISION,
    ma7 DOUBLE PRECISION,
    volatility DOUBLE PRECISION,
    PRIMARY KEY (ticker, date)
);

CREATE INDEX IF NOT EXISTS idx_stocks_data_ticker ON stocks_data (ticker);
