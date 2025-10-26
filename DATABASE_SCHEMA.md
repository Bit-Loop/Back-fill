# TimescaleDB Database Schema

## Overview
ChronoX uses TimescaleDB (PostgreSQL extension) for time-series market data storage.

## Table Structure

### 1. OHLCV Data (Hypertables)
**11 tables** for different timeframes: `market_data_{timeframe}`

**Timeframes:**
- `1m`, `5m`, `15m`, `30m` - Minute intervals
- `1h`, `2h`, `4h`, `12h` - Hour intervals
- `1d` - Daily
- `1w` - Weekly
- `1mo` - Monthly

**Schema for each table:**
```sql
CREATE TABLE market_data_{timeframe} (
    time        TIMESTAMPTZ NOT NULL,
    symbol      TEXT NOT NULL,
    open        DOUBLE PRECISION,
    high        DOUBLE PRECISION,
    low         DOUBLE PRECISION,
    close       DOUBLE PRECISION,
    volume      BIGINT,
    vwap        DOUBLE PRECISION,
    transactions INTEGER,
    PRIMARY KEY (time, symbol)
);
```

**Hypertable Configuration:**
- Chunk interval: 1 day
- Partitioned by `time` column for efficient time-series queries

**Indexes:**
- Primary: `(time, symbol)`
- Secondary: `(symbol, time DESC)` for per-symbol lookups

---

### 2. Reference Data
**Table:** `reference_data`

**Schema:**
```sql
CREATE TABLE reference_data (
    symbol              TEXT PRIMARY KEY,
    name                TEXT,
    market              TEXT,
    locale              TEXT,
    primary_exchange    TEXT,
    type                TEXT,
    active              BOOLEAN,
    currency_name       TEXT,
    cik                 TEXT,
    composite_figi      TEXT,
    share_class_figi    TEXT,
    market_cap          BIGINT,
    phone_number        TEXT,
    address             TEXT,
    description         TEXT,
    sic_code            TEXT,
    employees           INTEGER,
    list_date           DATE,
    logo_url            TEXT,
    icon_url            TEXT,
    homepage_url        TEXT,
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);
```

**Purpose:** Store ticker metadata (company info, exchange, asset type, market cap)

---

### 3. Corporate Actions
**Table:** `corporate_actions`

**Schema:**
```sql
CREATE TABLE corporate_actions (
    id                  SERIAL PRIMARY KEY,
    symbol              TEXT NOT NULL,
    action_type         TEXT NOT NULL,          -- 'dividend' or 'split'
    execution_date      DATE NOT NULL,
    ex_dividend_date    DATE,
    record_date         DATE,
    pay_date            DATE,
    cash_amount         DOUBLE PRECISION,       -- For dividends
    split_from          INTEGER,                -- For splits (e.g., 2 in 2:1)
    split_to            INTEGER,                -- For splits (e.g., 1 in 2:1)
    declaration_date    DATE,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);
```

**Indexes:**
- `(symbol, execution_date)` for per-ticker lookups

**Purpose:** Track dividends and stock splits

---

### 4. News Articles
**Table:** `news_articles`

**Schema:**
```sql
CREATE TABLE news_articles (
    id              TEXT PRIMARY KEY,           -- Polygon.io article ID
    title           TEXT,
    author          TEXT,
    published_utc   TIMESTAMPTZ,
    article_url     TEXT,
    tickers         TEXT[],                     -- Array of related tickers
    description     TEXT,
    keywords        TEXT[],                     -- Array of keywords
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
```

**Indexes:**
- `(published_utc DESC)` for chronological queries
- GIN index on `tickers` for array searches

**Purpose:** Store news articles related to tickers

---

### 5. Market Snapshots
**Table:** `market_snapshots`

**Schema:**
```sql
CREATE TABLE market_snapshots (
    symbol                  TEXT NOT NULL,
    updated_at              TIMESTAMPTZ NOT NULL,
    last_trade_price        DOUBLE PRECISION,
    last_trade_size         BIGINT,
    last_trade_exchange     TEXT,
    last_quote_bid          DOUBLE PRECISION,
    last_quote_ask          DOUBLE PRECISION,
    last_quote_bid_size     BIGINT,
    last_quote_ask_size     BIGINT,
    day_open                DOUBLE PRECISION,
    day_high                DOUBLE PRECISION,
    day_low                 DOUBLE PRECISION,
    day_close               DOUBLE PRECISION,
    day_volume              BIGINT,
    prev_close              DOUBLE PRECISION,
    prev_volume             BIGINT,
    todays_change           DOUBLE PRECISION,
    todays_change_percent   DOUBLE PRECISION,
    minute_vwap             DOUBLE PRECISION,
    minute_volume           BIGINT,
    raw_data                JSONB,              -- Full JSON response
    PRIMARY KEY (symbol, updated_at)
);
```

**Indexes:**
- Primary: `(symbol, updated_at)`
- `(updated_at DESC)` for recent snapshot queries

**Purpose:** Store real-time market snapshot data

---

## Data Format Examples

### OHLCV Bar
```python
{
    'time': '2024-01-15 14:30:00+00',  # UTC timestamp
    'symbol': 'AAPL',
    'open': 185.23,
    'high': 186.45,
    'low': 184.98,
    'close': 185.67,
    'volume': 1234567,
    'vwap': 185.42,
    'transactions': 8765
}
```

### Reference Data
```python
{
    'symbol': 'AAPL',
    'name': 'Apple Inc.',
    'market': 'stocks',
    'locale': 'us',
    'primary_exchange': 'XNAS',
    'type': 'CS',  # Common Stock
    'active': True,
    'currency_name': 'usd',
    'market_cap': 3000000000000,  # $3T
    'employees': 161000,
    'list_date': '1980-12-12',
    'homepage_url': 'https://www.apple.com'
}
```

### Dividend (Corporate Action)
```python
{
    'symbol': 'AAPL',
    'action_type': 'dividend',
    'execution_date': '2024-02-15',
    'ex_dividend_date': '2024-02-10',
    'record_date': '2024-02-12',
    'pay_date': '2024-02-15',
    'cash_amount': 0.24,  # $0.24 per share
    'declaration_date': '2024-02-01'
}
```

### Stock Split (Corporate Action)
```python
{
    'symbol': 'TSLA',
    'action_type': 'split',
    'execution_date': '2022-08-25',
    'split_from': 3,  # 3:1 split
    'split_to': 1,
    'declaration_date': '2022-08-05'
}
```

---

## Migration Notes

The schema includes automatic migrations for:
- **Legacy `ticker` → `symbol` rename** (all tables)
- **Missing columns** (transactions, date fields, numeric fields)
- **Backward compatibility** maintained

---

## Query Examples

### Get daily bars for ticker
```sql
SELECT time, open, high, low, close, volume
FROM market_data_1d
WHERE symbol = 'AAPL'
  AND time >= '2024-01-01'
  AND time < '2025-01-01'
ORDER BY time;
```

### Get corporate actions for ticker
```sql
SELECT action_type, execution_date, cash_amount, split_from, split_to
FROM corporate_actions
WHERE symbol = 'AAPL'
ORDER BY execution_date DESC;
```

### Get news for ticker
```sql
SELECT title, published_utc, article_url
FROM news_articles
WHERE 'AAPL' = ANY(tickers)
ORDER BY published_utc DESC
LIMIT 10;
```

### Get latest snapshot
```sql
SELECT *
FROM market_snapshots
WHERE symbol = 'AAPL'
ORDER BY updated_at DESC
LIMIT 1;
```

---

## Access Methods

### Via Storage Layer (Recommended)
```python
from storage.timescale import OHLCVRepository, ReferenceDataRepository

# OHLCV data
ohlcv_repo = OHLCVRepository(pool)
bars = ohlcv_repo.get_bars('AAPL', '1d', start_date, end_date)

# Reference data
ref_repo = ReferenceDataRepository(pool)
ticker_info = ref_repo.get_ticker_details('AAPL')
```

### Via Service Layer (Best Practice)
```python
from api.services import RegistryService

# Through REST API or direct service
service = RegistryService(repository)
tickers = service.search_tickers(query='Apple')
```

---

## Performance Characteristics

- **Hypertables:** Automatic partitioning by time (1-day chunks)
- **Compression:** TimescaleDB can compress older chunks
- **Indexes:** Optimized for time-range and symbol queries
- **JSONB:** Flexible storage for raw API responses (snapshots)

## Connection Info

Default connection (from environment variables):
```
Host: localhost
Port: 5432
Database: market_data
User: postgres
```

Set via `.env`:
```bash
TIMESCALE_HOST=localhost
TIMESCALE_PORT=5432
TIMESCALE_DB=market_data
TIMESCALE_USER=postgres
TIMESCALE_PASSWORD=your_password
```
