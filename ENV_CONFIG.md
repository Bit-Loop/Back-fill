# Environment Configuration

The scraper loads all API keys and configuration from the `.env` file.

## Setup

1. Copy `.env.example` to `.env`:
   ```bash
   cp .env.example .env
   ```

2. Edit `.env` and add your API keys:
   ```bash
   nano .env  # or use your preferred editor
   ```

## Required Configuration

### Polygon.io API Key (Required)
```env
POLYGON_API_KEY=your_polygon_api_key_here
```
Get your API key from: https://polygon.io/dashboard/api-keys

### Database Configuration (Required)
```env
TIMESCALE_HOST=localhost
TIMESCALE_PORT=5433
TIMESCALE_DB=chronox
TIMESCALE_USER=postgres
TIMESCALE_PASSWORD=your_db_password_here
```

### Redis Configuration (Required for streaming)
```env
REDIS_HOST=localhost
REDIS_PORT=6380
REDIS_DB=0
REDIS_PASSWORD=
```

## Optional Configuration

### Polygon S3 Flat Files (for bulk historical downloads)
```env
POLYGON_S3_ACCESS_KEY=your_s3_access_key_here
POLYGON_S3_SECRET_KEY=your_s3_secret_key_here
```

### Logging
```env
LOG_DIR=./logs
LOG_LEVEL=INFO  # Options: DEBUG, INFO, WARNING, ERROR
```

### Scraper Daemon
```env
SCRAPER_HOST=127.0.0.1
SCRAPER_PORT=9000
```

## Usage

Once configured, the scraper automatically loads all settings from `.env`:

```bash
# Backfill historical data
python scraper.py --tickers AAPL,MSFT --years 5

# Stream live data
python scraper.py --tickers AAPL --stream --stream-timeframe 1m

# Backfill + Stream simultaneously
python scraper.py --tickers AAPL,MSFT --years 5 --stream

# Run as daemon API
python scraper.py --daemon --host 0.0.0.0 --port 9000
```

## Verification

To verify your configuration is loaded correctly:

```bash
python -c "from dotenv import load_dotenv; import os; load_dotenv(); print('API Key:', 'SET' if os.getenv('POLYGON_API_KEY') else 'NOT SET')"
```

## Security Notes

- **Never commit `.env` to version control** - it contains sensitive credentials
- The `.env` file is already in `.gitignore`
- Use `.env.example` as a template without real credentials
- Rotate API keys periodically for security
