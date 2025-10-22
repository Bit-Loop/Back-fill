"""
Scraper Main Entry Point
Orchestrates historical data backfill and real-time data ingestion.

Usage:
    python scraper.py --tickers AMD,NVDA --years 2 --flatfiles
    python scraper.py --all --limit 50
"""
import sys
import argparse
import logging
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from common.config.settings import BackfillConfig
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(threadName)-15s] %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('./logs/backfill.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Backfill historical market data')
    parser.add_argument('--tickers', type=str, help='Comma-separated list of tickers')
    parser.add_argument('--all', action='store_true', help='Backfill all tickers')
    parser.add_argument('--limit', type=int, help='Limit number of tickers (with --all)')
    parser.add_argument('--years', type=int, default=5, help='Years of historical data')
    parser.add_argument('--flatfiles', action='store_true', help='Use S3 flat files')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--skip-reference', action='store_true', help='Skip Phase 1')
    parser.add_argument('--skip-corporate', action='store_true', help='Skip Phase 2')
    parser.add_argument('--skip-daily', action='store_true', help='Skip Phase 3')
    parser.add_argument('--skip-minute', action='store_true', help='Skip Phase 4')
    parser.add_argument('--skip-news', action='store_true', help='Skip Phase 5')
    return parser.parse_args()


def main():
    """Main scraper entry point"""
    # Load environment
    load_dotenv()
    
    # Parse arguments
    args = parse_args()
    
    # Create configuration
    config = BackfillConfig.default()
    config.debug = args.debug
    
    # Import after configuration (avoids circular imports)
    from scraper.orchestrator import BackfillOrchestrator
    from common.storage.timescale_writer import TimescaleWriter
    
    # Get ticker list
    if args.all:
        # TODO: Implement get_all_tickers from reference data
        logger.info("Fetching all active tickers...")
        tickers = []  # Placeholder
        if args.limit:
            tickers = tickers[:args.limit]
    else:
        if not args.tickers:
            logger.error("Must specify --tickers or --all")
            sys.exit(1)
        tickers = [t.strip().upper() for t in args.tickers.split(',')]
    
    logger.info(f"Starting backfill for {len(tickers)} tickers: {tickers}")
    
    # Initialize database writer
    db_writer = TimescaleWriter()
    
    # Create orchestrator
    orchestrator = BackfillOrchestrator(
        polygon_api_key=config.polygon.api_key,
        db_writer=db_writer,
        years_back=args.years,
        use_flatfiles=args.flatfiles,
        debug=args.debug,
        skip_reference=args.skip_reference,
        skip_corporate=args.skip_corporate,
        skip_daily=args.skip_daily,
        skip_minute=args.skip_minute,
        skip_news=args.skip_news
    )
    
    # Run backfill
    try:
        results = orchestrator.run_full_backfill(tickers)
        logger.info(f"Backfill complete: {results}")
    except KeyboardInterrupt:
        logger.info("Backfill interrupted by user")
    except Exception as e:
        logger.error(f"Backfill failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        orchestrator.close()
        db_writer.close()


if __name__ == '__main__':
    main()
