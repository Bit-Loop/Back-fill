"""
Backfill Orchestrator
Coordinates the complete historical data backfill process across 5 phases.

This module is extracted from the original monolithic backfill_historical_data.py.
For the complete flat file implementation with producer/consumer pipeline, adaptive
CPU scaling, and S3 integration, refer to the original file.
"""
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from tqdm import tqdm

from scraper.clients import (
    PolygonClient,
    AggregatesClient,
    CorporateActionsClient,
    ReferenceClient,
    NewsClient
)
from common.storage.timescale_writer import TimescaleWriter

logger = logging.getLogger(__name__)


class BackfillOrchestrator:
    """
    Orchestrates the complete historical data backfill process.
    
    Phases:
    1. Reference Data: Ticker metadata, exchanges, market information
    2. Corporate Actions: Dividends and splits
    3. Daily Bars: Daily OHLCV data
    4. Minute Bars: Intraday OHLCV data (REST API or S3 flat files)
    5. News: Recent news articles
    
    Uses TimescaleDB for storage with ACID transactions, SQL joins,
    continuous aggregates, and 10-20x compression.
    """
    
    def __init__(self, polygon_api_key: str, db_writer: TimescaleWriter,
                 years_back: int = 5, max_workers: int = 4, use_flatfiles: bool = False,
                 debug: bool = False, skip_reference: bool = False, skip_corporate: bool = False,
                 skip_daily: bool = False, skip_minute: bool = False, skip_news: bool = False,
                 enable_hybrid_backfill: bool = False):
        """
        Initialize backfill orchestrator.
        
        Args:
            polygon_api_key: Polygon.io API key
            db_writer: TimescaleDB writer instance
            years_back: How many years of historical data to fetch
            max_workers: Number of parallel workers for ticker processing
            use_flatfiles: Whether to use S3 flat files for Phase 4
            debug: Enable debug logging for queue states and stack transitions
            skip_reference: Skip Phase 1 (reference data)
            skip_corporate: Skip Phase 2 (corporate actions)
            skip_daily: Skip Phase 3 (daily bars)
            skip_minute: Skip Phase 4 (minute bars / flat files)
            skip_news: Skip Phase 5 (news)
            enable_hybrid_backfill: Enable hybrid mode (flat files + REST API gap filling)
        """
        # Initialize Polygon clients
        self.polygon_client = PolygonClient(polygon_api_key)
        self.agg_client = AggregatesClient(self.polygon_client)
        self.corp_client = CorporateActionsClient(self.polygon_client)
        self.ref_client = ReferenceClient(self.polygon_client)
        self.news_client = NewsClient(self.polygon_client)
        self.db_writer = db_writer
        
        # Configuration
        self.years_back = years_back
        self.max_workers = max_workers
        self.use_flatfiles = use_flatfiles
        self.debug = debug
        self.enable_hybrid_backfill = enable_hybrid_backfill
        
        # Skip flags for selective phase execution
        self.skip_reference = skip_reference
        self.skip_corporate = skip_corporate
        self.skip_daily = skip_daily
        self.skip_minute = skip_minute
        self.skip_news = skip_news
        
        # Calculate date range
        self.end_date = datetime.now()
        self.start_date = self.end_date - timedelta(days=365 * years_back)
        
        # Queue metrics interface (shared with GUI for real-time monitoring)
        self.queue_metrics = {
            'download_qsize': lambda: 0,
            'process_qsize': lambda: 0,
            'download_queue': None,
            'process_queue': None
        }
        
        logger.info(f"BackfillOrchestrator initialized: {self.start_date.date()} to {self.end_date.date()}")
        logger.info(f"Database: TimescaleDB (PostgreSQL + TimescaleDB extension)")
        if debug:
            logger.info(f"Debug mode: ENABLED")
    
    def phase1_reference_data(self, tickers: List[str]) -> Dict[str, bool]:
        """
        Phase 1: Fetch and store reference data for all tickers.
        
        Args:
            tickers: List of ticker symbols
        
        Returns:
            Dict mapping ticker to success status
        """
        logger.info(f"=== PHASE 1: Reference Data ({len(tickers)} tickers) ===")
        results = {}
        
        for ticker in tqdm(tickers, desc="Reference Data"):
            try:
                details = self.ref_client.get_ticker_details(ticker)
                
                if details:
                    name = details.get('name', 'Unknown')
                    exchange = details.get('primary_exchange', details.get('exchange', 'Unknown'))
                    ticker_type = details.get('type', 'Unknown')
                    market_cap = details.get('market_cap', 0)
                    
                    logger.info(f"✓ {ticker}: {name} | {exchange} | Type: {ticker_type} | Market Cap: ${market_cap:,}")
                    
                    success = self.db_writer.write_reference_data(ticker, details)
                    results[ticker] = success
                    
                    if success:
                        logger.debug(f"  └─ Wrote reference data to database")
                    else:
                        logger.warning(f"  └─ Failed to write reference data to database")
                else:
                    logger.warning(f"✗ No reference data for {ticker}")
                    results[ticker] = False
                
                time.sleep(0.1)  # Rate limit
            
            except Exception as e:
                logger.error(f"✗ Error fetching reference data for {ticker}: {str(e)}")
                results[ticker] = False
        
        success_count = sum(1 for v in results.values() if v)
        logger.info(f"Phase 1 complete: {success_count}/{len(tickers)} successful")
        return results
    
    def phase2_corporate_actions(self, tickers: List[str]) -> Dict[str, Dict]:
        """
        Phase 2: Fetch and store corporate actions (dividends and splits).
        
        Args:
            tickers: List of ticker symbols
        
        Returns:
            Dict with results per ticker
        """
        logger.info(f"=== PHASE 2: Corporate Actions ({len(tickers)} tickers) ===")
        results = {}
        since_date = self.start_date.strftime('%Y-%m-%d')
        
        for ticker in tqdm(tickers, desc="Corporate Actions"):
            try:
                ticker_results = {'dividends': 0, 'splits': 0}
                
                # Fetch dividends
                dividends = self.corp_client.get_dividends_for_ticker(ticker, since=since_date)
                if dividends:
                    count = self.db_writer.write_corporate_actions(ticker, dividends, 'dividend')
                    ticker_results['dividends'] = count
                    
                    logger.info(f"✓ {ticker}: Found {count} dividends since {since_date}")
                    for div in dividends[:3]:  # Show first 3
                        ex_date = div.get('ex_dividend_date', 'Unknown')
                        amount = div.get('cash_amount', 0)
                        logger.debug(f"  └─ Dividend: ${amount:.4f} on {ex_date}")
                    if len(dividends) > 3:
                        logger.debug(f"  └─ ... and {len(dividends)-3} more")
                else:
                    logger.debug(f"  {ticker}: No dividends since {since_date}")
                
                # Fetch splits
                splits = self.corp_client.get_splits_for_ticker(ticker, since=since_date)
                if splits:
                    count = self.db_writer.write_corporate_actions(ticker, splits, 'split')
                    ticker_results['splits'] = count
                    
                    logger.info(f"✓ {ticker}: Found {count} splits since {since_date}")
                    for split in splits:
                        ex_date = split.get('execution_date', 'Unknown')
                        ratio = split.get('split_to', 1) / split.get('split_from', 1)
                        logger.debug(f"  └─ Split: {ratio:.2f}:1 on {ex_date}")
                else:
                    logger.debug(f"  {ticker}: No splits since {since_date}")
                
                results[ticker] = ticker_results
                time.sleep(0.1)
            
            except Exception as e:
                logger.error(f"Error fetching corporate actions for {ticker}: {str(e)}")
                results[ticker] = {'dividends': 0, 'splits': 0, 'error': str(e)}
        
        logger.info(f"Phase 2 complete")
        return results
    
    def phase3_daily_bars(self, tickers: List[str]) -> Dict[str, int]:
        """
        Phase 3: Fetch and store daily OHLCV bars.
        
        Args:
            tickers: List of ticker symbols
        
        Returns:
            Dict mapping ticker to bar count
        """
        logger.info(f"=== PHASE 3: Daily Bars ({len(tickers)} tickers) ===")
        results = {}
        
        start_str = self.start_date.strftime('%Y-%m-%d')
        end_str = self.end_date.strftime('%Y-%m-%d')
        
        for ticker in tqdm(tickers, desc="Daily Bars"):
            try:
                bars = self.agg_client.get_daily_bars(ticker, start_str, end_str)
                
                if bars:
                    count = self.db_writer.write_ohlcv_bars(ticker, bars, timeframe="1d")
                    results[ticker] = count
                    
                    first_bar = bars[0]
                    last_bar = bars[-1]
                    
                    first_date = datetime.fromtimestamp(first_bar['t'] / 1000).strftime('%Y-%m-%d')
                    last_date = datetime.fromtimestamp(last_bar['t'] / 1000).strftime('%Y-%m-%d')
                    
                    logger.info(f"✓ {ticker}: {count} daily bars | {first_date} to {last_date}")
                    logger.debug(f"  └─ First: O=${first_bar['o']:.2f} H=${first_bar['h']:.2f} L=${first_bar['l']:.2f} C=${first_bar['c']:.2f} V={first_bar['v']:,}")
                    logger.debug(f"  └─ Last:  O=${last_bar['o']:.2f} H=${last_bar['h']:.2f} L=${last_bar['l']:.2f} C=${last_bar['c']:.2f} V={last_bar['v']:,}")
                else:
                    logger.warning(f"✗ {ticker}: No daily bars found for {start_str} to {end_str}")
                    results[ticker] = 0
                
                time.sleep(0.1)
            
            except Exception as e:
                logger.error(f"Error fetching daily bars for {ticker}: {str(e)}")
                results[ticker] = 0
        
        total_bars = sum(results.values())
        logger.info(f"Phase 3 complete: {total_bars:,} daily bars written")
        return results
    
    def phase4_minute_bars(self, tickers: List[str], chunk_days: int = 7) -> Dict[str, int]:
        """
        Phase 4: Fetch and store minute OHLCV bars (chunked for large date ranges).
        
        Args:
            tickers: List of ticker symbols
            chunk_days: Days per chunk (7 recommended to stay under 50k limit)
        
        Returns:
            Dict mapping ticker to bar count
        """
        logger.info(f"=== PHASE 4: Minute Bars ({len(tickers)} tickers) ===")
        logger.info(f"Date range: {self.start_date.date()} to {self.end_date.date()}")
        logger.info(f"Chunk size: {chunk_days} days")
        
        results = {}
        start_str = self.start_date.strftime('%Y-%m-%d')
        end_str = self.end_date.strftime('%Y-%m-%d')
        
        for ticker in tqdm(tickers, desc="Minute Bars"):
            try:
                # Use chunked fetching to handle multi-year data
                bars = self.agg_client.get_minute_bars(ticker, start_str, end_str)
                
                if bars:
                    count = self.db_writer.write_ohlcv_bars(ticker, bars, timeframe="1m")
                    results[ticker] = count
                else:
                    results[ticker] = 0
                
                time.sleep(0.2)  # Longer delay for large requests
            
            except Exception as e:
                logger.error(f"Error fetching minute bars for {ticker}: {str(e)}")
                results[ticker] = 0
        
        total_bars = sum(results.values())
        logger.info(f"Phase 4 complete: {total_bars:,} minute bars written")
        return results
    
    def phase4_flatfiles(self, tickers: List[str]) -> Dict[str, int]:
        """
        Phase 4 alternative: Use Polygon flat files (S3) with producer/consumer pipeline.
        
        NOTE: This is a stub implementation. The complete flat file implementation
        with producer/consumer pipeline, adaptive CPU scaling, exponential backoff,
        state machines, and S3 integration (~1500 lines) is available in the original
        backfill_historical_data.py file.
        
        For production use, refer to the original implementation which includes:
        - Producer threads (download from S3, I/O-bound, 24 workers)
        - Consumer threads (parse and insert, CPU-bound, 32 workers)
        - Bounded queues (backpressure, prevent memory exhaustion)
        - Exponential backoff (retry failed downloads with jitter)
        - State tracking (FileState FSM with logging)
        - Fault tolerance (per-file retry, graceful degradation to REST API)
        - Proper shutdown (poison pills + queue draining to avoid deadlock)
        - Adaptive CPU scaling (dynamic worker adjustment based on load)
        - Metrics logging (throughput, queue depth, latency tracking)
        
        Args:
            tickers: List of ticker symbols
        
        Returns:
            Dict mapping ticker to bar count
        """
        logger.warning("=== PHASE 4 (FlatFiles): Stub implementation ===")
        logger.warning("For full flat file support with S3 integration, use the original backfill_historical_data.py")
        logger.warning("Falling back to REST API...")
        
        return self.phase4_minute_bars(tickers)
    
    def phase5_news(self, tickers: List[str], limit: int = 100) -> Dict[str, int]:
        """
        Phase 5: Fetch and store news articles.
        
        Args:
            tickers: List of ticker symbols
            limit: Maximum articles per ticker
        
        Returns:
            Dict mapping ticker to article count
        """
        logger.info(f"=== PHASE 5: News Articles ({len(tickers)} tickers) ===")
        results = {}
        
        for ticker in tqdm(tickers, desc="News"):
            try:
                articles = self.news_client.get_news_for_ticker(ticker, limit=limit)
                
                if articles:
                    count = self.db_writer.write_news(articles)
                    results[ticker] = count
                else:
                    results[ticker] = 0
                
                time.sleep(0.1)
            
            except Exception as e:
                logger.error(f"Error fetching news for {ticker}: {str(e)}")
                results[ticker] = 0
        
        total_articles = sum(results.values())
        logger.info(f"Phase 5 complete: {total_articles:,} articles written")
        return results
    
    def run_full_backfill(self, tickers: List[str]) -> Dict:
        """
        Run complete backfill process for specified tickers.
        
        Args:
            tickers: List of ticker symbols
        
        Returns:
            Summary dict with results from all phases
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"STARTING FULL BACKFILL FOR {len(tickers)} TICKERS")
        logger.info(f"Date Range: {self.start_date.date()} to {self.end_date.date()}")
        
        # Log skip flags
        skip_list = []
        if self.skip_reference:
            skip_list.append("Phase 1 (Reference)")
        if self.skip_corporate:
            skip_list.append("Phase 2 (Corporate Actions)")
        if self.skip_daily:
            skip_list.append("Phase 3 (Daily Bars)")
        if self.skip_minute:
            skip_list.append("Phase 4 (Minute Bars)")
        if self.skip_news:
            skip_list.append("Phase 5 (News)")
        
        if skip_list:
            logger.info(f"Skipping: {', '.join(skip_list)}")
        
        logger.info(f"{'='*60}\n")
        
        start_time = time.time()
        summary = {}
        
        try:
            # Phase 1: Reference Data
            if not self.skip_reference:
                summary['reference'] = self.phase1_reference_data(tickers)
            else:
                logger.info("=== PHASE 1: SKIPPED (Reference Data) ===")
                summary['reference'] = {}
            
            # Phase 2: Corporate Actions
            if not self.skip_corporate:
                summary['corporate_actions'] = self.phase2_corporate_actions(tickers)
            else:
                logger.info("=== PHASE 2: SKIPPED (Corporate Actions) ===")
                summary['corporate_actions'] = {}
            
            # Phase 3: Daily Bars
            if not self.skip_daily:
                summary['daily_bars'] = self.phase3_daily_bars(tickers)
            else:
                logger.info("=== PHASE 3: SKIPPED (Daily Bars) ===")
                summary['daily_bars'] = {}
            
            # Phase 4: Minute Bars (most time-consuming)
            if not self.skip_minute:
                if self.use_flatfiles:
                    logger.info("Using flat file mode (stub - will fall back to REST API)")
                    summary['minute_bars'] = self.phase4_flatfiles(tickers)
                else:
                    # Pure REST API mode
                    summary['minute_bars'] = self.phase4_minute_bars(tickers)
            else:
                logger.info("=== PHASE 4: SKIPPED (Minute Bars / Flat Files) ===")
                summary['minute_bars'] = {}
            
            # Phase 5: News
            if not self.skip_news:
                summary['news'] = self.phase5_news(tickers)
            else:
                logger.info("=== PHASE 5: SKIPPED (News) ===")
                summary['news'] = {}
        
        except KeyboardInterrupt:
            logger.warning("\n\nBackfill interrupted by user!")
            raise
        
        except Exception as e:
            logger.error(f"Fatal error during backfill: {str(e)}")
            raise
        
        finally:
            elapsed = time.time() - start_time
            logger.info(f"\n{'='*60}")
            logger.info(f"BACKFILL COMPLETE")
            logger.info(f"Total time: {elapsed/60:.2f} minutes")
            logger.info(f"{'='*60}\n")
        
        return summary
    
    def close(self):
        """Clean up resources"""
        self.polygon_client.close()
        self.db_writer.close()
