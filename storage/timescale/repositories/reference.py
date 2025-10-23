"""
Reference data repository for TimescaleDB.

Handles reading and writing ticker reference/metadata.
"""
import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


class ReferenceDataRepository:
    """
    Repository for ticker reference data operations.
    
    Responsibilities:
    - Write ticker metadata (name, exchange, market cap, etc.)
    - Upsert logic for updating existing records
    """
    
    def __init__(self, pool):
        """
        Initialize reference data repository.
        
        Args:
            pool: PostgresConnectionPool instance
        """
        self.pool = pool
    
    def write_reference(self, symbol: str, details: Dict[str, Any]) -> bool:
        """
        Write ticker reference data to database.
        
        Args:
            symbol: Ticker symbol
            details: Dictionary with ticker details
        
        Returns:
            True if successful
        """
        with self.pool.get_connection() as conn:
            cur = conn.cursor()
            
            cur.execute("""
                INSERT INTO reference_data 
                (symbol, name, market, locale, primary_exchange, type, active,
                 currency_name, cik, composite_figi, share_class_figi, market_cap,
                 phone_number, address, description, sic_code, employees, list_date,
                 logo_url, icon_url, homepage_url, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (symbol) DO UPDATE SET
                    name = EXCLUDED.name,
                    market = EXCLUDED.market,
                    locale = EXCLUDED.locale,
                    primary_exchange = EXCLUDED.primary_exchange,
                    type = EXCLUDED.type,
                    active = EXCLUDED.active,
                    currency_name = EXCLUDED.currency_name,
                    cik = EXCLUDED.cik,
                    composite_figi = EXCLUDED.composite_figi,
                    share_class_figi = EXCLUDED.share_class_figi,
                    market_cap = EXCLUDED.market_cap,
                    phone_number = EXCLUDED.phone_number,
                    address = EXCLUDED.address,
                    description = EXCLUDED.description,
                    sic_code = EXCLUDED.sic_code,
                    employees = EXCLUDED.employees,
                    list_date = EXCLUDED.list_date,
                    logo_url = EXCLUDED.logo_url,
                    icon_url = EXCLUDED.icon_url,
                    homepage_url = EXCLUDED.homepage_url,
                    updated_at = NOW()
            """, (
                symbol,
                details.get('name'),
                details.get('market'),
                details.get('locale'),
                details.get('primary_exchange'),
                details.get('type'),
                details.get('active'),
                details.get('currency_name'),
                details.get('cik'),
                details.get('composite_figi'),
                details.get('share_class_figi'),
                details.get('market_cap'),
                details.get('phone_number'),
                details.get('address', {}).get('address1') if isinstance(details.get('address'), dict) else None,
                details.get('description'),
                details.get('sic_code'),
                details.get('total_employees'),
                details.get('list_date'),
                details.get('branding', {}).get('logo_url') if isinstance(details.get('branding'), dict) else None,
                details.get('branding', {}).get('icon_url') if isinstance(details.get('branding'), dict) else None,
                details.get('homepage_url')
            ))
            
            conn.commit()
            logger.debug(f"Wrote reference data for {symbol}")
            return True
