"""
Corporate actions repository for TimescaleDB.

Handles reading and writing corporate actions (dividends, splits).
"""
import logging
from typing import Dict, List

from psycopg2 import extras

logger = logging.getLogger(__name__)


class CorporateActionsRepository:
    """
    Repository for corporate actions data operations.
    
    Responsibilities:
    - Write dividend records
    - Write split records
    - Batch insert optimization
    """
    
    def __init__(self, pool):
        """
        Initialize corporate actions repository.
        
        Args:
            pool: PostgresConnectionPool instance
        """
        self.pool = pool
    
    def write_actions(self, symbol: str, actions: List[Dict], action_type: str) -> int:
        """
        Write corporate actions to database.
        
        Args:
            symbol: Ticker symbol
            actions: List of action dictionaries
            action_type: 'dividend' or 'split'
        
        Returns:
            Number of actions written
        """
        if not actions:
            return 0
        
        with self.pool.get_connection() as conn:
            cur = conn.cursor()
            
            data = []
            for action in actions:
                if action_type == 'dividend':
                    data.append((
                        symbol,
                        'dividend',
                        action.get('ex_dividend_date'),
                        action.get('ex_dividend_date'),
                        action.get('record_date'),
                        action.get('pay_date'),
                        action.get('cash_amount'),
                        None,
                        None,
                        action.get('declaration_date')
                    ))
                else:  # split
                    data.append((
                        symbol,
                        'split',
                        action.get('execution_date'),
                        action.get('execution_date'),
                        None,
                        None,
                        None,
                        action.get('split_from'),
                        action.get('split_to'),
                        None
                    ))
            
            extras.execute_batch(cur, """
                INSERT INTO corporate_actions 
                (symbol, action_type, execution_date, ex_dividend_date, record_date,
                 pay_date, cash_amount, split_from, split_to, declaration_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, data, page_size=100)
            
            conn.commit()
            logger.debug(f"Wrote {len(data)} {action_type} actions for {symbol}")
            return len(data)
