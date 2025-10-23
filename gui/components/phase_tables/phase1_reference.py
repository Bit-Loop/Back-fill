"""
Phase 1 Table - Reference Data display

Shows:
- Ticker symbol
- Company name
- Exchange
- Asset type
- Market capitalization
- Status (Success/Missing)

Public Methods:
    update_ticker(ticker: str, data: dict): Update or add ticker info
    clear(): Clear all table data
    get_data() -> dict: Get current phase 1 data
"""

from typing import Dict
from PyQt6.QtWidgets import QTableWidget, QTableWidgetItem, QHeaderView
from PyQt6.QtGui import QFont, QColor


class Phase1ReferenceTable(QTableWidget):
    """
    Phase 1 Reference Data table widget.
    
    Displays metadata for each ticker: name, exchange, type, market cap, status.
    Color-codes status: Green (Success), Red (Missing), Orange (Other).
    """
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.phase1_data: Dict[str, Dict] = {}
        self._init_ui()
    
    def _init_ui(self):
        """Initialize table UI"""
        self.setColumnCount(6)
        self.setHorizontalHeaderLabels([
            "Ticker", "Name", "Exchange", "Type", "Market Cap", "Status"
        ])
        header = self.horizontalHeader()
        if header:
            header.setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
    
    # ============================================================
    # PUBLIC API
    # ============================================================
    
    def update_ticker(self, ticker: str, data: dict):
        """
        Update or add ticker reference data.
        
        Args:
            ticker: Stock ticker symbol
            data: Dict with keys: status, name, exchange, type, market_cap
        """
        self.phase1_data[ticker] = {
            'status': data.get('status', 'Unknown'),
            'name': data.get('name', ''),
            'exchange': data.get('exchange', ''),
            'type': data.get('type', ''),
            'market_cap': data.get('market_cap', '')
        }
        self._refresh()
    
    def clear(self):
        """Clear all table data"""
        self.phase1_data.clear()
        self.setRowCount(0)
    
    def get_data(self) -> Dict[str, Dict]:
        """Get current phase 1 data"""
        return self.phase1_data.copy()
    
    # ============================================================
    # PRIVATE METHODS
    # ============================================================
    
    def _refresh(self):
        """Refresh table display"""
        self.setRowCount(len(self.phase1_data))
        
        for row, (ticker, info) in enumerate(sorted(self.phase1_data.items())):
            # Ticker (bold, monospace)
            ticker_item = QTableWidgetItem(ticker)
            ticker_item.setFont(QFont("Courier New", weight=QFont.Weight.Bold))
            self.setItem(row, 0, ticker_item)
            
            # Name
            name_item = QTableWidgetItem(info.get('name', ''))
            self.setItem(row, 1, name_item)
            
            # Exchange
            exchange_item = QTableWidgetItem(info.get('exchange', ''))
            self.setItem(row, 2, exchange_item)
            
            # Type
            type_item = QTableWidgetItem(info.get('type', ''))
            self.setItem(row, 3, type_item)
            
            # Market Cap
            market_cap_item = QTableWidgetItem(info.get('market_cap', ''))
            self.setItem(row, 4, market_cap_item)
            
            # Status (color-coded)
            status = info.get('status', 'Unknown')
            status_item = QTableWidgetItem(status)
            
            if status == 'Success':
                status_item.setForeground(QColor('#4CAF50'))  # Green
            elif status == 'Missing':
                status_item.setForeground(QColor('#f44336'))  # Red
            else:
                status_item.setForeground(QColor('#FF9800'))  # Orange
            
            self.setItem(row, 5, status_item)
