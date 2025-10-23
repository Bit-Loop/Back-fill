"""
Phase 3 Table - Daily Bars display (also shows Phase 4 debug data)

Shows Phase 3 (aggregate per-ticker):
- Ticker symbol
- Date range
- Bar count
- Status
- Progress percentage

Shows Phase 4 (debug mode - per-flatfile):
- Date
- Tickers in file
- Bar count
- Status
- Progress

Public Methods:
    set_debug_mode(enabled: bool): Switch between Phase 3 and Phase 4 display
    update_ticker(ticker: str, data: dict): Update Phase 3 ticker data
    update_flatfile(date: str, data: dict): Update Phase 4 flatfile data
    clear(): Clear all table data
"""

from typing import Dict
from PyQt6.QtWidgets import QTableWidget, QTableWidgetItem
from PyQt6.QtGui import QFont, QColor


class Phase3DailyTable(QTableWidget):
    """
    Phase 3 Daily Bars table widget.
    
    Normal Mode: Shows aggregate daily bars per ticker
    Debug Mode: Shows individual flatfile dates with all tickers
    """
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.phase3_data: Dict[str, Dict] = {}  # Aggregate per-ticker
        self.phase4_data: Dict[str, Dict] = {}  # Per-flatfile (debug)
        self.debug_mode: bool = False
        self._init_ui()
    
    def _init_ui(self):
        """Initialize table UI (Phase 3 headers by default)"""
        self.setColumnCount(5)
        self.setHorizontalHeaderLabels([
            "Ticker", "Date Range", "Bars", "Status", "Progress"
        ])
        header = self.horizontalHeader()
        if header:
            from PyQt6.QtWidgets import QHeaderView
            header.setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
    
    # ============================================================
    # PUBLIC API
    # ============================================================
    
    def set_debug_mode(self, enabled: bool):
        """
        Switch between Phase 3 (aggregate) and Phase 4 (debug) display.
        
        Args:
            enabled: True for debug mode (Phase 4), False for normal (Phase 3)
        """
        self.debug_mode = enabled
        self._refresh()
    
    def update_ticker(self, ticker: str, data: dict):
        """
        Update Phase 3 ticker data (aggregate daily bars).
        
        Args:
            ticker: Stock ticker symbol
            data: Dict with keys: bars, date_range, status, progress
        """
        self.phase3_data[ticker] = {
            'bars': data.get('bars', 0),
            'status': data.get('status', 'Processing'),
            'progress': data.get('progress', 0),
            'date_range': data.get('date_range', '')
        }
        self._refresh()
    
    def update_flatfile(self, date: str, data: dict):
        """
        Update Phase 4 flatfile data (debug mode).
        
        Args:
            date: Date string (YYYY-MM-DD)
            data: Dict with keys: bars, tickers, status
        """
        self.phase4_data[date] = {
            'bars': data.get('bars', 0),
            'tickers': data.get('tickers', ''),
            'status': data.get('status', 'Complete')
        }
        if self.debug_mode:
            self._refresh()
    
    def clear(self):
        """Clear all table data"""
        self.phase3_data.clear()
        self.phase4_data.clear()
        self.setRowCount(0)
    
    # ============================================================
    # PRIVATE METHODS
    # ============================================================
    
    def _refresh(self):
        """Refresh table display based on debug mode"""
        if self.debug_mode and self.phase4_data:
            self._refresh_debug_mode()
        else:
            self._refresh_normal_mode()
    
    def _refresh_debug_mode(self):
        """Refresh Phase 4 debug data (per-flatfile)"""
        # Update headers for debug mode
        self.setHorizontalHeaderLabels([
            "Date", "Tickers", "Bars", "Status", "Progress"
        ])
        
        self.setRowCount(len(self.phase4_data))
        
        for row, (date_str, info) in enumerate(sorted(self.phase4_data.items())):
            # Date (bold, monospace)
            date_item = QTableWidgetItem(date_str)
            date_item.setFont(QFont("Courier New", weight=QFont.Weight.Bold))
            self.setItem(row, 0, date_item)
            
            # Tickers
            tickers_item = QTableWidgetItem(info.get('tickers', ''))
            self.setItem(row, 1, tickers_item)
            
            # Bars (formatted with commas)
            bars = info.get('bars', 0)
            bars_item = QTableWidgetItem(f"{bars:,}")
            self.setItem(row, 2, bars_item)
            
            # Status (color-coded)
            status = info.get('status', 'Processing')
            status_item = QTableWidgetItem(status)
            
            if status == 'Complete':
                status_item.setForeground(QColor('#4CAF50'))  # Green
            else:
                status_item.setForeground(QColor('#FF9800'))  # Orange
            
            self.setItem(row, 3, status_item)
            
            # Progress (always 100% for completed files)
            progress_item = QTableWidgetItem("100%")
            self.setItem(row, 4, progress_item)
    
    def _refresh_normal_mode(self):
        """Refresh Phase 3 aggregate data (per-ticker)"""
        # Update headers for normal mode
        self.setHorizontalHeaderLabels([
            "Ticker", "Date Range", "Bars", "Status", "Progress"
        ])
        
        self.setRowCount(len(self.phase3_data))
        
        for row, (ticker, info) in enumerate(sorted(self.phase3_data.items())):
            # Ticker (bold, monospace)
            ticker_item = QTableWidgetItem(ticker)
            ticker_item.setFont(QFont("Courier New", weight=QFont.Weight.Bold))
            self.setItem(row, 0, ticker_item)
            
            # Date Range
            range_item = QTableWidgetItem(info.get('date_range', ''))
            self.setItem(row, 1, range_item)
            
            # Bars (formatted with commas)
            bars = info.get('bars', 0)
            bars_item = QTableWidgetItem(f"{bars:,}")
            self.setItem(row, 2, bars_item)
            
            # Status (color-coded)
            status = info.get('status', 'Processing')
            status_item = QTableWidgetItem(status)
            
            if status == 'Complete':
                status_item.setForeground(QColor('#4CAF50'))  # Green
            else:
                status_item.setForeground(QColor('#FF9800'))  # Orange
            
            self.setItem(row, 3, status_item)
            
            # Progress
            progress = info.get('progress', 0)
            progress_item = QTableWidgetItem(f"{progress}%")
            self.setItem(row, 4, progress_item)
