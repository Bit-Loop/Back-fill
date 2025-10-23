"""
Phase 2 Table - Corporate Actions display

Shows:
- Dividends: Summary row + details (up to 3)
- Stock Splits: Summary row + all details

Each ticker shows:
- Type: "Dividends" (blue) or "Splits" (orange)
- Date
- Amount/Ratio
- Count
- Status

Public Methods:
    update_dividends(ticker: str, count: int): Update dividend count
    add_dividend_detail(ticker: str, amount: float, date: str): Add dividend detail
    update_splits(ticker: str, count: int): Update split count
    add_split_detail(ticker: str, ratio: float, date: str): Add split detail
    set_current_ticker(ticker: str): Track current ticker for details
    clear(): Clear all table data
"""

from typing import Dict, List
from PyQt6.QtWidgets import QTableWidget, QTableWidgetItem
from PyQt6.QtGui import QFont, QColor


class Phase2CorporateTable(QTableWidget):
    """
    Phase 2 Corporate Actions table widget.
    
    Displays:
    - Dividend summary rows with first 3 details
    - Split summary rows with all details
    - Color-coded by type (blue=dividends, orange=splits)
    """
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.phase2_data: Dict[str, Dict] = {}
        self.current_ticker: str = ""
        self._init_ui()
    
    def _init_ui(self):
        """Initialize table UI"""
        self.setColumnCount(6)
        self.setHorizontalHeaderLabels([
            "Ticker", "Type", "Date", "Amount/Ratio", "Count", "Status"
        ])
        header = self.horizontalHeader()
        if header:
            from PyQt6.QtWidgets import QHeaderView
            header.setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
    
    # ============================================================
    # PUBLIC API
    # ============================================================
    
    def set_current_ticker(self, ticker: str):
        """Track current ticker for detail additions"""
        self.current_ticker = ticker
    
    def update_dividends(self, ticker: str, count: int):
        """
        Update dividend count for ticker.
        
        Args:
            ticker: Stock ticker symbol
            count: Number of dividends found
        """
        if ticker not in self.phase2_data:
            self.phase2_data[ticker] = {
                'dividends': 0,
                'splits': 0,
                'div_details': [],
                'split_details': []
            }
        
        self.phase2_data[ticker]['dividends'] = count
        self.current_ticker = ticker
        self._refresh()
    
    def add_dividend_detail(self, ticker: str, amount: float, date: str):
        """
        Add dividend detail for ticker.
        
        Args:
            ticker: Stock ticker symbol
            amount: Dividend amount ($)
            date: Ex-dividend date (YYYY-MM-DD)
        """
        if ticker in self.phase2_data:
            self.phase2_data[ticker]['div_details'].append({
                'amount': amount,
                'date': date
            })
            self._refresh()
    
    def update_splits(self, ticker: str, count: int):
        """
        Update split count for ticker.
        
        Args:
            ticker: Stock ticker symbol
            count: Number of splits found
        """
        if ticker not in self.phase2_data:
            self.phase2_data[ticker] = {
                'dividends': 0,
                'splits': 0,
                'div_details': [],
                'split_details': []
            }
        
        self.phase2_data[ticker]['splits'] = count
        self.current_ticker = ticker
        self._refresh()
    
    def add_split_detail(self, ticker: str, ratio: float, date: str):
        """
        Add split detail for ticker.
        
        Args:
            ticker: Stock ticker symbol
            ratio: Split ratio (e.g., 2.0 for 2:1 split)
            date: Execution date (YYYY-MM-DD)
        """
        if ticker in self.phase2_data:
            self.phase2_data[ticker]['split_details'].append({
                'ratio': ratio,
                'date': date
            })
            self._refresh()
    
    def clear(self):
        """Clear all table data"""
        self.phase2_data.clear()
        self.current_ticker = ""
        self.setRowCount(0)
    
    # ============================================================
    # PRIVATE METHODS
    # ============================================================
    
    def _refresh(self):
        """Refresh table display with summary + details"""
        # Count total rows needed (summary + detail rows)
        total_rows = 0
        for ticker, info in self.phase2_data.items():
            # Dividend summary + up to 3 details
            total_rows += 1
            if info['dividends'] > 0:
                total_rows += min(3, len(info['div_details']))
            
            # Split summary + all details
            if info['splits'] > 0:
                total_rows += 1
                total_rows += len(info['split_details'])
        
        self.setRowCount(total_rows)
        
        current_row = 0
        for ticker in sorted(self.phase2_data.keys()):
            info = self.phase2_data[ticker]
            
            # ============================================================
            # DIVIDEND SUMMARY ROW
            # ============================================================
            ticker_item = QTableWidgetItem(ticker)
            ticker_item.setFont(QFont("Courier New", weight=QFont.Weight.Bold))
            self.setItem(current_row, 0, ticker_item)
            
            type_item = QTableWidgetItem("Dividends")
            type_item.setForeground(QColor('#2196F3'))  # Blue
            self.setItem(current_row, 1, type_item)
            
            self.setItem(current_row, 2, QTableWidgetItem("Summary"))
            self.setItem(current_row, 3, QTableWidgetItem("—"))
            
            count_item = QTableWidgetItem(f"{info['dividends']} found")
            count_font = QFont("Courier New", weight=QFont.Weight.Bold)
            count_item.setFont(count_font)
            self.setItem(current_row, 4, count_item)
            
            status_text = "OK" if info['dividends'] > 0 else "None"
            status_color = QColor('#4CAF50') if info['dividends'] > 0 else QColor('#9E9E9E')
            status_item = QTableWidgetItem(status_text)
            status_item.setForeground(status_color)
            self.setItem(current_row, 5, status_item)
            
            current_row += 1
            
            # ============================================================
            # DIVIDEND DETAILS (first 3)
            # ============================================================
            for div in info['div_details'][:3]:
                self.setItem(current_row, 0, QTableWidgetItem("  ↳"))
                self.setItem(current_row, 1, QTableWidgetItem("Dividend"))
                self.setItem(current_row, 2, QTableWidgetItem(div['date']))
                
                amount_item = QTableWidgetItem(f"${div['amount']:.4f}")
                amount_item.setForeground(QColor('#4CAF50'))
                self.setItem(current_row, 3, amount_item)
                
                self.setItem(current_row, 4, QTableWidgetItem("—"))
                self.setItem(current_row, 5, QTableWidgetItem("✓"))
                
                current_row += 1
            
            # ============================================================
            # SPLIT SUMMARY ROW (if any splits)
            # ============================================================
            if info['splits'] > 0:
                ticker_item = QTableWidgetItem(ticker)
                ticker_item.setFont(QFont("Courier New", weight=QFont.Weight.Bold))
                self.setItem(current_row, 0, ticker_item)
                
                type_item = QTableWidgetItem("Splits")
                type_item.setForeground(QColor('#FF9800'))  # Orange
                self.setItem(current_row, 1, type_item)
                
                self.setItem(current_row, 2, QTableWidgetItem("Summary"))
                self.setItem(current_row, 3, QTableWidgetItem("—"))
                
                count_item = QTableWidgetItem(f"{info['splits']} found")
                count_font = QFont("Courier New", weight=QFont.Weight.Bold)
                count_item.setFont(count_font)
                self.setItem(current_row, 4, count_item)
                
                status_item = QTableWidgetItem("OK")
                status_item.setForeground(QColor('#4CAF50'))
                self.setItem(current_row, 5, status_item)
                
                current_row += 1
                
                # ============================================================
                # SPLIT DETAILS (all)
                # ============================================================
                for split in info['split_details']:
                    self.setItem(current_row, 0, QTableWidgetItem("  ↳"))
                    self.setItem(current_row, 1, QTableWidgetItem("Split"))
                    self.setItem(current_row, 2, QTableWidgetItem(split['date']))
                    
                    ratio_item = QTableWidgetItem(f"{split['ratio']:.2f}:1")
                    ratio_item.setForeground(QColor('#FF9800'))
                    self.setItem(current_row, 3, ratio_item)
                    
                    self.setItem(current_row, 4, QTableWidgetItem("—"))
                    self.setItem(current_row, 5, QTableWidgetItem("✓"))
                    
                    current_row += 1
