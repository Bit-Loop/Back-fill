"""
Control Panel Component - User input controls for backfill configuration

Provides:
- Ticker symbol input
- Date range selection (start/end dates)
- Start/Stop backfill buttons
- Backfill mode checkbox (hybrid flatfile + REST)
- Purge flatfiles button

Emits signals when user actions occur (Start, Stop, Purge).
"""

from PyQt6.QtWidgets import (
    QGroupBox, QHBoxLayout, QLabel, QLineEdit,
    QDateEdit, QPushButton, QCheckBox
)
from PyQt6.QtCore import Qt, pyqtSignal, QDate


class ControlPanel(QGroupBox):
    """
    Control panel for backfill configuration and actions.
    
    Signals:
        start_clicked: Emitted when "Start Backfill" is clicked
        stop_clicked: Emitted when "Stop" is clicked
        purge_clicked: Emitted when "Purge Flatfiles" is clicked
    
    Public Methods:
        get_ticker() -> str: Returns current ticker symbol
        get_start_date() -> str: Returns start date as 'YYYY-MM-DD'
        get_end_date() -> str: Returns end date as 'YYYY-MM-DD'
        is_backfill_enabled() -> bool: Returns backfill checkbox state
        set_running(is_running: bool): Updates button states
    """
    
    # Signals
    start_clicked = pyqtSignal()
    stop_clicked = pyqtSignal()
    purge_clicked = pyqtSignal()
    
    def __init__(self, parent=None):
        super().__init__("Control Panel", parent)
        self._init_ui()
    
    def _init_ui(self):
        """Initialize control panel UI"""
        layout = QHBoxLayout()
        
        # Stock ticker input
        layout.addWidget(QLabel("Ticker:"))
        self.ticker_input = QLineEdit()
        self.ticker_input.setText("AMD")
        self.ticker_input.setMaximumWidth(100)
        layout.addWidget(self.ticker_input)
        
        # Start date picker
        layout.addWidget(QLabel("Start Date:"))
        self.start_date = QDateEdit()
        self.start_date.setDate(QDate.currentDate().addYears(-5))
        self.start_date.setCalendarPopup(True)
        self.start_date.setDisplayFormat("yyyy-MM-dd")
        layout.addWidget(self.start_date)
        
        # End date picker
        layout.addWidget(QLabel("End Date:"))
        self.end_date = QDateEdit()
        self.end_date.setDate(QDate.currentDate())
        self.end_date.setCalendarPopup(True)
        self.end_date.setDisplayFormat("yyyy-MM-dd")
        layout.addWidget(self.end_date)
        
        # Start button
        self.go_button = QPushButton("▶ Start Backfill")
        self.go_button.setStyleSheet(
            "background-color: #4CAF50; color: white; font-weight: bold; padding: 8px;"
        )
        self.go_button.clicked.connect(self.start_clicked.emit)
        layout.addWidget(self.go_button)
        
        # Stop button
        self.stop_button = QPushButton("⬛ Stop")
        self.stop_button.setStyleSheet(
            "background-color: #f44336; color: white; font-weight: bold; padding: 8px;"
        )
        self.stop_button.clicked.connect(self.stop_clicked.emit)
        self.stop_button.setEnabled(False)
        layout.addWidget(self.stop_button)
        
        # Backfill mode checkbox
        self.backfill_checkbox = QCheckBox("Run Backfill")
        self.backfill_checkbox.setChecked(True)
        self.backfill_checkbox.setToolTip(
            "Enable hybrid backfill: flat files first, then REST API to fill gaps > 3 days"
        )
        layout.addWidget(self.backfill_checkbox)
        
        # Purge flatfiles button
        self.purge_button = QPushButton("🗑️ Purge Flatfiles")
        self.purge_button.setStyleSheet(
            "background-color: #D32F2F; color: white; font-weight: bold; padding: 8px;"
        )
        self.purge_button.clicked.connect(self.purge_clicked.emit)
        self.purge_button.setToolTip(
            "Delete all downloaded flatpack files (.csv, .csv.gz) from data/flatfiles"
        )
        layout.addWidget(self.purge_button)
        
        layout.addStretch()
        self.setLayout(layout)
    
    # ============================================================
    # PUBLIC API
    # ============================================================
    
    def get_ticker(self) -> str:
        """Get the current ticker symbol"""
        return self.ticker_input.text().strip().upper()
    
    def get_start_date(self) -> str:
        """Get the start date as 'YYYY-MM-DD'"""
        return self.start_date.date().toString("yyyy-MM-dd")
    
    def get_end_date(self) -> str:
        """Get the end date as 'YYYY-MM-DD'"""
        return self.end_date.date().toString("yyyy-MM-dd")
    
    def is_backfill_enabled(self) -> bool:
        """Check if hybrid backfill mode is enabled"""
        return self.backfill_checkbox.isChecked()
    
    def set_running(self, is_running: bool):
        """
        Update button states based on backfill running status.
        
        Args:
            is_running: True if backfill is currently running
        """
        self.go_button.setEnabled(not is_running)
        self.stop_button.setEnabled(is_running)
        self.ticker_input.setEnabled(not is_running)
        self.start_date.setEnabled(not is_running)
        self.end_date.setEnabled(not is_running)
        self.backfill_checkbox.setEnabled(not is_running)
