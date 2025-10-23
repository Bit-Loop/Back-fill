"""
Log Widget Component - Live log output with filtering

Provides:
- Color-coded log messages (error, warning, info, debug)
- Log level filtering (ALL, INFO, WARNING, ERROR, DEBUG)
- Clear log button
- Auto-scroll to bottom
- Dark theme styling

Public Methods:
    append_log(text: str): Append colored log message
    clear(): Clear all log output
"""

from PyQt6.QtWidgets import (
    QGroupBox, QVBoxLayout, QHBoxLayout, QLabel,
    QComboBox, QPushButton, QTextEdit
)


class LogWidget(QGroupBox):
    """
    Live log output widget with filtering and auto-scroll.
    
    Features:
    - Color-coded messages based on log level
    - Log level filter dropdown
    - Clear log button
    - Auto-scroll to bottom on new messages
    - Dark theme console styling
    """
    
    def __init__(self, parent=None):
        super().__init__("Live Log Output", parent)
        self._init_ui()
    
    def _init_ui(self):
        """Initialize log widget UI"""
        layout = QVBoxLayout()
        
        # Log controls (filter + clear button)
        controls_layout = QHBoxLayout()
        controls_layout.addWidget(QLabel("Filter:"))
        
        self.log_filter = QComboBox()
        self.log_filter.addItems(["ALL", "INFO", "WARNING", "ERROR", "DEBUG"])
        self.log_filter.currentTextChanged.connect(self._apply_log_filter)
        controls_layout.addWidget(self.log_filter)
        
        clear_btn = QPushButton("Clear Log")
        clear_btn.clicked.connect(self.clear)
        controls_layout.addWidget(clear_btn)
        
        controls_layout.addStretch()
        layout.addLayout(controls_layout)
        
        # Log text area (read-only, dark theme)
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setMaximumHeight(200)
        self.log_output.setStyleSheet(
            "background-color: #1e1e1e; "
            "color: #d4d4d4; "
            "font-family: 'Courier New';"
        )
        layout.addWidget(self.log_output)
        
        self.setLayout(layout)
    
    # ============================================================
    # PUBLIC API
    # ============================================================
    
    def append_log(self, text: str):
        """
        Append color-coded log message.
        
        Args:
            text: Log message (will be color-coded based on keywords)
        
        Color Scheme:
            - ERROR/FAILED: Red (#f44336)
            - WARNING: Orange (#FF9800)
            - INFO/✓: Green (#4CAF50)
            - DEBUG: Gray (#9E9E9E)
            - Default: Light Gray (#d4d4d4)
        """
        # Color code based on log level keywords
        if "ERROR" in text or "FAILED" in text:
            color = "#f44336"
        elif "WARNING" in text:
            color = "#FF9800"
        elif "INFO" in text or "✓" in text:
            color = "#4CAF50"
        elif "DEBUG" in text:
            color = "#9E9E9E"
        else:
            color = "#d4d4d4"
        
        self.log_output.append(f'<span style="color: {color}">{text}</span>')
        
        # Auto-scroll to bottom
        scrollbar = self.log_output.verticalScrollBar()
        if scrollbar:
            scrollbar.setValue(scrollbar.maximum())
    
    def clear(self):
        """Clear all log output"""
        self.log_output.clear()
    
    # ============================================================
    # PRIVATE METHODS
    # ============================================================
    
    def _apply_log_filter(self):
        """
        Apply log level filter.
        
        Note: Current implementation is a placeholder.
        To fully implement filtering, would need to:
        1. Buffer all log messages internally
        2. Re-render visible messages when filter changes
        3. Support regex filtering for advanced use cases
        """
        # TODO: Implement log buffering and filtering
        pass
