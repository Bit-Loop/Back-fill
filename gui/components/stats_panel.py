"""
Stats Panel Component - Real-time queue depth indicators

Provides:
- Download queue depth progress bar
- Process queue depth progress bar
- Dynamic maxsize updates
- Color-coded visualization (blue for download, orange for process)

Public Methods:
    update_download_queue(current: int, maxsize: int): Update download queue metrics
    update_process_queue(current: int, maxsize: int): Update process queue metrics
    reset(): Reset all progress bars to 0
"""

from PyQt6.QtWidgets import QGroupBox, QVBoxLayout, QHBoxLayout, QLabel, QProgressBar


class StatsPanel(QGroupBox):
    """
    Real-time queue depth indicators panel.
    
    Displays:
    - Download Queue: Current depth / Max capacity
    - Process Queue: Current depth / Max capacity
    
    Both progress bars dynamically update their max values and formats
    to reflect actual queue sizes.
    """
    
    def __init__(self, parent=None):
        super().__init__("Real-Time Queue Metrics", parent)
        self._init_ui()
    
    def _init_ui(self):
        """Initialize stats panel UI"""
        layout = QVBoxLayout()
        
        # Download Queue Depth
        dl_queue_layout = QHBoxLayout()
        dl_queue_layout.addWidget(QLabel("Download Queue:"))
        
        self.download_queue_bar = QProgressBar()
        self.download_queue_bar.setMinimum(0)
        self.download_queue_bar.setMaximum(100)  # Default, updated dynamically
        self.download_queue_bar.setValue(0)
        self.download_queue_bar.setTextVisible(True)
        self.download_queue_bar.setFormat("%v/100")  # Default, updated dynamically
        self.download_queue_bar.setStyleSheet(
            "QProgressBar::chunk { background-color: #2196F3; }"
        )
        
        dl_queue_layout.addWidget(self.download_queue_bar)
        layout.addLayout(dl_queue_layout)
        
        # Process Queue Depth
        pr_queue_layout = QHBoxLayout()
        pr_queue_layout.addWidget(QLabel("Process Queue:"))
        
        self.process_queue_bar = QProgressBar()
        self.process_queue_bar.setMinimum(0)
        self.process_queue_bar.setMaximum(100)  # Default, updated dynamically
        self.process_queue_bar.setValue(0)
        self.process_queue_bar.setTextVisible(True)
        self.process_queue_bar.setFormat("%v/100")  # Default, updated dynamically
        self.process_queue_bar.setStyleSheet(
            "QProgressBar::chunk { background-color: #FF9800; }"
        )
        
        pr_queue_layout.addWidget(self.process_queue_bar)
        layout.addLayout(pr_queue_layout)
        
        self.setLayout(layout)
    
    # ============================================================
    # PUBLIC API
    # ============================================================
    
    def update_download_queue(self, current: int, maxsize: int):
        """
        Update download queue depth indicator.
        
        Args:
            current: Current queue size
            maxsize: Maximum queue capacity
        """
        self.download_queue_bar.setMaximum(maxsize)
        self.download_queue_bar.setValue(current)
        self.download_queue_bar.setFormat(f"%v/{maxsize}")
    
    def update_process_queue(self, current: int, maxsize: int):
        """
        Update process queue depth indicator.
        
        Args:
            current: Current queue size
            maxsize: Maximum queue capacity
        """
        self.process_queue_bar.setMaximum(maxsize)
        self.process_queue_bar.setValue(current)
        self.process_queue_bar.setFormat(f"%v/{maxsize}")
    
    def reset(self):
        """Reset all progress bars to initial state"""
        self.download_queue_bar.setMaximum(100)
        self.download_queue_bar.setValue(0)
        self.download_queue_bar.setFormat("%v/100")
        
        self.process_queue_bar.setMaximum(100)
        self.process_queue_bar.setValue(0)
        self.process_queue_bar.setFormat("%v/100")
