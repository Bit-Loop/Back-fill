"""
GUI Main Entry Point
Real-time market data visualization and backfill monitoring.

Usage:
    python gui.py
"""
import sys
import logging
from pathlib import Path
from PyQt6.QtWidgets import QApplication

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(threadName)-15s] %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main GUI entry point"""
    # Load environment
    load_dotenv()
    
    # Import after environment setup
    from gui.main_window import BackfillVisualizerWindow
    
    # Create Qt application
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
    
    # Create and show main window
    window = BackfillVisualizerWindow()
    window.show()
    
    # Run event loop
    sys.exit(app.exec())


if __name__ == '__main__':
    main()
