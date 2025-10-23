"""Worker threads for background tasks."""
import logging
import subprocess
import sys
import os
from pathlib import Path
from typing import Optional
from PyQt6.QtCore import QThread, pyqtSignal

logger = logging.getLogger(__name__)


class BackfillWorker(QThread):
    """
    Background thread that runs the backfill process via subprocess.
    
    Parses output from backfill_historical_data.py and emits signals
    to update the GUI with real-time progress.
    """
    
    log_signal = pyqtSignal(str)
    stats_signal = pyqtSignal(dict)
    task_update_signal = pyqtSignal(str, str, dict)  # task_id, state, info
    finished_signal = pyqtSignal(bool, str)  # success, message
    phase_signal = pyqtSignal(str, dict)  # phase_name, data (for Phase 1/2/3)
    queue_signal = pyqtSignal(int, int, int, int)  # dl_qsize, pr_qsize, dl_maxsize, pr_maxsize
    
    def __init__(
        self,
        ticker: str,
        start_date: str,
        end_date: str,
        years: int = 5,
        debug: bool = False,
        backfill: bool = True
    ):
        """
        Initialize backfill worker.
        
        Args:
            ticker: Stock ticker symbol
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            years: Number of years to backfill
            debug: Enable debug mode
            backfill: Enable hybrid backfill mode (flatfiles + REST API gap filling)
        """
        super().__init__()
        self.ticker = ticker
        self.start_date = start_date
        self.end_date = end_date
        self.years = years
        self.debug = debug
        self.backfill = backfill
        self.process: Optional[subprocess.Popen] = None
        self.is_running = False
        
    def run(self):
        """Run the backfill process as subprocess."""
        try:
            self.is_running = True
            
            # Get the Python executable from the virtual environment
            venv_python = sys.executable
            
            # Build command
            cmd = [
                venv_python,
                'scripts/backfill_historical_data.py',
                '--tickers', self.ticker,
                '--flatfiles',
                '--years', str(self.years)
            ]
            
            # Add debug flag if enabled
            if self.debug:
                cmd.append('--debug')
            
            # Add backfill flag if enabled (triggers hybrid mode: flat files + REST API gap filling)
            if self.backfill:
                cmd.append('--backfill')
            
            self.log_signal.emit(f"Starting backfill: {' '.join(cmd)}")
            
            # Activate venv and set environment
            env = os.environ.copy()
            
            # Start process
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                env=env,
                cwd=str(Path(__file__).parent.parent.parent)
            )
            
            # Read output line by line
            if self.process.stdout:
                for line in iter(self.process.stdout.readline, ''):
                    if not self.is_running:
                        break
                    
                    line = line.rstrip()
                    if line:
                        self.log_signal.emit(line)
                        self._parse_log_line(line)
            
            # Wait for process to complete
            return_code = self.process.wait()
            
            if return_code == 0:
                self.finished_signal.emit(True, "Backfill completed successfully")
            else:
                self.finished_signal.emit(False, f"Backfill failed with exit code {return_code}")
                
        except Exception as e:
            self.log_signal.emit(f"ERROR: {e}")
            self.finished_signal.emit(False, str(e))
        finally:
            self.is_running = False
    
    def _parse_log_line(self, line: str):
        """
        Parse log line and emit appropriate signals.
        
        This method extracts structured data from log output and emits
        signals for:
        - Real-time queue metrics
        - Phase progress (Phase 1/2/3/4)
        - Download/processing statistics
        """
        # Real-time queue metrics
        if "⚡ Pipeline Metrics" in line and "Download Q:" in line:
            try:
                # Extract queue sizes (current/max format)
                dl_part = line.split("Download Q:")[1].split("|")[0].strip()
                pr_part = line.split("Process Q:")[1].split("|")[0].strip()
                
                # Parse "current/max" format
                dl_current, dl_max = map(int, dl_part.split("/"))
                pr_current, pr_max = map(int, pr_part.split("/"))
                
                # Emit queue sizes AND maxsizes to GUI for dynamic progress bars
                self.queue_signal.emit(dl_current, pr_current, dl_max, pr_max)
                
                # Extract counts and stats
                downloaded_count = 0
                if "Downloaded:" in line:
                    downloaded_part = line.split("Downloaded:")[1].split("(")[0].strip()
                    downloaded_count = int(downloaded_part)
                
                processed_count = 0
                if "Processed:" in line:
                    processed_part = line.split("Processed:")[1].split("|")[0].strip()
                    processed_count = int(processed_part)
                
                download_mbps = 0.0
                if "MB/s avg" in line:
                    mbps_part = line.split("(")[1].split("MB/s")[0].strip()
                    download_mbps = float(mbps_part)
                
                download_latency_ms = 0.0
                process_latency_ms = 0.0
                if "Latency: DL=" in line:
                    latency_part = line.split("Latency: DL=")[1]
                    download_latency_ms = float(latency_part.split("ms")[0])
                    if "PR=" in latency_part:
                        process_latency_ms = float(latency_part.split("PR=")[1].split("ms")[0])
                
                # Emit all stats
                self.stats_signal.emit({
                    'downloaded': downloaded_count,
                    'processed': processed_count,
                    'download_mbps': download_mbps,
                    'download_latency_ms': download_latency_ms,
                    'process_latency_ms': process_latency_ms
                })
            except Exception as e:
                pass  # Silently ignore parsing errors
        
        # Phase 1: Reference Data
        if "=== PHASE 1: Reference Data" in line:
            self.phase_signal.emit('phase1_start', {})
        
        # TODO: Add more phase parsers as needed
        # The full parsing logic is extensive (500+ lines)
        # For now, this is a simplified version focusing on core metrics
    
    def stop(self):
        """Stop the backfill process gracefully."""
        logger.info("Stopping backfill worker...")
        self.is_running = False
        
        if self.process and self.process.poll() is None:
            # Send SIGTERM (graceful shutdown)
            self.process.terminate()
            
            # Wait up to 5 seconds for graceful shutdown
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                # Force kill if still running
                logger.warning("Backfill process did not terminate gracefully, force killing...")
                self.process.kill()
                self.process.wait()
        
        self.quit()
        self.wait(3000)


class PurgeWorker(QThread):
    """
    Background thread for purging flatfiles from data/flatfiles directory.
    
    This removes all downloaded CSV/CSV.GZ files to free up disk space.
    """
    
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int, int)  # current, total
    finished_signal = pyqtSignal(bool, str)  # success, message
    
    def __init__(self, flatfiles_dir: Path):
        """
        Initialize purge worker.
        
        Args:
            flatfiles_dir: Path to flatfiles directory
        """
        super().__init__()
        self.flatfiles_dir = flatfiles_dir
        
    def run(self):
        """Purge flatfiles from disk."""
        try:
            if not self.flatfiles_dir.exists():
                self.finished_signal.emit(True, "No flatfiles directory found")
                return
            
            # Find all CSV/CSV.GZ files
            csv_files = list(self.flatfiles_dir.rglob("*.csv"))
            gz_files = list(self.flatfiles_dir.rglob("*.csv.gz"))
            all_files = csv_files + gz_files
            
            total_files = len(all_files)
            
            if total_files == 0:
                self.finished_signal.emit(True, "No flatfiles to purge")
                return
            
            self.log_signal.emit(f"Found {total_files} flatfiles to delete")
            
            # Delete files
            deleted_count = 0
            for i, file_path in enumerate(all_files, 1):
                try:
                    file_path.unlink()
                    deleted_count += 1
                    self.progress_signal.emit(i, total_files)
                    
                    if i % 100 == 0:
                        self.log_signal.emit(f"Deleted {i}/{total_files} files...")
                        
                except Exception as e:
                    self.log_signal.emit(f"Error deleting {file_path.name}: {e}")
            
            self.finished_signal.emit(True, f"Deleted {deleted_count}/{total_files} flatfiles")
            
        except Exception as e:
            logger.error(f"Purge error: {e}", exc_info=True)
            self.finished_signal.emit(False, str(e))
