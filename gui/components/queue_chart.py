"""Real-time queue visualization charts using matplotlib."""
import logging
from collections import deque
from typing import Optional
from PyQt6.QtWidgets import QWidget, QVBoxLayout
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

logger = logging.getLogger(__name__)


class QueueChartWidget(QWidget):
    """
    Live multi-line charts for pipeline progress and performance metrics.
    
    Displays two stacked charts:
    1. Pipeline Progress: Downloaded, Processed, Skipped, Failed, Retrying
    2. Performance Metrics: Active tasks, queue sizes, and latencies
    """
    
    def __init__(self, parent: Optional[QWidget] = None, max_samples: int = 100):
        """
        Initialize queue chart widget.
        
        Args:
            parent: Parent widget
            max_samples: Maximum number of samples to display
        """
        super().__init__(parent)
        
        # Data storage (last N samples)
        self.max_samples = max_samples
        self.time_history = deque(maxlen=self.max_samples)
        
        # Pipeline Progress metrics
        self.downloaded_history = deque(maxlen=self.max_samples)
        self.processed_history = deque(maxlen=self.max_samples)
        self.skipped_history = deque(maxlen=self.max_samples)
        self.failed_history = deque(maxlen=self.max_samples)
        self.retrying_history = deque(maxlen=self.max_samples)
        
        # Performance metrics - latencies and queue
        self.download_latency_history = deque(maxlen=self.max_samples)
        self.process_latency_history = deque(maxlen=self.max_samples)
        self.process_qsize_history = deque(maxlen=self.max_samples)
        
        # State metrics (active tasks by state)
        self.downloading_history = deque(maxlen=self.max_samples)
        self.processing_history = deque(maxlen=self.max_samples)
        self.waiting_history = deque(maxlen=self.max_samples)
        
        # Setup matplotlib figure with 2 vertically stacked charts
        self.figure = Figure(figsize=(14, 8), facecolor='#2b2b2b')
        self.canvas = FigureCanvas(self.figure)
        
        # Create 2 subplots (vertical stack)
        self.ax_progress = self.figure.add_subplot(2, 1, 1)
        self.ax_performance = self.figure.add_subplot(2, 1, 2)
        
        # Style both axes
        for ax in [self.ax_progress, self.ax_performance]:
            ax.set_facecolor('#1e1e1e')
            ax.tick_params(colors='white', which='both')
            ax.spines['bottom'].set_color('white')
            ax.spines['top'].set_color('white')
            ax.spines['left'].set_color('white')
            ax.spines['right'].set_color('white')
            ax.grid(True, alpha=0.3, color='gray')
        
        # ============================================================
        # TOP CHART: Pipeline Progress (5 lines)
        # ============================================================
        self.ax_progress.set_xlabel('Time (samples)', color='white', fontsize=9)
        self.ax_progress.set_ylabel('File Count', color='white', fontsize=9)
        self.ax_progress.set_title(
            'Pipeline Progress (Downloaded/Processed/Skipped/Failed/Retrying)',
            color='white', fontweight='bold', fontsize=12
        )
        
        self.line_downloaded, = self.ax_progress.plot(
            [], [], '#2196F3', linewidth=2.5, label='Downloaded', marker='o', markersize=3
        )
        self.line_processed, = self.ax_progress.plot(
            [], [], '#4CAF50', linewidth=2.5, label='Processed', marker='s', markersize=3
        )
        self.line_skipped, = self.ax_progress.plot(
            [], [], '#FF9800', linewidth=2.5, label='Skipped', marker='^', markersize=3
        )
        self.line_failed, = self.ax_progress.plot(
            [], [], '#f44336', linewidth=2.5, label='Failed', marker='x', markersize=4
        )
        self.line_retrying, = self.ax_progress.plot(
            [], [], '#9C27B0', linewidth=2.5, label='Retrying', marker='d', markersize=3
        )
        
        self.ax_progress.legend(
            loc='upper left', facecolor='#2b2b2b', edgecolor='white',
            labelcolor='white', framealpha=0.9, fontsize=9, ncol=5
        )
        
        # ============================================================
        # BOTTOM CHART: Performance Metrics (6 lines, dual y-axis)
        # ============================================================
        self.ax_performance.set_xlabel('Time (samples)', color='white', fontsize=9)
        self.ax_performance.set_ylabel('Active Tasks / Queue Size', color='white', fontsize=9)
        self.ax_performance.set_title(
            'Performance Metrics (State/Queue/Latency)',
            color='white', fontweight='bold', fontsize=12
        )
        
        # Left y-axis: State metrics + Queue size
        self.line_downloading, = self.ax_performance.plot(
            [], [], '#2196F3', linewidth=2.5, label='Downloading', marker='o', markersize=3
        )
        self.line_processing, = self.ax_performance.plot(
            [], [], '#4CAF50', linewidth=2.5, label='Processing', marker='s', markersize=3
        )
        self.line_waiting, = self.ax_performance.plot(
            [], [], '#FF9800', linewidth=2.5, label='Waiting', marker='^', markersize=3
        )
        self.line_pr_qsize, = self.ax_performance.plot(
            [], [], '#E91E63', linewidth=2, label='PR Queue', alpha=0.7
        )
        
        # Right y-axis: Latencies (ms)
        self.ax_performance_latency = self.ax_performance.twinx()
        self.ax_performance_latency.set_facecolor('#1e1e1e')
        self.ax_performance_latency.tick_params(colors='white', which='both')
        self.ax_performance_latency.spines['right'].set_color('white')
        self.ax_performance_latency.set_ylabel('Latency (ms)', color='white', fontsize=9)
        
        self.line_dl_latency, = self.ax_performance_latency.plot(
            [], [], '#00BCD4', linewidth=2, label='DL Latency (ms)', linestyle='--'
        )
        self.line_pr_latency, = self.ax_performance_latency.plot(
            [], [], '#FF5722', linewidth=2, label='PR Latency (ms)', linestyle='--'
        )
        
        # Combined legend for both y-axes
        lines1 = [self.line_downloading, self.line_processing, self.line_waiting, self.line_pr_qsize]
        lines2 = [self.line_dl_latency, self.line_pr_latency]
        labels1 = [l.get_label() for l in lines1]
        labels2 = [l.get_label() for l in lines2]
        self.ax_performance.legend(
            lines1 + lines2, labels1 + labels2, loc='upper left',
            facecolor='#2b2b2b', edgecolor='white', labelcolor='white',
            framealpha=0.9, fontsize=8, ncol=3
        )
        
        # Tight layout
        self.figure.tight_layout(pad=2.0)
        
        # Layout
        layout = QVBoxLayout()
        layout.addWidget(self.canvas)
        layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(layout)
        
        # Initialize sample counter
        self.sample_count = 0
    
    def update_data(
        self,
        downloaded: int = 0,
        processed: int = 0,
        skipped: int = 0,
        failed: int = 0,
        retrying: int = 0,
        downloading: int = 0,
        processing: int = 0,
        waiting: int = 0,
        download_latency_ms: float = 0.0,
        process_latency_ms: float = 0.0,
        process_qsize: int = 0
    ):
        """
        Add new data point and refresh all charts with multiple metrics.
        
        Args:
            downloaded: Total files downloaded
            processed: Total files processed
            skipped: Total files skipped
            failed: Total files failed
            retrying: Total files retrying
            downloading: Active downloading tasks
            processing: Active processing tasks
            waiting: Active waiting tasks
            download_latency_ms: Download latency in milliseconds
            process_latency_ms: Process latency in milliseconds
            process_qsize: Process queue size
        """
        self.sample_count += 1
        self.time_history.append(self.sample_count)
        
        # Update pipeline progress data
        self.downloaded_history.append(downloaded)
        self.processed_history.append(processed)
        self.skipped_history.append(skipped)
        self.failed_history.append(failed)
        self.retrying_history.append(retrying)
        
        # Update performance data - state metrics
        self.downloading_history.append(downloading)
        self.processing_history.append(processing)
        self.waiting_history.append(waiting)
        self.process_qsize_history.append(process_qsize)
        
        # Update latency data
        self.download_latency_history.append(download_latency_ms)
        self.process_latency_history.append(process_latency_ms)
        
        # Update all line charts
        if len(self.time_history) > 0:
            time_list = list(self.time_history)
            
            # Pipeline Progress chart (5 lines)
            self.line_downloaded.set_data(time_list, list(self.downloaded_history))
            self.line_processed.set_data(time_list, list(self.processed_history))
            self.line_skipped.set_data(time_list, list(self.skipped_history))
            self.line_failed.set_data(time_list, list(self.failed_history))
            self.line_retrying.set_data(time_list, list(self.retrying_history))
            
            # Performance chart - Left y-axis (state counts, queue size)
            self.line_downloading.set_data(time_list, list(self.downloading_history))
            self.line_processing.set_data(time_list, list(self.processing_history))
            self.line_waiting.set_data(time_list, list(self.waiting_history))
            self.line_pr_qsize.set_data(time_list, list(self.process_qsize_history))
            
            # Performance chart - Right y-axis (latencies in ms)
            self.line_dl_latency.set_data(time_list, list(self.download_latency_history))
            self.line_pr_latency.set_data(time_list, list(self.process_latency_history))
            
            # Auto-scale axes
            if len(self.time_history) > 10:
                x_min, x_max = self.time_history[0], self.time_history[-1]
                self.ax_progress.set_xlim(x_min, x_max)
                self.ax_performance.set_xlim(x_min, x_max)
            
            # Rescale y-axes
            self.ax_progress.relim()
            self.ax_progress.autoscale_view(True, True, True)
            self.ax_performance.relim()
            self.ax_performance.autoscale_view(True, True, True)
            self.ax_performance_latency.relim()
            self.ax_performance_latency.autoscale_view(True, True, True)
        
        # Redraw canvas
        self.canvas.draw()
