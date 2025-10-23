"""Structured logging for ChronoX ingestion."""
import json
import logging
import sys
from datetime import datetime
from typing import Any, Dict, Optional


class StructuredLogger:
    """
    JSON-structured logger for production ingestion.
    
    Outputs log lines in JSON format for easy parsing:
    {
        "time": "2025-01-13T14:00:00.000Z",
        "level": "INFO",
        "message": "symbol_execution_started",
        "symbol": "AAPL",
        "phase": "snapshot",
        "records": 1,
        "kafka": true,
        "redis": false
    }
    """
    
    def __init__(self, name: str, level: int = logging.INFO):
        """Initialize structured logger."""
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        handler.setFormatter(logging.Formatter('%(message)s'))
        
        self.logger.addHandler(handler)
        self.logger.propagate = False
    
    def _log(self, level: str, message: str, **kwargs: Any) -> None:
        """Log structured message."""
        log_entry = {
            'time': datetime.utcnow().isoformat() + 'Z',
            'level': level,
            'message': message
        }
        
        log_entry.update(kwargs)
        
        json_str = json.dumps(log_entry)
        
        if level == 'ERROR':
            self.logger.error(json_str)
        elif level == 'WARN':
            self.logger.warning(json_str)
        elif level == 'DEBUG':
            self.logger.debug(json_str)
        else:
            self.logger.info(json_str)
    
    def info(self, message: str, **kwargs: Any) -> None:
        """Log info message."""
        self._log('INFO', message, **kwargs)
    
    def error(self, message: str, **kwargs: Any) -> None:
        """Log error message."""
        self._log('ERROR', message, **kwargs)
    
    def warn(self, message: str, **kwargs: Any) -> None:
        """Log warning message."""
        self._log('WARN', message, **kwargs)
    
    def debug(self, message: str, **kwargs: Any) -> None:
        """Log debug message."""
        self._log('DEBUG', message, **kwargs)
    
    def bind(self, **kwargs: Any) -> 'StructuredLogger':
        """Return a bound logger with context."""
        return BoundLogger(self, kwargs)
    
    def set_level(self, level: int) -> None:
        """Set log level."""
        self.logger.setLevel(level)
        for handler in self.logger.handlers:
            handler.setLevel(level)


class BoundLogger:
    """Logger with bound context variables."""
    
    def __init__(self, logger: StructuredLogger, context: Dict[str, Any]):
        """Initialize bound logger."""
        self.logger = logger
        self.context = context
    
    def info(self, message: str, **kwargs: Any) -> None:
        """Log info with context."""
        merged = {**self.context, **kwargs}
        self.logger.info(message, **merged)
    
    def error(self, message: str, **kwargs: Any) -> None:
        """Log error with context."""
        merged = {**self.context, **kwargs}
        self.logger.error(message, **merged)
    
    def warn(self, message: str, **kwargs: Any) -> None:
        """Log warning with context."""
        merged = {**self.context, **kwargs}
        self.logger.warn(message, **merged)
    
    def debug(self, message: str, **kwargs: Any) -> None:
        """Log debug with context."""
        merged = {**self.context, **kwargs}
        self.logger.debug(message, **merged)


def get_logger(name: str, level: int = logging.INFO) -> StructuredLogger:
    """
    Get or create structured logger.
    
    Args:
        name: Logger name
        level: Log level
    
    Returns:
        StructuredLogger instance
    """
    return StructuredLogger(name, level)
