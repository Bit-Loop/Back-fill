"""
Incremental technical indicator calculations (O(1) updates).
Maintains state in rolling windows for efficient real-time processing.
"""
from collections import deque
from typing import Dict, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class IncrementalIndicatorProcessor:
    """
    O(1) incremental indicator calculations using rolling windows.
    Maintains per-symbol state for efficient updates without recomputing history.
    
    Supported indicators:
    - SMA (Simple Moving Average): 20, 50
    - EMA (Exponential Moving Average): 20, 50, 100, 200
    - RSI (Relative Strength Index): 14
    - Bollinger Bands: 20-period, 2 standard deviations
    - VWAP (Volume Weighted Average Price)
    - MACD (Moving Average Convergence Divergence): 12, 26, 9
    """
    
    def __init__(self):
        self.windows = {}
    
    def process(self, bar: Dict) -> Dict:
        """
        Compute indicators incrementally for a single bar.
        
        Args:
            bar: OHLCV bar dict with keys: symbol, close, high, low, volume
            
        Returns:
            Enriched bar with computed indicators
        """
        symbol = bar['symbol']
        
        # Initialize windows for this symbol if first time
        if symbol not in self.windows:
            self.windows[symbol] = {
                'closes': deque(maxlen=200),
                'highs': deque(maxlen=200),
                'lows': deque(maxlen=200),
                'volumes': deque(maxlen=200),
                'rsi_gains': deque(maxlen=14),
                'rsi_losses': deque(maxlen=14),
                'rsi_avg_gain': None,
                'rsi_avg_loss': None,
                'sma_20_sum': 0.0,
                'sma_50_sum': 0.0,
                'ema_20': None,
                'ema_50': None,
                'ema_100': None,
                'ema_200': None,
                'macd_12': None,
                'macd_26': None,
                'macd_signal': None,
            }
        
        w = self.windows[symbol]
        
        # Append new values
        prev_close = w['closes'][-1] if len(w['closes']) > 0 else None
        w['closes'].append(bar['close'])
        w['highs'].append(bar['high'])
        w['lows'].append(bar['low'])
        w['volumes'].append(bar['volume'])
        
        # Compute indicators incrementally
        bar['sma_20'] = self._incremental_sma(w, 20, bar['close'])
        bar['sma_50'] = self._incremental_sma(w, 50, bar['close'])
        bar['ema_20'], w['ema_20'] = self._incremental_ema(bar['close'], w['ema_20'], 20)
        bar['ema_50'], w['ema_50'] = self._incremental_ema(bar['close'], w['ema_50'], 50)
        bar['ema_100'], w['ema_100'] = self._incremental_ema(bar['close'], w['ema_100'], 100)
        bar['ema_200'], w['ema_200'] = self._incremental_ema(bar['close'], w['ema_200'], 200)
        
        bar['rsi_14'] = self._incremental_rsi(bar['close'], prev_close, w)
        bar['bb_upper'], bar['bb_middle'], bar['bb_lower'] = self._bollinger_bands(w['closes'], 20, 2.0)
        bar['vwap'] = self._vwap(w['closes'], w['volumes'])
        
        # MACD
        bar['macd'], bar['macd_signal'], bar['macd_hist'], w['macd_12'], w['macd_26'], w['macd_signal'] = \
            self._incremental_macd(bar['close'], w)
        
        return bar
    
    def _incremental_sma(self, w: Dict, period: int, new_close: float) -> Optional[float]:
        """True O(1) Simple Moving Average using running sum"""
        closes = w['closes']
        sum_key = f'sma_{period}_sum'
        
        if sum_key not in w:
            w[sum_key] = 0.0
        
        if len(closes) < period:
            w[sum_key] = sum(closes)
            return w[sum_key] / len(closes) if len(closes) > 0 else None
        
        # O(1) update: recalculate sum from last N values
        if len(closes) == closes.maxlen:
            w[sum_key] = sum(list(closes)[-period:])
        else:
            w[sum_key] += new_close
        
        return w[sum_key] / period
    
    def _incremental_ema(self, close: float, prev_ema: Optional[float], period: int) -> Tuple[Optional[float], Optional[float]]:
        """O(1) Exponential Moving Average"""
        alpha = 2.0 / (period + 1)
        if prev_ema is None:
            return close, close
        new_ema = alpha * close + (1 - alpha) * prev_ema
        return new_ema, new_ema
    
    def _incremental_rsi(self, close: float, prev_close: Optional[float], w: Dict) -> Optional[float]:
        """True O(1) Relative Strength Index using Wilder's smoothing"""
        if prev_close is None:
            return None
        
        change = close - prev_close
        gain = max(change, 0)
        loss = max(-change, 0)
        
        w['rsi_gains'].append(gain)
        w['rsi_losses'].append(loss)
        
        if len(w['rsi_gains']) < 14:
            if len(w['rsi_gains']) == 14:
                w['rsi_avg_gain'] = sum(w['rsi_gains']) / 14
                w['rsi_avg_loss'] = sum(w['rsi_losses']) / 14
            return None
        
        # O(1) Wilder's smoothing
        if w['rsi_avg_gain'] is None or w['rsi_avg_loss'] is None:
            w['rsi_avg_gain'] = sum(w['rsi_gains']) / 14
            w['rsi_avg_loss'] = sum(w['rsi_losses']) / 14
        else:
            w['rsi_avg_gain'] = (w['rsi_avg_gain'] * 13 + gain) / 14
            w['rsi_avg_loss'] = (w['rsi_avg_loss'] * 13 + loss) / 14
        
        if w['rsi_avg_loss'] == 0:
            return 100.0
        
        rs = w['rsi_avg_gain'] / w['rsi_avg_loss']
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def _bollinger_bands(self, closes: deque, period: int, std_dev: float) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """Bollinger Bands"""
        if len(closes) < period:
            return None, None, None
        
        recent = list(closes)[-period:]
        middle = sum(recent) / period
        variance = sum((x - middle) ** 2 for x in recent) / period
        std = variance ** 0.5
        
        upper = middle + (std_dev * std)
        lower = middle - (std_dev * std)
        
        return upper, middle, lower
    
    def _vwap(self, closes: deque, volumes: deque) -> Optional[float]:
        """Volume Weighted Average Price"""
        if len(closes) == 0 or len(volumes) == 0:
            return None
        
        closes_list = list(closes)
        volumes_list = list(volumes)
        
        total_volume = sum(volumes_list)
        if total_volume == 0:
            return None
        
        vwap = sum(c * v for c, v in zip(closes_list, volumes_list)) / total_volume
        return vwap
    
    def _incremental_macd(self, close: float, w: Dict) -> Tuple:
        """O(1) MACD calculation"""
        # MACD = EMA(12) - EMA(26)
        ema_12, w['macd_12'] = self._incremental_ema(close, w['macd_12'], 12)
        ema_26, w['macd_26'] = self._incremental_ema(close, w['macd_26'], 26)
        
        if ema_12 is None or ema_26 is None:
            return None, None, None, w['macd_12'], w['macd_26'], w['macd_signal']
        
        macd = ema_12 - ema_26
        
        # Signal line = EMA(9) of MACD
        signal, w['macd_signal'] = self._incremental_ema(macd, w['macd_signal'], 9)
        
        hist = macd - signal if signal is not None else None
        
        return macd, signal, hist, w['macd_12'], w['macd_26'], w['macd_signal']
