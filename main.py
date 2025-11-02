import os
import time
import logging
import signal
import sys
from typing import List, Optional, Dict, Any, Tuple
from functools import wraps
import asyncio

import requests
import numpy as np
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    ContextTypes, filters
)
from telegram.error import TimedOut, NetworkError, RetryAfter

# -----------------------
# Logging Configuration for Railway
# -----------------------
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger("mudrex_mi_bot")

# -----------------------
# Environment Variables (Railway)
# -----------------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
PORT = int(os.getenv("PORT", 8080))

# -----------------------
# Telegram Timeout Configuration
# -----------------------
TELEGRAM_CONNECT_TIMEOUT = 30
TELEGRAM_READ_TIMEOUT = 30
TELEGRAM_WRITE_TIMEOUT = 30
TELEGRAM_POOL_TIMEOUT = 30

# -----------------------
# Exchange Configuration - PRIORITY ORDER: Binance → Bybit → KuCoin
# -----------------------
BINANCE_HOSTS = [
    "https://api.binance.com",
    "https://api-gcp.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
    "https://api4.binance.com",
]

BYBIT_HOSTS = [
    "https://api.bybit.com",
    "https://api.bytick.com"
]

KUCOIN_HOSTS = [
    "https://api.kucoin.com"
]

# CoinGlass API Configuration
COINGLASS_API_BASE = "https://open-api.coinglass.com/public/v2"
COINGLASS_TIMEOUT = 5

HTTP_TIMEOUT = 5
EXCHANGEINFO_TTL = 30 * 60

_exchangeinfo_cache: Dict[str, Any] = {
    "binance": {"expires": 0, "data": None},
    "bybit": {"expires": 0, "data": None},
    "kucoin": {"expires": 0, "data": None}
}
PREFERRED_QUOTES = ["USDT", "FDUSD", "USDC"]

# Interval mappings
VALID_INTERVALS = {
    "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w"
}

INTERVAL_TO_BYBIT = {
    "1m": "1", "3m": "3", "5m": "5", "15m": "15", "30m": "30",
    "1h": "60", "2h": "120", "4h": "240", "6h": "360", "8h": "480", "12h": "720",
    "1d": "D", "3d": "D", "1w": "W"
}

INTERVAL_TO_KUCOIN = {
    "1m": "1min", "3m": "3min", "5m": "5min", "15m": "15min", "30m": "30min",
    "1h": "1hour", "2h": "2hour", "4h": "4hour", "6h": "6hour", "8h": "6hour", "12h": "12hour",
    "1d": "1day", "3d": "1day", "1w": "1week"
}

DEFAULT_INTERVAL = "1h"

# Global application instance
application = None

# -----------------------
# Retry Decorator for Network Resilience
# -----------------------
def retry_on_telegram_error(max_retries: int = 3, delay: int = 5):
    """Decorator to retry Telegram operations on network errors"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except (TimedOut, NetworkError) as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        log.warning(f"Telegram error on attempt {attempt + 1}/{max_retries}: {e}. Retrying in {delay}s...")
                        await asyncio.sleep(delay)
                    else:
                        log.error(f"All {max_retries} attempts failed for {func.__name__}: {e}")
                except RetryAfter as e:
                    log.warning(f"Rate limited. Waiting {e.retry_after}s...")
                    await asyncio.sleep(e.retry_after)
                    return await func(*args, **kwargs)
                except Exception as e:
                    log.error(f"Unexpected error in {func.__name__}: {e}")
                    raise
            raise last_exception
        return wrapper
    return decorator

# -----------------------
# HTTP Helper with Failover
# -----------------------
def _http_get_binance(path: str, params: dict) -> Optional[requests.Response]:
    """HTTP GET for Binance with multi-host failover and retry logic"""
    last_exc = None
    for host in BINANCE_HOSTS:
        url = f"{host}{path}"
        for attempt in range(2):
            try:
                r = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
                if 400 <= r.status_code < 500 and r.status_code != 429:
                    return r
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(0.5 * (attempt + 1))
                    continue
                if r.status_code == 200:
                    return r
            except requests.RequestException as e:
                last_exc = e
                time.sleep(0.3)
    log.error(f"Binance HTTP GET FAILED: {path} | Last error: {last_exc}")
    return None

def _http_get_bybit(path: str, params: dict) -> Optional[requests.Response]:
    """HTTP GET for Bybit with retry logic"""
    last_exc = None
    for host in BYBIT_HOSTS:
        url = f"{host}{path}"
        for attempt in range(2):
            try:
                r = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
                if 400 <= r.status_code < 500 and r.status_code != 429:
                    return r
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(0.5 * (attempt + 1))
                    continue
                if r.status_code == 200:
                    return r
            except requests.RequestException as e:
                last_exc = e
                time.sleep(0.3)
    log.error(f"Bybit HTTP GET FAILED: {path} | Last error: {last_exc}")
    return None

def _http_get_kucoin(path: str, params: dict) -> Optional[requests.Response]:
    """HTTP GET for KuCoin with retry logic"""
    last_exc = None
    for host in KUCOIN_HOSTS:
        url = f"{host}{path}"
        for attempt in range(2):
            try:
                r = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
                if 400 <= r.status_code < 500 and r.status_code != 429:
                    return r
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(0.5 * (attempt + 1))
                    continue
                if r.status_code == 200:
                    return r
            except requests.RequestException as e:
                last_exc = e
                time.sleep(0.3)
    log.error(f"KuCoin HTTP GET FAILED: {path} | Last error: {last_exc}")
    return None

def _http_get_coinglass(endpoint: str, params: dict = None) -> Optional[dict]:
    """HTTP GET for CoinGlass API"""
    try:
        url = f"{COINGLASS_API_BASE}{endpoint}"
        r = requests.get(url, params=params or {}, timeout=COINGLASS_TIMEOUT)
        if r.status_code == 200:
            return r.json()
        return None
    except Exception as e:
        log.warning(f"CoinGlass API error: {e}")
        return None

# -----------------------
# Binance Functions
# -----------------------
def _get_binance_exchangeinfo() -> Optional[dict]:
    """Get Binance exchange info with caching"""
    now = time.time()
    if _exchangeinfo_cache["binance"]["data"] and now < _exchangeinfo_cache["binance"]["expires"]:
        return _exchangeinfo_cache["binance"]["data"]
    r = _http_get_binance("/api/v3/exchangeInfo", params={})
    if not r or r.status_code != 200:
        return None
    data = r.json()
    _exchangeinfo_cache["binance"]["data"] = data
    _exchangeinfo_cache["binance"]["expires"] = now + EXCHANGEINFO_TTL
    return data

def _binance_symbol_exists(symbol: str) -> bool:
    """Check if a trading symbol exists on Binance"""
    info = _get_binance_exchangeinfo()
    if info and "symbols" in info:
        symbols = {s["symbol"] for s in info["symbols"]}
        if symbol in symbols:
            return True
    r = _http_get_binance("/api/v3/exchangeInfo", params={"symbol": symbol})
    return bool(r and r.status_code == 200)

def fetch_binance_ticker(symbol: str) -> Optional[dict]:
    """Fetch 24-hour ticker statistics from Binance"""
    r = _http_get_binance("/api/v3/ticker/24hr", params={"symbol": symbol})
    if not r or r.status_code != 200:
        return None
    return r.json()

def fetch_binance_klines(symbol: str, interval: str, limit: int = 100) -> Optional[List[List]]:
    """Fetch candlestick/kline data from Binance"""
    r = _http_get_binance("/api/v3/klines", params={"symbol": symbol, "interval": interval, "limit": limit})
    if not r or r.status_code != 200:
        return None
    return r.json()

# -----------------------
# Bybit Functions
# -----------------------
def _bybit_symbol_exists(symbol: str) -> bool:
    """Check if a trading symbol exists on Bybit"""
    try:
        r = _http_get_bybit("/v5/market/tickers", params={"category": "spot", "symbol": symbol})
        if r and r.status_code == 200:
            data = r.json()
            return data.get("retCode") == 0 and data.get("result", {}).get("list")
        return False
    except Exception as e:
        log.error(f"Error checking Bybit symbol: {e}")
        return False

def fetch_bybit_ticker(symbol: str) -> Optional[dict]:
    """Fetch 24-hour ticker statistics from Bybit"""
    try:
        r = _http_get_bybit("/v5/market/tickers", params={"category": "spot", "symbol": symbol})
        if not r or r.status_code != 200:
            return None
        data = r.json()
        if data.get("retCode") != 0 or not data.get("result", {}).get("list"):
            return None
        ticker = data["result"]["list"][0]
        return {
            "symbol": symbol,
            "lastPrice": ticker.get("lastPrice"),
            "priceChangePercent": float(ticker.get("price24hPcnt", 0)) * 100,
            "highPrice": ticker.get("highPrice24h"),
            "lowPrice": ticker.get("lowPrice24h"),
            "volume": ticker.get("volume24h")
        }
    except Exception as e:
        log.error(f"Error fetching Bybit ticker: {e}")
        return None

def fetch_bybit_klines(symbol: str, interval: str, limit: int = 100) -> Optional[List[List]]:
    """Fetch candlestick/kline data from Bybit"""
    try:
        bybit_interval = INTERVAL_TO_BYBIT.get(interval, "60")
        r = _http_get_bybit("/v5/market/kline", params={
            "category": "spot",
            "symbol": symbol,
            "interval": bybit_interval,
            "limit": limit
        })
        if not r or r.status_code != 200:
            return None
        data = r.json()
        if data.get("retCode") != 0 or not data.get("result", {}).get("list"):
            return None
        klines = []
        for k in data["result"]["list"]:
            klines.append([k[0], k[1], k[2], k[3], k[4], k[5], k[0]])
        return list(reversed(klines))
    except Exception as e:
        log.error(f"Error fetching Bybit klines: {e}")
        return None

# -----------------------
# KuCoin Functions
# -----------------------
def _kucoin_symbol_exists(symbol: str) -> bool:
    """Check if a trading symbol exists on KuCoin"""
    try:
        kucoin_symbol = f"{symbol[:-4]}-{symbol[-4:]}" if len(symbol) > 4 else symbol
        r = _http_get_kucoin("/api/v1/market/orderbook/level1", params={"symbol": kucoin_symbol})
        if r and r.status_code == 200:
            data = r.json()
            return data.get("code") == "200000" and data.get("data") is not None
        return False
    except Exception as e:
        log.error(f"Error checking KuCoin symbol: {e}")
        return False

def fetch_kucoin_ticker(symbol: str) -> Optional[dict]:
    """Fetch 24-hour ticker statistics from KuCoin"""
    try:
        kucoin_symbol = f"{symbol[:-4]}-{symbol[-4:]}" if len(symbol) > 4 else symbol
        r = _http_get_kucoin("/api/v1/market/stats", params={"symbol": kucoin_symbol})
        if not r or r.status_code != 200:
            return None
        data = r.json()
        if data.get("code") != "200000" or not data.get("data"):
            return None
        ticker = data["data"]
        return {
            "symbol": symbol,
            "lastPrice": ticker.get("last"),
            "priceChangePercent": float(ticker.get("changeRate", 0)) * 100,
            "highPrice": ticker.get("high"),
            "lowPrice": ticker.get("low"),
            "volume": ticker.get("vol")
        }
    except Exception as e:
        log.error(f"Error fetching KuCoin ticker: {e}")
        return None

def fetch_kucoin_klines(symbol: str, interval: str, limit: int = 100) -> Optional[List[List]]:
    """Fetch candlestick/kline data from KuCoin"""
    try:
        kucoin_symbol = f"{symbol[:-4]}-{symbol[-4:]}" if len(symbol) > 4 else symbol
        kucoin_interval = INTERVAL_TO_KUCOIN.get(interval, "1hour")
        end_time = int(time.time())
        start_time = end_time - (limit * 3600)
        r = _http_get_kucoin("/api/v1/market/candles", params={
            "symbol": kucoin_symbol,
            "type": kucoin_interval,
            "startAt": start_time,
            "endAt": end_time
        })
        if not r or r.status_code != 200:
            return None
        data = r.json()
        if data.get("code") != "200000" or not data.get("data"):
            return None
        klines = []
        for k in data["data"]:
            klines.append([k[0], k[1], k[3], k[4], k[2], k[5], k[0]])
        return list(reversed(klines))
    except Exception as e:
        log.error(f"Error fetching KuCoin klines: {e}")
        return None

# -----------------------
# CoinGlass Functions
# -----------------------
def fetch_coinglass_data(symbol: str) -> Dict[str, Any]:
    """Fetch Open Interest, Funding Rate, and Liquidations from CoinGlass"""
    result = {
        "open_interest": None,
        "oi_change": None,
        "funding_rate": None,
        "liquidations": None
    }
    
    try:
        base_symbol = symbol.replace("USDT", "").replace("USDC", "").replace("FDUSD", "")
        
        oi_data = _http_get_coinglass(f"/indicator/open-interest", {"symbol": base_symbol})
        if oi_data and oi_data.get("success") and oi_data.get("data"):
            result["open_interest"] = oi_data["data"].get("openInterest")
            result["oi_change"] = oi_data["data"].get("openInterestChange24h")
        
        fr_data = _http_get_coinglass(f"/indicator/funding-rate", {"symbol": base_symbol})
        if fr_data and fr_data.get("success") and fr_data.get("data"):
            result["funding_rate"] = fr_data["data"].get("fundingRate")
        
        liq_data = _http_get_coinglass(f"/indicator/liquidation", {"symbol": base_symbol})
        if liq_data and liq_data.get("success") and liq_data.get("data"):
            result["liquidations"] = liq_data["data"]
            
    except Exception as e:
        log.warning(f"Error fetching CoinGlass data: {e}")
    
    return result

# -----------------------
# Symbol Resolution - PRIORITY: Binance → Bybit → KuCoin
# -----------------------
def _normalize_command(text: str) -> Tuple[str, Optional[str]]:
    """Parse command text to extract symbol and timeframe"""
    s = text.strip()
    if s.startswith("/"):
        s = s[1:]
    s = s.split("@")[0]
    parts = s.split()
    symbol = parts[0].upper().replace("/", "")
    tf = parts[1].lower() if len(parts) > 1 else None
    return symbol, tf

def _resolve_symbol_or_fallback(symbol: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Resolve symbol with priority: Binance → Bybit → KuCoin
    Returns: (resolved_symbol, exchange, note)
    """
    # Priority 1: Check Binance
    if _binance_symbol_exists(symbol):
        log.info(f"✅ Found {symbol} on Binance (Priority 1)")
        return symbol, "Binance", None
    
    # Priority 2: Check Bybit
    if _bybit_symbol_exists(symbol):
        log.info(f"✅ Found {symbol} on Bybit (Priority 2)")
        return symbol, "Bybit", None
    
    # Priority 3: Check KuCoin
    if _kucoin_symbol_exists(symbol):
        log.info(f"✅ Found {symbol} on KuCoin (Priority 3)")
        return symbol, "KuCoin", None

    # Try quote currency variations with priority order
    for q in sorted(PREFERRED_QUOTES, key=len, reverse=True):
        if symbol.endswith(q):
            base = symbol[:-len(q)]
            if not base:
                break
            for alt in PREFERRED_QUOTES:
                alt_sym = f"{base}{alt}"
                
                # Check Binance first
                if _binance_symbol_exists(alt_sym):
                    log.info(f"✅ Resolved {symbol} to {alt_sym} on Binance")
                    return alt_sym, "Binance", f"Resolved to {alt_sym}"
                
                # Check Bybit second
                if _bybit_symbol_exists(alt_sym):
                    log.info(f"✅ Resolved {symbol} to {alt_sym} on Bybit")
                    return alt_sym, "Bybit", f"Resolved to {alt_sym}"
                
                # Check KuCoin third
                if _kucoin_symbol_exists(alt_sym):
                    log.info(f"✅ Resolved {symbol} to {alt_sym} on KuCoin")
                    return alt_sym, "KuCoin", f"Resolved to {alt_sym}"
            break

    # Try appending quote currencies with priority order
    for q in PREFERRED_QUOTES:
        candidate = f"{symbol}{q}"
        
        # Check Binance first
        if _binance_symbol_exists(candidate):
            log.info(f"✅ Found {candidate} on Binance")
            return candidate, "Binance", None
        
        # Check Bybit second
        if _bybit_symbol_exists(candidate):
            log.info(f"✅ Found {candidate} on Bybit")
            return candidate, "Bybit", None
        
        # Check KuCoin third
        if _kucoin_symbol_exists(candidate):
            log.info(f"✅ Found {candidate} on KuCoin")
            return candidate, "KuCoin", None

    return None, None, None

# -----------------------
# Unified Data Fetching
# -----------------------
def fetch_ticker(symbol: str, exchange: str) -> Optional[dict]:
    """Fetch ticker data from specified exchange"""
    if exchange == "Binance":
        return fetch_binance_ticker(symbol)
    elif exchange == "Bybit":
        return fetch_bybit_ticker(symbol)
    elif exchange == "KuCoin":
        return fetch_kucoin_ticker(symbol)
    return None

def fetch_klines(symbol: str, exchange: str, interval: str, limit: int = 100) -> Optional[List[List]]:
    """Fetch klines data from specified exchange"""
    if exchange == "Binance":
        return fetch_binance_klines(symbol, interval, limit)
    elif exchange == "Bybit":
        return fetch_bybit_klines(symbol, interval, limit)
    elif exchange == "KuCoin":
        return fetch_kucoin_klines(symbol, interval, limit)
    return None

# -----------------------
# ENHANCED Technical Analysis
# -----------------------
class EnhancedTechnicalAnalysis:
    """Enhanced technical analysis with EMA, optimized MACD, improved RSI, Volume, and ADX"""
    
    def __init__(self, klines: List[List]):
        if len(klines) < 50:
            raise ValueError("Need at least 50 candles for accurate analysis")
        
        self.closes = np.array([float(k[4]) for k in klines], dtype=float)
        self.highs = np.array([float(k[2]) for k in klines], dtype=float)
        self.lows = np.array([float(k[3]) for k in klines], dtype=float)
        self.volumes = np.array([float(k[5]) for k in klines], dtype=float)
        self.opens = np.array([float(k[1]) for k in klines], dtype=float)

    def _ema(self, data: np.ndarray, period: int) -> np.ndarray:
        """Calculate Exponential Moving Average"""
        alpha = 2 / (period + 1)
        ema = np.zeros_like(data, dtype=float)
        ema[0] = data[0]
        for i in range(1, len(data)):
            ema[i] = alpha * data[i] + (1 - alpha) * ema[i - 1]
        return ema

    def calculate_rsi_enhanced(self, period: int = 14) -> Tuple[float, str]:
        """Enhanced RSI with Wilder's smoothing and slope analysis"""
        delta = np.diff(self.closes)
        gains = np.where(delta > 0, delta, 0.0)
        losses = np.where(delta < 0, -delta, 0.0)
        
        avg_gain = np.zeros(len(gains))
        avg_loss = np.zeros(len(losses))
        avg_gain[period-1] = np.mean(gains[:period])
        avg_loss[period-1] = np.mean(losses[:period])
        
        for i in range(period, len(gains)):
            avg_gain[i] = (avg_gain[i-1] * (period - 1) + gains[i]) / period
            avg_loss[i] = (avg_loss[i-1] * (period - 1) + losses[i]) / period
        
        rs = avg_gain[-1] / avg_loss[-1] if avg_loss[-1] != 0 else 100
        rsi_value = 100 - (100 / (1 + rs))
        
        if len(self.closes) >= period + 5:
            rsi_prev = 100 - (100 / (1 + (avg_gain[-5] / avg_loss[-5]))) if avg_loss[-5] != 0 else 100
            rsi_slope = "Rising" if rsi_value > rsi_prev else "Falling"
        else:
            rsi_slope = "Neutral"
        
        return round(rsi_value, 2), rsi_slope

    def calculate_macd_optimized(self, fast: int = 8, slow: int = 21, signal: int = 5):
        """Optimized MACD for faster intraday response"""
        macd_line = self._ema(self.closes, fast) - self._ema(self.closes, slow)
        signal_line = self._ema(macd_line, signal)
        histogram = macd_line - signal_line
        
        macd_val = round(macd_line[-1], 6)
        signal_val = round(signal_line[-1], 6)
        hist_val = round(histogram[-1], 6)
        
        if macd_val > signal_val:
            trend = "Bullish"
            strength = "Strong" if hist_val > histogram[-2] else "Weak"
        else:
            trend = "Bearish"
            strength = "Strong" if hist_val < histogram[-2] else "Weak"
        
        return macd_val, signal_val, hist_val, trend, strength

    def calculate_ema_trend(self, fast: int = 9, slow: int = 21) -> Tuple[str, float]:
        """EMA-based trend detection"""
        ema_fast = self._ema(self.closes, fast)
        ema_slow = self._ema(self.closes, slow)
        
        current_fast = ema_fast[-1]
        current_slow = ema_slow[-1]
        
        distance_pct = ((current_fast - current_slow) / current_slow) * 100
        
        if current_fast > current_slow:
            if distance_pct > 2:
                trend = "Strong Uptrend"
            elif distance_pct > 0.5:
                trend = "Uptrend"
            else:
                trend = "Weak Uptrend"
        elif current_fast < current_slow:
            if distance_pct < -2:
                trend = "Strong Downtrend"
            elif distance_pct < -0.5:
                trend = "Downtrend"
            else:
                trend = "Weak Downtrend"
        else:
            trend = "Neutral"
        
        return trend, round(distance_pct, 2)

    def calculate_volume_signal(self) -> Tuple[str, str]:
        """Volume confirmation analysis"""
        avg_volume = np.mean(self.volumes[-20:])
        current_volume = self.volumes[-1]
        volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1
        
        price_change = self.closes[-1] - self.closes[-2]
        
        if volume_ratio > 1.5:
            if price_change > 0:
                signal = "Strong Bullish 🟢"
                description = "Price ↑ + Volume ↑"
            else:
                signal = "Strong Bearish 🔴"
                description = "Price ↓ + Volume ↑"
        elif volume_ratio > 1.0:
            if price_change > 0:
                signal = "Bullish 🟢"
                description = "Price ↑ + Volume ↑"
            else:
                signal = "Bearish 🔴"
                description = "Price ↓ + Volume ↑"
        else:
            signal = "Weak ⚪️"
            description = "Low volume"
        
        return signal, description

    def calculate_adx(self, period: int = 14) -> Tuple[float, str]:
        """ADX for trend strength confirmation"""
        high_diff = np.diff(self.highs)
        low_diff = -np.diff(self.lows)
        
        plus_dm = np.where((high_diff > low_diff) & (high_diff > 0), high_diff, 0)
        minus_dm = np.where((low_diff > high_diff) & (low_diff > 0), low_diff, 0)
        
        tr = np.maximum(
            self.highs[1:] - self.lows[1:],
            np.maximum(
                np.abs(self.highs[1:] - self.closes[:-1]),
                np.abs(self.lows[1:] - self.closes[:-1])
            )
        )
        
        atr = np.mean(tr[-period:])
        
        plus_di = 100 * np.mean(plus_dm[-period:]) / atr if atr > 0 else 0
        minus_di = 100 * np.mean(minus_dm[-period:]) / atr if atr > 0 else 0
        
        dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di) if (plus_di + minus_di) > 0 else 0
        
        adx_value = dx
        
        if adx_value > 25:
            strength = "Strong Trend"
        elif adx_value > 20:
            strength = "Moderate Trend"
        else:
            strength = "Weak/No Trend"
        
        return round(adx_value, 2), strength

    def calculate_enhanced_rating(self) -> Tuple[str, int, Dict[str, Any]]:
        """Enhanced rating system with percentage score"""
        score = 0
        max_score = 100
        details = {}
        
        # 1. Enhanced RSI (20 points)
        rsi, rsi_slope = self.calculate_rsi_enhanced()
        details["rsi"] = rsi
        details["rsi_slope"] = rsi_slope
        
        if 40 <= rsi <= 60:
            score += 20
        elif 30 <= rsi < 40 and rsi_slope == "Rising":
            score += 18
        elif 60 < rsi <= 70 and rsi_slope == "Falling":
            score += 15
        elif rsi < 30:
            score += 12
        elif rsi > 70:
            score += 10
        else:
            score += 16
        
        # 2. Optimized MACD (20 points)
        macd_val, signal_val, hist, macd_trend, macd_strength = self.calculate_macd_optimized()
        details["macd"] = {"value": macd_val, "signal": signal_val, "trend": macd_trend, "strength": macd_strength}
        
        if macd_trend == "Bullish" and macd_strength == "Strong":
            score += 20
        elif macd_trend == "Bullish" and macd_strength == "Weak":
            score += 15
        elif macd_trend == "Bearish" and macd_strength == "Strong":
            score += 8
        elif macd_trend == "Bearish" and macd_strength == "Weak":
            score += 12
        else:
            score += 14
        
        # 3. EMA Trend (20 points)
        ema_trend, ema_distance = self.calculate_ema_trend()
        details["ema_trend"] = ema_trend
        details["ema_distance"] = ema_distance
        
        if "Strong Uptrend" in ema_trend:
            score += 20
        elif "Uptrend" in ema_trend:
            score += 16
        elif "Strong Downtrend" in ema_trend:
            score += 8
        elif "Downtrend" in ema_trend:
            score += 12
        else:
            score += 14
        
        # 4. Volume Signal (20 points)
        volume_signal, volume_desc = self.calculate_volume_signal()
        details["volume_signal"] = volume_signal
        details["volume_desc"] = volume_desc
        
        if "Strong Bullish" in volume_signal:
            score += 20
        elif "Bullish" in volume_signal:
            score += 16
        elif "Strong Bearish" in volume_signal:
            score += 8
        elif "Bearish" in volume_signal:
            score += 12
        else:
            score += 14
        
        # 5. ADX Trend Strength (20 points)
        adx_value, adx_strength = self.calculate_adx()
        details["adx"] = adx_value
        details["adx_strength"] = adx_strength
        
        if "Strong Trend" in adx_strength:
            score += 20
        elif "Moderate Trend" in adx_strength:
            score += 15
        else:
            score += 10
        
        # Determine rating with CORRECTED EMOJIS
        if score >= 85:
            rating = "Strong Buy 🚀"
        elif score >= 70:
            rating = "Buy 🚀"
        elif score >= 55:
            rating = "Moderate Buy 🚀"
        elif score >= 45:
            rating = "Neutral ⚪️"
        elif score >= 30:
            rating = "Moderate Sell 🔻"
        elif score >= 15:
            rating = "Sell 🔻"
        else:
            rating = "Strong Sell 🔻"
        
        # Market sentiment
        if score >= 60:
            sentiment = "Bullish 🚀"
        elif score >= 40:
            sentiment = "Neutral ⚪️"
        else:
            sentiment = "Bearish 🔻"
        
        details["sentiment"] = sentiment
        
        return rating, score, details

# -----------------------
# Formatting
# -----------------------
def fmt_price(v: float) -> str:
    """Format price with appropriate decimal places"""
    if v >= 1000:
        return f"${v:,.0f}"
    if v >= 1:
        return f"${v:,.2f}"
    return f"${v:,.6f}"

def build_update(symbol_in: str, interval_in: Optional[str]) -> str:
    """Build formatted market update message with new format"""
    symbol = symbol_in.upper().replace("/", "")
    interval = (interval_in or DEFAULT_INTERVAL).lower()
    if interval not in VALID_INTERVALS:
        interval = DEFAULT_INTERVAL

    resolved, exchange, note = _resolve_symbol_or_fallback(symbol)

    if not resolved or not exchange:
        return (f"⚠️ Symbol `{symbol}` not found.\n"
                f"Try `/BTCUSDT`, `/ETHUSDT`, `/XAUTUSDT`, or `/SOLUSDT`.")

    t = fetch_ticker(resolved, exchange)
    if not t or "lastPrice" not in t:
        return f"⚠️ Could not fetch 24h stats for `{resolved}` from {exchange}. Try again later."

    kl = fetch_klines(resolved, exchange, interval=interval, limit=100)

    ta_details = None
    rating = "N/A"
    score = 0
    sentiment = "N/A"
    volume_signal = "N/A"
    volume_desc = ""
    
    if kl and len(kl) >= 50:
        try:
            ta = EnhancedTechnicalAnalysis(kl)
            rating, score, ta_details = ta.calculate_enhanced_rating()
            sentiment = ta_details.get("sentiment", "N/A")
            volume_signal = ta_details.get("volume_signal", "N/A")
            volume_desc = ta_details.get("volume_desc", "")
        except Exception as e:
            log.warning("Enhanced TA failed for %s: %s", resolved, e)

    coinglass_data = fetch_coinglass_data(resolved)
    
    oi_line = ""
    funding_line = ""
    liq_line = ""
    
    if coinglass_data.get("oi_change") is not None:
        oi_change = coinglass_data["oi_change"]
        if oi_change > 5:
            oi_line = f"• Open Interest: Rising 🔼 (Bullish bias)\n"
        elif oi_change < -5:
            oi_line = f"• Open Interest: Falling 🔽 (Bearish bias)\n"
        else:
            oi_line = f"• Open Interest: Stable ➡️\n"
    
    if coinglass_data.get("funding_rate") is not None:
        fr = coinglass_data["funding_rate"]
        if fr > 0.01:
            funding_line = f"• Funding Rate: Positive (Longs paying)\n"
        elif fr < -0.01:
            funding_line = f"• Funding Rate: Negative (Shorts paying)\n"
        else:
            funding_line = f"• Funding Rate: Neutral\n"
    
    if coinglass_data.get("liquidations"):
        liq_data = coinglass_data["liquidations"]
        liq_line = f"• Liquidations: Active 💥\n"

    last_price = float(t.get("lastPrice", 0))
    pct = float(t.get("priceChangePercent", 0.0))
    high = float(t.get("highPrice", last_price))
    low = float(t.get("lowPrice", last_price))
    volume = float(t.get("volume", 0))
    arrow = "▲" if pct >= 0 else "▼"
    
    if kl and len(kl) >= 2:
        prev_volume = float(kl[-2][5])
        volume_change_pct = ((volume - prev_volume) / prev_volume * 100) if prev_volume > 0 else 0
        if volume_change_pct > 100:
            volume_change_str = f"▲ 1000%+"
        else:
            volume_change_str = f"{'▲' if volume_change_pct >= 0 else '▼'} {abs(volume_change_pct):.1f}%"
    else:
        volume_change_str = "N/A"

    header = f"🔸 {resolved} — Market Update ([Mudrex](https://mudrex.go.link/f8PJF))"
    if note:
        header += f"\n_{note}_"

    return (
        f"{header}\n\n"
        f"**Signals**\n"
        f"• Market Sentiment: {sentiment}\n"
        f"• Technical Rating: {rating} ({score}%)\n"
        f"• Volume Signal: {volume_signal} ({volume_desc})\n"
        f"{oi_line}"
        f"{funding_line}"
        f"{liq_line}\n"
        f"**Stats**\n"
        f"• Last Price: {fmt_price(last_price)}\n"
        f"• 24h Change: {arrow} {abs(pct):.2f}%\n"
        f"• Day High: {fmt_price(high)}\n"
        f"• Day Low: {fmt_price(low)}\n"
        f"• 24h Volume Change: {volume_change_str}\n"
        f"══════════════════════════\n"
        f"Powered by Mudrex Market Intelligence"
    )

# -----------------------
# Telegram Handlers with Retry Logic
# -----------------------
@retry_on_telegram_error(max_retries=3, delay=5)
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    if update.message:
        await update.message.reply_text(
            "Welcome to Mudrex MI Bot! 🚀\n\n"
            "Get real-time crypto market analysis with enhanced technical indicators.\n\n"
            "Quick Start:\n"
            "• `/BTCUSDT` - Get Bitcoin market update\n"
            "• `/ETHUSDT 15m` - Get Ethereum with 15-minute timeframe\n"
            "• `/XAUTUSDT` - Get XAUT (Gold token)\n"
            "• `/help` - View detailed usage guide\n\n"
            "Try it now! Send any crypto symbol like `/BTC` or `/ETH`"
        )

@retry_on_telegram_error(max_retries=3, delay=5)
async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /help command"""
    if update.message:
        await update.message.reply_text(
            "📖 *Mudrex MI Bot - Usage Guide*\n\n"
            "*Basic Usage:*\n"
            "• `/BTCUSDT` – Get market update (default 1h timeframe)\n"
            "• `/BTCUSDT 15m` – Specify custom timeframe\n"
            "• `/ETH` – Auto-resolves to ETHUSDT\n"
            "• `/XAUTUSDT` – GOLD\n\n"
            "*Supported Timeframes:*\n"
            "`1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`, `3d`, `1w`\n\n"
            "*Enhanced Features:*\n"
            "✅ Real-time price data\n"
            "✅ Enhanced Technical Analysis (EMA, Optimized MACD, Improved RSI)\n"
            "✅ Volume confirmation signals\n"
            "✅ ADX trend strength\n"
            "✅ Open Interest & Funding Rate (CoinGlass)\n"
            "✅ Liquidation tracking\n"
            "✅ Percentage-based rating system\n\n"
            "*Examples:*\n"
            "• `/SOLUSDT` - Solana market update\n"
            "• `/ATOM 4h` - Cosmos with 4-hour chart\n\n"
            "Powered by [Mudrex Market Intelligence](https://mudrex.go.link/f8PJF)",
            parse_mode="Markdown",
            disable_web_page_preview=True
        )

@retry_on_telegram_error(max_retries=3, delay=5)
async def any_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle any symbol command"""
    if not update.message:
        return
    text = update.message.text or ""
    symbol, tf = _normalize_command(text)
    msg = build_update(symbol, tf)
    await update.message.reply_text(msg, parse_mode="Markdown", disable_web_page_preview=True)

async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Handle errors"""
    log.exception("Unhandled error: %s", context.error)

# -----------------------
# Graceful Shutdown for Railway
# -----------------------
def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global application
    log.info(f"🛑 Received signal {signum}. Shutting down gracefully...")
    if application:
        try:
            application.stop()
            log.info("✅ Bot stopped successfully")
        except Exception as e:
            log.error(f"Error during shutdown: {e}")
    sys.exit(0)

# -----------------------
# Main Entry Point for Railway
# -----------------------
def main():
    """Main entry point optimized for Railway deployment"""
    global application
    
    if not TELEGRAM_TOKEN:
        log.error("❌ TELEGRAM_TOKEN not set! Add to Railway environment variables.")
        log.info("💡 For Smartbook testing: This is expected. Set TELEGRAM_TOKEN on Railway.")
        return

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        application = (
            ApplicationBuilder()
            .token(TELEGRAM_TOKEN)
            .connect_timeout(TELEGRAM_CONNECT_TIMEOUT)
            .read_timeout(TELEGRAM_READ_TIMEOUT)
            .write_timeout(TELEGRAM_WRITE_TIMEOUT)
            .pool_timeout(TELEGRAM_POOL_TIMEOUT)
            .build()
        )

        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("help", help_cmd))
        application.add_handler(MessageHandler(filters.COMMAND, any_command))
        application.add_error_handler(on_error)

        log.info("=" * 60)
        log.info("🚂 MUDREX MI BOT - ENHANCED EDITION")
        log.info("=" * 60)
        log.info(f"✅ Environment: Railway")
        log.info(f"✅ Port: {PORT}")
        log.info(f"✅ Exchange Priority: Binance → Bybit → KuCoin")
        log.info(f"✅ CoinGlass: Open Interest, Funding, Liquidations")
        log.info(f"✅ Enhanced TA: EMA, Optimized MACD, Improved RSI, Volume, ADX")
        log.info(f"✅ Timeouts: {TELEGRAM_CONNECT_TIMEOUT}s")
        log.info(f"✅ Retry Logic: 3 attempts with 5s delay")
        log.info(f"✅ Starting polling mode...")
        log.info("=" * 60)
        
        application.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True,
            close_loop=False,
            timeout=20,
            poll_interval=2.0
        )
        
    except KeyboardInterrupt:
        log.info("🛑 Received keyboard interrupt. Shutting down...")
    except Exception as e:
        log.error(f"❌ FATAL ERROR: {e}")
        log.exception("Full traceback:")
    finally:
        if application:
            log.info("🧹 Cleaning up resources...")

if __name__ == "__main__":
    log.info("🚀 Starting Mudrex MI Bot - Enhanced Edition...")
    main()

print("=" * 70)
print("✅ ENHANCED BOT CODE LOADED IN QUANTRA_95!")
print("=" * 70)
print("📦 Mudrex MI Bot - Enhanced Edition v2.0")
print("=" * 70)
print("🎯 EXCHANGE PRIORITY ORDER:")
print("   🥇 Binance (checked first)")
print("   🥈 Bybit (checked if Binance fails)")
print("   🥉 KuCoin (checked if both Binance and Bybit fail)")
print("=" * 70)
print("🎨 CORRECTED EMOJI RATINGS:")
print("   85%+ = Strong Buy 🚀")
print("   70-84% = Buy 🚀")
print("   55-69% = Moderate Buy 🚀")
print("   45-54% = Neutral ⚪️")
print("   30-44% = Moderate Sell 🔻")
print("   15-29% = Sell 🔻")
print("   <15% = Strong Sell 🔻")
print("=" * 70)
print("✅ RAILWAY DEPLOYMENT CHECKLIST:")
print("   ✅ Environment variables: TELEGRAM_TOKEN, PORT")
print("   ✅ Graceful shutdown handlers (SIGTERM, SIGINT)")
print("   ✅ Network resilience (30s timeouts, 3x retry)")
print("   ✅ Polling mode optimized (timeout=20, interval=2.0)")
print("   ✅ Error handling and logging")
print("   ✅ Multi-exchange failover support")
print("=" * 70)
