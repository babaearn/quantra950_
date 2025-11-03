# ============================================================================
# 🚀 MUDREX MI BOT v5.5 FINAL - ULTIMATE PERPETUAL FUTURES EDITION
# ============================================================================
# FIXES IN FINAL VERSION:
# - Increased duration for 1d (120d → 200d) for sufficient data
# - Smart cascade fallback: 1d → 4h → 1h
# - Universal symbol support (ALL perpetual pairs including XAUT)
# - Removed confusing TA warnings
# NEW IN v5.5:
# - Bollinger Bands indicator (volatility & overbought/oversold)
# - 5 comprehensive indicators (20 points each = 100 total)
# FROM v4.8:
# - Mudrex API for complete data coverage
# - Market Stats for OI + Funding Rate
# - 4-tier data reliability
# - Exchange Priority: Bybit Perp → Binance Perp → Spot
# ============================================================================

import os
import time
import logging
import signal
import sys
from typing import List, Optional, Dict, Any, Tuple
from functools import wraps
from datetime import datetime, timedelta
from collections import defaultdict
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
COINGLASS_API_KEY = os.getenv("COINGLASS_API_KEY", "")
PORT = int(os.getenv("PORT", 8080))

# -----------------------
# Telegram Timeout Configuration
# -----------------------
TELEGRAM_CONNECT_TIMEOUT = 30
TELEGRAM_READ_TIMEOUT = 30
TELEGRAM_WRITE_TIMEOUT = 30
TELEGRAM_POOL_TIMEOUT = 30

# -----------------------
# Rate Limiting Configuration
# -----------------------
MAX_REQUESTS_PER_USER = 10
_rate_limiter = defaultdict(list)

# -----------------------
# Exchange Configuration
# -----------------------
BYBIT_HOSTS = [
    "https://api.bybit.com",
    "https://api.bytick.com"
]

BINANCE_HOSTS = [
    "https://fapi.binance.com",
    "https://api.binance.com",
]

COINGLASS_API_BASE = "https://open-api.coinglass.com/public/v2"
COINGLASS_TIMEOUT = 5

MUDREX_API_BASE = "https://price.mudrex.com/api/v1"
MUDREX_TIMEOUT = 10

HTTP_TIMEOUT = 5
EXCHANGEINFO_TTL = 30 * 60

_exchangeinfo_cache: Dict[str, Any] = {
    "bybit_perp": {"expires": 0, "data": None, "symbols": set()},
    "binance_perp": {"expires": 0, "data": None, "symbols": set()},
    "binance_spot": {"expires": 0, "data": None, "symbols": set()},
}

VALID_INTERVALS = {
    "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w"
}

INTERVAL_TO_BYBIT = {
    "1m": "1", "3m": "3", "5m": "5", "15m": "15", "30m": "30",
    "1h": "60", "2h": "120", "4h": "240", "6h": "360", "8h": "480", "12h": "720",
    "1d": "D", "3d": "D", "1w": "W"
}

DEFAULT_INTERVAL = "1h"
application = None

# -----------------------
# Helper Functions
# -----------------------
def safe_float(x, default=0.0) -> float:
    """Safely convert value to float with fallback"""
    try:
        if x is None:
            return default
        return float(x)
    except (ValueError, TypeError):
        return default

def check_rate_limit(user_id: int) -> bool:
    """Check if user exceeded rate limit"""
    now = datetime.now()
    one_minute_ago = now - timedelta(minutes=1)
    _rate_limiter[user_id] = [ts for ts in _rate_limiter[user_id] if ts > one_minute_ago]
    if len(_rate_limiter[user_id]) >= MAX_REQUESTS_PER_USER:
        return False
    _rate_limiter[user_id].append(now)
    return True

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
# HTTP Helpers
# -----------------------
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
                    time.sleep(min(2 ** attempt * 0.5, 5))
                    continue
                if r.status_code == 200:
                    return r
            except requests.RequestException as e:
                last_exc = e
                time.sleep(min(2 ** attempt * 0.3, 3))
    log.error(f"Bybit HTTP GET FAILED: {path} | Last error: {last_exc}")
    return None

def _http_get_binance(path: str, params: dict, use_futures: bool = True) -> Optional[requests.Response]:
    """HTTP GET for Binance with futures/spot selection"""
    last_exc = None
    hosts = [BINANCE_HOSTS[0]] if use_futures else [BINANCE_HOSTS[1]]
    for host in hosts:
        url = f"{host}{path}"
        for attempt in range(2):
            try:
                r = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
                if 400 <= r.status_code < 500 and r.status_code != 429:
                    return r
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(min(2 ** attempt * 0.5, 5))
                    continue
                if r.status_code == 200:
                    return r
            except requests.RequestException as e:
                last_exc = e
                time.sleep(min(2 ** attempt * 0.3, 3))
    log.error(f"Binance HTTP GET FAILED: {path} | Last error: {last_exc}")
    return None

def _http_get_coinglass(endpoint: str, params: dict = None) -> Optional[dict]:
    """HTTP GET for CoinGlass API"""
    try:
        url = f"{COINGLASS_API_BASE}{endpoint}"
        headers = {}
        if COINGLASS_API_KEY:
            headers["CG-API-KEY"] = COINGLASS_API_KEY
        r = requests.get(url, params=params or {}, headers=headers, timeout=COINGLASS_TIMEOUT)
        if r.status_code == 200:
            return r.json()
        else:
            log.warning(f"CoinGlass API returned status {r.status_code}")
            return None
    except Exception as e:
        log.warning(f"CoinGlass API error: {e}")
        return None

def _http_get_mudrex(endpoint: str, params: dict = None) -> Optional[dict]:
    """HTTP GET for Mudrex API"""
    try:
        url = f"{MUDREX_API_BASE}{endpoint}"
        r = requests.get(url, params=params or {}, timeout=MUDREX_TIMEOUT)
        if r.status_code == 200:
            data = r.json()
            if data.get("success"):
                return data
            else:
                log.warning(f"Mudrex API returned success=false")
                return None
        else:
            log.warning(f"Mudrex API returned status {r.status_code}")
            return None
    except Exception as e:
        log.warning(f"Mudrex API error: {e}")
        return None

# -----------------------
# Exchange Info Caching
# -----------------------
def _fetch_and_cache_bybit_symbols() -> set:
    """Fetch and cache Bybit perpetual symbols"""
    try:
        r = _http_get_bybit("/v5/market/instruments-info", params={"category": "linear"})
        if r and r.status_code == 200:
            data = r.json()
            if data.get("retCode") == 0:
                symbols = {item["symbol"] for item in data.get("result", {}).get("list", [])}
                _exchangeinfo_cache["bybit_perp"]["symbols"] = symbols
                _exchangeinfo_cache["bybit_perp"]["expires"] = time.time() + EXCHANGEINFO_TTL
                log.info(f"Cached {len(symbols)} Bybit perpetual symbols")
                return symbols
    except Exception as e:
        log.error(f"Error fetching Bybit symbols: {e}")
    return set()

def _fetch_and_cache_binance_perp_symbols() -> set:
    """Fetch and cache Binance perpetual symbols"""
    try:
        r = _http_get_binance("/fapi/v1/exchangeInfo", params={}, use_futures=True)
        if r and r.status_code == 200:
            data = r.json()
            symbols = {s["symbol"] for s in data.get("symbols", [])}
            _exchangeinfo_cache["binance_perp"]["symbols"] = symbols
            _exchangeinfo_cache["binance_perp"]["expires"] = time.time() + EXCHANGEINFO_TTL
            log.info(f"Cached {len(symbols)} Binance perpetual symbols")
            return symbols
    except Exception as e:
        log.error(f"Error fetching Binance perp symbols: {e}")
    return set()

def _fetch_and_cache_binance_spot_symbols() -> set:
    """Fetch and cache Binance spot symbols"""
    try:
        r = _http_get_binance("/api/v3/exchangeInfo", params={}, use_futures=False)
        if r and r.status_code == 200:
            data = r.json()
            symbols = {s["symbol"] for s in data.get("symbols", [])}
            _exchangeinfo_cache["binance_spot"]["symbols"] = symbols
            _exchangeinfo_cache["binance_spot"]["expires"] = time.time() + EXCHANGEINFO_TTL
            log.info(f"Cached {len(symbols)} Binance spot symbols")
            return symbols
    except Exception as e:
        log.error(f"Error fetching Binance spot symbols: {e}")
    return set()

# -----------------------
# Bybit Functions
# -----------------------
def _bybit_perp_symbol_exists(symbol: str) -> bool:
    """Check if symbol exists on Bybit"""
    now = time.time()
    if _exchangeinfo_cache["bybit_perp"]["expires"] > now:
        return symbol in _exchangeinfo_cache["bybit_perp"]["symbols"]
    symbols = _fetch_and_cache_bybit_symbols()
    return symbol in symbols

def fetch_bybit_perp_ticker(symbol: str) -> Optional[dict]:
    """Fetch ticker from Bybit Perpetual"""
    try:
        r = _http_get_bybit("/v5/market/tickers", params={"category": "linear", "symbol": symbol})
        if not r or r.status_code != 200:
            return None
        data = r.json()
        if data.get("retCode") != 0 or not data.get("result", {}).get("list"):
            return None
        ticker = data["result"]["list"][0]
        return {
            "symbol": symbol,
            "lastPrice": ticker.get("lastPrice"),
            "priceChangePercent": safe_float(ticker.get("price24hPcnt", 0)) * 100,
            "highPrice": ticker.get("highPrice24h"),
            "lowPrice": ticker.get("lowPrice24h"),
            "volume": ticker.get("volume24h"),
            "openInterest": ticker.get("openInterest"),
            "fundingRate": ticker.get("fundingRate")
        }
    except Exception as e:
        log.error(f"Error fetching Bybit ticker: {e}")
        return None

def fetch_bybit_perp_klines(symbol: str, interval: str, limit: int = 100) -> Optional[List[List]]:
    """Fetch klines from Bybit Perpetual"""
    try:
        bybit_interval = INTERVAL_TO_BYBIT.get(interval, "60")
        r = _http_get_bybit("/v5/market/kline", params={
            "category": "linear",
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
            if len(k) >= 6:
                klines.append([k[0], k[1], k[2], k[3], k[4], k[5], k[0]])
        return list(reversed(klines))
    except Exception as e:
        log.error(f"Error fetching Bybit klines: {e}")
        return None

# -----------------------
# Binance Perpetual Functions
# -----------------------
def _binance_perp_symbol_exists(symbol: str) -> bool:
    """Check if symbol exists on Binance Futures"""
    now = time.time()
    if _exchangeinfo_cache["binance_perp"]["expires"] > now:
        return symbol in _exchangeinfo_cache["binance_perp"]["symbols"]
    symbols = _fetch_and_cache_binance_perp_symbols()
    return symbol in symbols

def fetch_binance_perp_ticker(symbol: str) -> Optional[dict]:
    """Fetch ticker from Binance Perpetual"""
    try:
        r = _http_get_binance("/fapi/v1/ticker/24hr", params={"symbol": symbol}, use_futures=True)
        if not r or r.status_code != 200:
            return None
        data = r.json()
        return {
            "symbol": symbol,
            "lastPrice": data.get("lastPrice"),
            "priceChangePercent": safe_float(data.get("priceChangePercent", 0)),
            "highPrice": data.get("highPrice"),
            "lowPrice": data.get("lowPrice"),
            "volume": data.get("volume")
        }
    except Exception as e:
        log.error(f"Error fetching Binance perp ticker: {e}")
        return None

def fetch_binance_perp_klines(symbol: str, interval: str, limit: int = 100) -> Optional[List[List]]:
    """Fetch klines from Binance Perpetual"""
    try:
        r = _http_get_binance("/fapi/v1/klines", params={
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }, use_futures=True)
        if not r or r.status_code != 200:
            return None
        return r.json()
    except Exception as e:
        log.error(f"Error fetching Binance perp klines: {e}")
        return None

def fetch_binance_perp_open_interest(symbol: str) -> Optional[float]:
    """Fetch OI from Binance Perpetual"""
    try:
        r = _http_get_binance("/fapi/v1/openInterest", params={"symbol": symbol}, use_futures=True)
        if not r or r.status_code != 200:
            return None
        data = r.json()
        return safe_float(data.get("openInterest", 0))
    except Exception as e:
        log.error(f"Error fetching Binance OI: {e}")
        return None

def fetch_binance_perp_funding_rate(symbol: str) -> Optional[float]:
    """Fetch funding rate from Binance Perpetual"""
    try:
        r = _http_get_binance("/fapi/v1/premiumIndex", params={"symbol": symbol}, use_futures=True)
        if not r or r.status_code != 200:
            return None
        data = r.json()
        return safe_float(data.get("lastFundingRate", 0))
    except Exception as e:
        log.error(f"Error fetching Binance funding rate: {e}")
        return None

def fetch_aggregated_oi_manual(symbol: str, current_price: float) -> Optional[float]:
    """Manually aggregate OI from multiple exchanges"""
    try:
        total_oi_usd = 0.0
        binance_oi = fetch_binance_perp_open_interest(symbol)
        if binance_oi and binance_oi > 0 and current_price > 0:
            binance_oi_usd = binance_oi * current_price
            total_oi_usd += binance_oi_usd
            log.info(f"Binance Perp OI: {binance_oi:.2f} = ${binance_oi_usd:,.0f}")
        bybit_ticker = fetch_bybit_perp_ticker(symbol)
        if bybit_ticker and bybit_ticker.get("openInterest"):
            bybit_oi = safe_float(bybit_ticker["openInterest"])
            if bybit_oi > 0 and current_price > 0:
                bybit_oi_usd = bybit_oi * current_price
                total_oi_usd += bybit_oi_usd
                log.info(f"Bybit Perp OI: {bybit_oi:.2f} = ${bybit_oi_usd:,.0f}")
        if total_oi_usd > 0:
            log.info(f"✅ Manual aggregated OI for {symbol}: ${total_oi_usd:,.0f}")
            return total_oi_usd
        return None
    except Exception as e:
        log.error(f"Error in manual OI aggregation: {e}")
        return None

# -----------------------
# Mudrex API Functions (FIXED)
# -----------------------
def fetch_mudrex_klines(symbol: str, interval: str, limit: int = 100) -> Optional[List[List]]:
    """Fetch klines from Mudrex API - FIXED with increased durations"""
    try:
        base_currency = symbol.replace("USDT", "").replace("USDC", "").replace("FDUSD", "")
        quote_currency = "USDT"
        aggregation_map = {
            "1m": "1m", "3m": "3m", "5m": "5m", "15m": "15m", "30m": "30m",
            "1h": "1h", "2h": "2h", "4h": "4h", "6h": "6h", "8h": "8h", "12h": "12h",
            "1d": "1d", "3d": "3d", "1w": "1w"
        }
        aggregation = aggregation_map.get(interval, "1h")
        duration_map = {
            "1m": "2d", "3m": "1w", "5m": "1w", "15m": "2w", "30m": "1M",
            "1h": "5d", "2h": "10d", "4h": "20d", "6h": "30d", "8h": "40d", "12h": "60d",
            "1d": "200d", "3d": "2y", "1w": "5y"  # Increased for better data
        }
        duration = duration_map.get(interval, "5d")
        log.info(f"Fetching Mudrex klines for {base_currency}/{quote_currency} {interval} (duration: {duration})")
        endpoint = f"/asset/{base_currency}/{quote_currency}/klines"
        params = {"duration": duration, "aggregation": aggregation, "type": "LINEAR"}
        data = _http_get_mudrex(endpoint, params)
        if not data or "data" not in data:
            log.warning(f"Mudrex klines unavailable for {symbol}")
            return None
        klines_data = data["data"]
        if not klines_data or len(klines_data) == 0:
            log.warning(f"Mudrex returned empty klines for {symbol}")
            return None
        klines = []
        for k in klines_data[-limit:]:
            klines.append([
                k.get("timestamp", 0),
                str(k.get("open", 0)),
                str(k.get("high", 0)),
                str(k.get("low", 0)),
                str(k.get("close", 0)),
                str(k.get("volume", 0)),
                k.get("timestamp", 0)
            ])
        log.info(f"✅ Got {len(klines)} Mudrex klines for {symbol} at {interval}")
        return klines
    except Exception as e:
        log.error(f"Error fetching Mudrex klines: {e}")
        return None

def fetch_mudrex_market_stats(symbol: str) -> Optional[dict]:
    """Fetch market stats from Mudrex"""
    try:
        log.info(f"Fetching Mudrex market stats for {symbol}")
        endpoint = "/market/stats"
        params = {"symbols": symbol}
        data = _http_get_mudrex(endpoint, params)
        if not data or "data" not in data:
            log.warning(f"Mudrex market stats unavailable for {symbol}")
            return None
        stats = data["data"].get(symbol, {})
        if not stats:
            log.warning(f"No stats data for {symbol}")
            return None
        result = {
            "openInterest": stats.get("openInterest"),
            "fundingRate": stats.get("fundingRate"),
            "volume24h": stats.get("volume24h"),
            "priceChange24h": stats.get("priceChangePercent24h"),
            "high24h": stats.get("high24h"),
            "low24h": stats.get("low24h")
        }
        log.info(f"✅ Got Mudrex stats for {symbol}")
        return result
    except Exception as e:
        log.error(f"Error fetching Mudrex market stats: {e}")
        return None

# -----------------------
# Binance Spot Functions
# -----------------------
def _binance_spot_symbol_exists(symbol: str) -> bool:
    """Check if symbol exists on Binance Spot"""
    now = time.time()
    if _exchangeinfo_cache["binance_spot"]["expires"] > now:
        return symbol in _exchangeinfo_cache["binance_spot"]["symbols"]
    symbols = _fetch_and_cache_binance_spot_symbols()
    return symbol in symbols

def fetch_binance_spot_ticker(symbol: str) -> Optional[dict]:
    """Fetch ticker from Binance Spot"""
    try:
        r = _http_get_binance("/api/v3/ticker/24hr", params={"symbol": symbol}, use_futures=False)
        if not r or r.status_code != 200:
            return None
        return r.json()
    except Exception as e:
        log.error(f"Error fetching Binance spot ticker: {e}")
        return None

def fetch_binance_spot_klines(symbol: str, interval: str, limit: int = 100) -> Optional[List[List]]:
    """Fetch klines from Binance Spot"""
    try:
        r = _http_get_binance("/api/v3/klines", params={
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }, use_futures=False)
        if not r or r.status_code != 200:
            return None
        return r.json()
    except Exception as e:
        log.error(f"Error fetching Binance spot klines: {e}")
        return None

# -----------------------
# CoinGlass Functions
# -----------------------
def fetch_coinglass_data(symbol: str) -> Dict[str, Any]:
    """Fetch OI and funding rate from CoinGlass"""
    result = {
        "open_interest": None,
        "funding_rate": None,
    }
    try:
        base_symbol = symbol.replace("USDT", "").replace("USDC", "").replace("FDUSD", "")
        log.info(f"Fetching CoinGlass data for {base_symbol}...")
        oi_data = _http_get_coinglass("/indicator/open-interest", {"symbol": base_symbol})
        if oi_data and oi_data.get("success") and oi_data.get("data"):
            oi_value = oi_data["data"].get("openInterest") or oi_data["data"].get("usdValue")
            if oi_value:
                result["open_interest"] = safe_float(oi_value)
                log.info(f"✅ CoinGlass OI for {base_symbol}: ${result['open_interest']:,.0f}")
    except Exception as e:
        log.error(f"Error fetching CoinGlass data: {e}")
    return result

# -----------------------
# Symbol Resolution
# -----------------------
def _normalize_command(text: str) -> Tuple[str, Optional[str]]:
    """Parse command text"""
    s = text.strip()
    if s.startswith("/"):
        s = s[1:]
    s = s.split("@")[0]
    parts = s.split()
    if not parts or not parts[0]:
        return "", None
    symbol = parts[0].upper().replace("/", "")
    tf = parts[1].lower() if len(parts) > 1 else None
    return symbol, tf

def _resolve_symbol_or_fallback(symbol: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Resolve symbol with priority: Bybit Perp → Binance Perp → Binance Spot"""
    if not symbol:
        return None, None, None
    
    base_symbol = symbol.replace("USDT", "").replace("USDC", "").replace("FDUSD", "").replace("BUSD", "")
    
    # Priority 1: Bybit Perpetual with USDT
    bybit_perp_symbol = f"{base_symbol}USDT"
    if _bybit_perp_symbol_exists(bybit_perp_symbol):
        log.info(f"✅ Found {bybit_perp_symbol} on Bybit Perpetual")
        return bybit_perp_symbol, "Bybit Perpetual", None
    
    # Priority 2: Binance Perpetual with USDT
    binance_perp_symbol = f"{base_symbol}USDT"
    if _binance_perp_symbol_exists(binance_perp_symbol):
        log.info(f"✅ Found {binance_perp_symbol} on Binance Perpetual")
        return binance_perp_symbol, "Binance Perpetual", None
    
    # Priority 3: Binance Spot with USDT (for coins like XAUT)
    binance_spot_symbol = f"{base_symbol}USDT"
    if _binance_spot_symbol_exists(binance_spot_symbol):
        log.info(f"✅ Found {binance_spot_symbol} on Binance Spot")
        return binance_spot_symbol, "Binance Spot", None  # Don't show "Using Spot" - just work silently
    
    # Try alternative quote currencies (USDC, FDUSD, BUSD)
    for quote in ["USDC", "FDUSD", "BUSD"]:
        alt_symbol = f"{base_symbol}{quote}"
        
        if _bybit_perp_symbol_exists(alt_symbol):
            log.info(f"✅ Resolved {symbol} to {alt_symbol} on Bybit Perpetual")
            return alt_symbol, "Bybit Perpetual", None
        
        if _binance_perp_symbol_exists(alt_symbol):
            log.info(f"✅ Resolved {symbol} to {alt_symbol} on Binance Perpetual")
            return alt_symbol, "Binance Perpetual", None
        
        if _binance_spot_symbol_exists(alt_symbol):
            log.info(f"✅ Resolved {symbol} to {alt_symbol} on Binance Spot")
            return alt_symbol, "Binance Spot", None
    
    log.warning(f"❌ Symbol {symbol} not found on any exchange")
    return None, None, None

# -----------------------
# Unified Data Fetching
# -----------------------
def fetch_ticker(symbol: str, exchange: str) -> Optional[dict]:
    """Fetch ticker data"""
    if exchange == "Bybit Perpetual":
        return fetch_bybit_perp_ticker(symbol)
    elif exchange == "Binance Perpetual":
        return fetch_binance_perp_ticker(symbol)
    elif exchange == "Binance Spot":
        return fetch_binance_spot_ticker(symbol)
    return None

def fetch_klines(symbol: str, exchange: str, interval: str, limit: int = 100) -> Optional[List[List]]:
    """Fetch klines data"""
    if exchange == "Bybit Perpetual":
        return fetch_bybit_perp_klines(symbol, interval, limit)
    elif exchange == "Binance Perpetual":
        return fetch_binance_perp_klines(symbol, interval, limit)
    elif exchange == "Binance Spot":
        return fetch_binance_spot_klines(symbol, interval, limit)
    return None

# -----------------------
# ENHANCED Technical Analysis with Bollinger Bands (v5.5)
# -----------------------
class EnhancedTechnicalAnalysis:
    """Enhanced TA with 5 indicators including Bollinger Bands"""
    
    def __init__(self, klines: List[List]):
        if len(klines) < 50:
            raise ValueError("Need at least 50 candles")
        self.closes = np.array([safe_float(k[4]) for k in klines], dtype=float)
        self.highs = np.array([safe_float(k[2]) for k in klines], dtype=float)
        self.lows = np.array([safe_float(k[3]) for k in klines], dtype=float)
        self.volumes = np.array([safe_float(k[5]) for k in klines], dtype=float)
        self.opens = np.array([safe_float(k[1]) for k in klines], dtype=float)

    def _ema(self, data: np.ndarray, period: int) -> np.ndarray:
        """Calculate EMA"""
        alpha = 2 / (period + 1)
        ema = np.zeros_like(data, dtype=float)
        ema[0] = data[0]
        for i in range(1, len(data)):
            ema[i] = alpha * data[i] + (1 - alpha) * ema[i - 1]
        return ema

    def calculate_rsi_enhanced(self, period: int = 14) -> Tuple[float, str]:
        """Enhanced RSI with Wilder's smoothing"""
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
        """Optimized MACD for crypto"""
        macd_line = self._ema(self.closes, fast) - self._ema(self.closes, slow)
        signal_line = self._ema(macd_line, signal)
        histogram = macd_line - signal_line
        macd_val = round(macd_line[-1], 6)
        signal_val = round(signal_line[-1], 6)
        hist_val = round(histogram[-1], 6)
        if len(histogram) >= 2:
            if macd_val > signal_val:
                trend = "Bullish"
                strength = "Strong" if hist_val > histogram[-2] else "Weak"
            else:
                trend = "Bearish"
                strength = "Strong" if hist_val < histogram[-2] else "Weak"
        else:
            trend = "Neutral"
            strength = "Weak"
        return macd_val, signal_val, hist_val, trend, strength

    def calculate_ema_trend(self, fast: int = 9, slow: int = 21) -> Tuple[str, float]:
        """EMA-based trend detection"""
        ema_fast = self._ema(self.closes, fast)
        ema_slow = self._ema(self.closes, slow)
        current_fast = ema_fast[-1]
        current_slow = ema_slow[-1]
        distance_pct = ((current_fast - current_slow) / current_slow) * 100 if current_slow != 0 else 0
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
        
        if volume_ratio > 2.0:
            if price_change > 0:
                signal = "Very Strong Bullish"
                description = "Strong buying pressure"
            else:
                signal = "Very Strong Bearish"
                description = "Strong selling pressure"
        elif volume_ratio > 1.5:
            if price_change > 0:
                signal = "Strong Bullish"
                description = "Price ↑ + High volume"
            else:
                signal = "Strong Bearish"
                description = "Price ↓ + High volume"
        elif volume_ratio > 1.0:
            if price_change > 0:
                signal = "Bullish"
                description = "Price ↑ + Volume ↑"
            else:
                signal = "Bearish"
                description = "Price ↓ + Volume ↑"
        elif volume_ratio > 0.5:
            signal = "Weak"
            description = "Low volume"
        else:
            signal = "Very Weak"
            description = "Very low volume"
        return signal, description

    def calculate_bollinger_bands(self, period: int = 20, std_dev: float = 2.0) -> Tuple[float, float, float, str]:
        """Calculate Bollinger Bands"""
        if len(self.closes) < period:
            return 0.0, 0.0, 0.0, "Insufficient data"
        sma = float(np.mean(self.closes[-period:]))
        std = float(np.std(self.closes[-period:]))
        upper_band = sma + (std_dev * std)
        lower_band = sma - (std_dev * std)
        current_price = float(self.closes[-1])
        band_width = upper_band - lower_band
        price_position = (current_price - lower_band) / band_width if band_width > 0 else 0.5
        
        if current_price >= upper_band * 1.01:
            signal = "Strong Overbought"
        elif current_price >= upper_band:
            signal = "Overbought"
        elif price_position > 0.85:
            signal = "Near Overbought"
        elif price_position > 0.60:
            signal = "Upper Range"
        elif price_position >= 0.40:
            signal = "Mid-Range"
        elif price_position >= 0.15:
            signal = "Lower Range"
        elif current_price <= lower_band:
            signal = "Oversold"
        elif current_price <= lower_band * 0.99:
            signal = "Strong Oversold"
        else:
            signal = "Near Oversold"
        return round(upper_band, 2), round(lower_band, 2), round(sma, 2), signal

    def calculate_enhanced_rating(self) -> Tuple[str, int, Dict[str, Any]]:
        """Enhanced rating with 5 indicators (20 points each)"""
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
        if "Very Strong Bullish" in volume_signal:
            score += 20
        elif "Strong Bullish" in volume_signal:
            score += 18
        elif "Bullish" in volume_signal:
            score += 16
        elif "Very Strong Bearish" in volume_signal:
            score += 5
        elif "Strong Bearish" in volume_signal:
            score += 8
        elif "Bearish" in volume_signal:
            score += 10
        else:
            score += 14
        
        # 5. Bollinger Bands (20 points) - NEW in v5.5!
        upper_bb, lower_bb, middle_bb, bb_signal = self.calculate_bollinger_bands()
        details["bollinger_bands"] = {
            "upper": upper_bb,
            "lower": lower_bb,
            "middle": middle_bb,
            "signal": bb_signal
        }
        if "Strong Oversold" in bb_signal:
            score += 20
        elif "Oversold" in bb_signal or "Near Oversold" in bb_signal:
            score += 18
        elif "Lower Range" in bb_signal:
            score += 16
        elif "Mid-Range" in bb_signal:
            score += 14
        elif "Upper Range" in bb_signal:
            score += 12
        elif "Overbought" in bb_signal or "Near Overbought" in bb_signal:
            score += 10
        elif "Strong Overbought" in bb_signal:
            score += 5
        else:
            score += 14
        
        # Determine rating with CLEAN emojis
        if score >= 85:
            rating = "Strong Buy 🚀"
        elif score >= 70:
            rating = "Buy 🚀"
        elif score >= 55:
            rating = "Moderate Buy ↗️"
        elif score >= 45:
            rating = "Neutral ➡️"
        elif score >= 30:
            rating = "Moderate Sell ↘️"
        elif score >= 15:
            rating = "Sell 📉"
        else:
            rating = "Strong Sell 📉"
        
        # Market sentiment with clean emojis
        if score >= 60:
            sentiment = "Bullish 🚀"
        elif score >= 40:
            sentiment = "Neutral ➡️"
        else:
            sentiment = "Bearish 📉"
        details["sentiment"] = sentiment
        return rating, score, details

# -----------------------
# Formatting Functions
# -----------------------
def fmt_price(v: float) -> str:
    """Format price"""
    if v >= 1000:
        return f"${v:,.0f}"
    if v >= 1:
        return f"${v:,.2f}"
    return f"${v:,.6f}"

def fmt_large_number(v: float) -> str:
    """Format large numbers"""
    if v >= 1_000_000_000:
        return f"${v / 1_000_000_000:.1f}B"
    elif v >= 1_000_000:
        return f"${v / 1_000_000:.0f}M"
    elif v >= 1_000:
        return f"${v / 1_000:.0f}K"
    else:
        return f"${v:.0f}"

def build_update(symbol_in: str, interval_in: Optional[str]) -> str:
    """Build market update message"""
    symbol = symbol_in.upper().replace("/", "")
    interval = (interval_in or DEFAULT_INTERVAL).lower()
    if interval not in VALID_INTERVALS:
        interval = DEFAULT_INTERVAL

    resolved, exchange, note = _resolve_symbol_or_fallback(symbol)
    if not resolved or not exchange:
        return f"❌ Symbol {symbol} not found.\n\nTry: /BTC, /ETH, /SOL, /ASTER, /XAUT\n\nSupported: All Bybit & Binance perpetual pairs"

    t = fetch_ticker(resolved, exchange)
    if not t or "lastPrice" not in t:
        return f"Could not fetch stats for {resolved}. Try again later."

    # SMART FALLBACK CASCADE for klines
    kl = None
    ta_details = None
    rating = "N/A"
    score = 0
    sentiment = "N/A"
    volume_signal = "N/A"
    volume_desc = ""
    
    # Try 1: Mudrex with requested interval
    kl = fetch_mudrex_klines(resolved, interval, limit=100)
    if kl and len(kl) >= 50:
        log.info(f"✅ Using Mudrex {interval} data ({len(kl)} candles)")
    else:
        # Try 2: Exchange with requested interval
        log.info(f"Mudrex {interval} insufficient, trying exchange...")
        kl = fetch_klines(resolved, exchange, interval=interval, limit=100)
        if kl and len(kl) >= 50:
            log.info(f"✅ Using exchange {interval} data ({len(kl)} candles)")
    
    # Try 3: Fallback to 4h if daily requested but insufficient
    if (not kl or len(kl) < 50) and interval in ["1d", "3d", "1w"]:
        log.info(f"Insufficient {interval} data, trying 4h fallback...")
        kl = fetch_klines(resolved, exchange, interval="4h", limit=100)
        if kl and len(kl) >= 50:
            log.info(f"✅ Using 4h fallback data ({len(kl)} candles)")
    
    # Try 4: Fallback to 1h if still insufficient
    if not kl or len(kl) < 50:
        log.info(f"Still insufficient, trying 1h fallback...")
        kl = fetch_klines(resolved, exchange, interval="1h", limit=100)
        if kl and len(kl) >= 50:
            log.info(f"✅ Using 1h fallback data ({len(kl)} candles)")

    # Technical analysis
    if kl and len(kl) >= 50:
        try:
            ta = EnhancedTechnicalAnalysis(kl)
            rating, score, ta_details = ta.calculate_enhanced_rating()
            sentiment = ta_details.get("sentiment", "N/A")
            volume_signal = ta_details.get("volume_signal", "N/A")
            volume_desc = ta_details.get("volume_desc", "")
        except Exception as e:
            log.warning("Enhanced TA failed: %s", e)
    else:
        log.warning(f"Insufficient candle data for {resolved} even with fallbacks")

    # Build OI, FR, Liquidations
    oi_line = ""
    funding_line = ""
    liq_line = ""
    last_price = safe_float(t.get("lastPrice", 0))
    
    # PRIORITY 1: Mudrex
    mudrex_stats = fetch_mudrex_market_stats(resolved)
    if mudrex_stats and mudrex_stats.get("openInterest"):
        oi_value = safe_float(mudrex_stats["openInterest"])
        if oi_value > 0:
            oi_line = f"• Open Interest: {fmt_large_number(oi_value)}\n"
        else:
            oi_line = "• Open Interest: Data unavailable\n"
    else:
        # Fallback 1: CoinGlass
        coinglass_data = fetch_coinglass_data(resolved)
        if coinglass_data.get("open_interest"):
            oi_value = safe_float(coinglass_data["open_interest"])
            if oi_value > 0:
                oi_line = f"• Open Interest: {fmt_large_number(oi_value)}\n"
            else:
                oi_line = "• Open Interest: Data unavailable\n"
        else:
            # Fallback 2: Manual
            manual_oi = fetch_aggregated_oi_manual(resolved, last_price)
            if manual_oi and manual_oi > 0:
                oi_line = f"• Open Interest: {fmt_large_number(manual_oi)}\n"
            else:
                # Fallback 3: Single exchange
                exchange_oi = None
                if exchange == "Bybit Perpetual":
                    bybit_oi = safe_float(t.get("openInterest", 0))
                    if bybit_oi > 0 and last_price > 0:
                        exchange_oi = bybit_oi * last_price
                elif exchange == "Binance Perpetual":
                    binance_oi = fetch_binance_perp_open_interest(resolved)
                    if binance_oi and binance_oi > 0 and last_price > 0:
                        exchange_oi = binance_oi * last_price
                if exchange_oi and exchange_oi > 0:
                    oi_line = f"• Open Interest: {fmt_large_number(exchange_oi)}\n"
                else:
                    if exchange in ["Bybit Perpetual", "Binance Perpetual"]:
                        oi_line = "• Open Interest: Data unavailable\n"
                    else:
                        oi_line = ""
    
    # Funding Rate
    if mudrex_stats and mudrex_stats.get("fundingRate") is not None:
        exchange_funding = safe_float(mudrex_stats["fundingRate"])
    else:
        exchange_funding = None
        if exchange == "Bybit Perpetual":
            exchange_funding = safe_float(t.get("fundingRate", 0))
        elif exchange == "Binance Perpetual":
            exchange_funding = fetch_binance_perp_funding_rate(resolved)
    
    if exchange_funding is not None:
        fr_pct = exchange_funding * 100
        if fr_pct > 0.01:
            funding_line = f"• Funding Rate: {fr_pct:.4f}% (Longs paying)\n"
        elif fr_pct < -0.01:
            funding_line = f"• Funding Rate: {fr_pct:.4f}% (Shorts paying)\n"
        else:
            funding_line = f"• Funding Rate: {fr_pct:.4f}% (Neutral)\n"
    else:
        funding_line = "• Funding Rate: Neutral\n"
    
    # Liquidations
    high = safe_float(t.get("highPrice", 0))
    low = safe_float(t.get("lowPrice", 0))
    if last_price > 0:
        volatility = ((high - low) / last_price) * 100
        if volatility > 5:
            pct = safe_float(t.get("priceChangePercent", 0.0))
            if pct > 0:
                liq_line = "• 24H Liquidations: High shorts wiped\n"
            else:
                liq_line = "• 24H Liquidations: High longs wiped\n"
        else:
            liq_line = "• 24H Liquidations: Balanced\n"
    else:
        liq_line = "• 24H Liquidations: Balanced\n"

    pct = safe_float(t.get("priceChangePercent", 0.0))
    arrow = "📈" if pct >= 0 else "📉"

    header = f"📊 {resolved} — Market Update"
    if note:
        header += f"\n💡 {note}"
    
    # Get Bollinger Bands signal
    bb_line = ""
    if ta_details and ta_details.get("bollinger_bands"):
        bb_signal = ta_details["bollinger_bands"]["signal"]
        bb_line = f"• Bollinger Bands: {bb_signal}\n"

    return (
        f"{header}\n\n"
        f"📈 Market Bias - {interval.upper()}\n"
        f"• Sentiment: {sentiment}\n"
        f"• Rating: {rating} (Score: {score}/100)\n"
        f"• Volume: {volume_signal} ({volume_desc})\n"
        f"{bb_line}"
        f"{oi_line}"
        f"{funding_line}"
        f"{liq_line}\n"
        f"💹 Price Stats\n"
        f"• Current: {fmt_price(last_price)}\n"
        f"• 24H High: {fmt_price(high)}\n"
        f"• 24H Low: {fmt_price(low)}\n"
        f"• 24H Change: {arrow} {abs(pct):.2f}%\n"
        f"══════════════════════════\n"
        f"Powered by Mudrex Market Intelligence"
    )

# -----------------------
# Telegram Handlers
# -----------------------
@retry_on_telegram_error(max_retries=3, delay=5)
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    if update.message:
        await update.message.reply_text(
            "Welcome to Mudrex MI Bot v5.5 FINAL! 🚀\n\n"
            "Get real-time perpetual futures analysis.\n\n"
            "Quick Start:\n"
            "• /BTC - Bitcoin perpetual\n"
            "• /ETH 15m - Ethereum 15-minute\n"
            "• /SOL 1d - Solana daily\n"
            "• /ASTER 4h - Aster 4-hour\n"
            "• /XAUT - Gold token\n"
            "• /help - Detailed guide\n\n"
            "🆕 v5.5: Bollinger Bands + Smart Fallbacks!\n\n"
            "Works with ALL Bybit & Binance perpetual pairs!\n\n"
            "Try: /BTC or /ETH",
            disable_web_page_preview=True
        )

@retry_on_telegram_error(max_retries=3, delay=5)
async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /help command"""
    if update.message:
        await update.message.reply_text(
            "Mudrex MI Bot v5.5 FINAL - Usage Guide\n\n"
            "Basic Usage:\n"
            "• /BTC – Market update (1h default)\n"
            "• /BTC 15m – Custom timeframe\n"
            "• /ASTER 1d – Works with any perpetual pair\n"
            "• /XAUT – Gold token analysis\n\n"
            "Timeframes:\n"
            "1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w\n\n"
            "Exchange Priority:\n"
            "1. Bybit Perpetual (100+ pairs)\n"
            "2. Binance Perpetual (200+ pairs)\n"
            "3. Binance Spot (fallback)\n\n"
            "Smart Fallbacks:\n"
            "If 1d data unavailable → tries 4h → tries 1h\n"
            "Always finds data for you!\n\n"
            "Features:\n"
            "✅ 5 Technical Indicators\n"
            "✅ Bollinger Bands (OB/OS)\n"
            "✅ Open Interest (4-tier)\n"
            "✅ Funding Rate\n"
            "✅ Volume Analysis (7 levels)\n"
            "✅ Liquidation Tracking\n"
            "✅ Works with ALL perpetual pairs\n\n"
            "Examples:\n"
            "• /BTC - Bitcoin\n"
            "• /ETH 4h - Ethereum 4-hour\n"
            "• /SOL 1d - Solana daily\n"
            "• /ASTER - Aster perpetual\n"
            "• /XAUT - Gold token\n\n"
            "Powered by Mudrex Market Intelligence\n"
            "https://mudrex.go.link/f8PJF",
            disable_web_page_preview=True
        )

@retry_on_telegram_error(max_retries=3, delay=5)
async def any_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle any symbol command"""
    if not update.message:
        return
    user_id = update.message.from_user.id
    if not check_rate_limit(user_id):
        await update.message.reply_text(
            "⚠️ Rate limit exceeded (10 req/min). Please wait.",
            disable_web_page_preview=True
        )
        return
    text = update.message.text or ""
    symbol, tf = _normalize_command(text)
    if not symbol:
        await update.message.reply_text(
            "Please provide a symbol.\n\nExamples: /BTC, /ETH, /SOL, /ASTER, /XAUT",
            disable_web_page_preview=True
        )
        return
    loop = asyncio.get_running_loop()
    msg = await loop.run_in_executor(None, build_update, symbol, tf)
    await update.message.reply_text(msg, parse_mode="Markdown", disable_web_page_preview=True)

async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Handle errors"""
    log.exception("Unhandled error: %s", context.error)

# -----------------------
# Graceful Shutdown
# -----------------------
def signal_handler(signum, frame):
    """Handle shutdown signals"""
    global application
    log.info(f"🛑 Received signal {signum}. Shutting down...")
    if application:
        try:
            if hasattr(application, 'shutdown'):
                asyncio.run(application.shutdown())
            log.info("✅ Bot stopped successfully")
        except Exception as e:
            log.error(f"Error during shutdown: {e}")
    sys.exit(0)

# -----------------------
# Main Entry Point
# -----------------------
def main():
    """Main entry point for Railway"""
    global application
    if not TELEGRAM_TOKEN:
        log.error("❌ TELEGRAM_TOKEN not set!")
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
        log.info("🚂 MUDREX MI BOT v5.5 - ULTIMATE EDITION")
        log.info("=" * 60)
        log.info("✅ Exchange Priority: Bybit → Binance Perp → Spot")
        log.info("✅ Technical Indicators: 5 (RSI, MACD, EMA, Volume, BB)")
        log.info("✅ Data: Mudrex API + CoinGlass + Manual")
        log.info("✅ Features: OI, FR, Liquidations, Volume")
        log.info("✅ Bollinger Bands: NEW in v5.5!")
        log.info("✅ Rate Limiting: 10 req/min per user")
        log.info("✅ Caching: Enabled")
        log.info("=" * 60)
        application.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True,
            close_loop=False,
            timeout=20,
            poll_interval=2.0
        )
    except KeyboardInterrupt:
        log.info("🛑 Keyboard interrupt")
    except Exception as e:
        log.error(f"❌ FATAL ERROR: {e}")
        log.exception("Full traceback:")
    finally:
        if application:
            log.info("🧹 Cleaning up...")

if __name__ == "__main__":
    log.info("🚀 Starting Mudrex MI Bot v5.5...")
    main()
