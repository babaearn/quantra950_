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
# Exchange Configuration
# -----------------------
BINANCE_HOSTS = [
    "https://api.binance.com",
    "https://api-gcp.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
    "https://api4.binance.com",
]

KUCOIN_HOSTS = [
    "https://api.kucoin.com"
]

HTTP_TIMEOUT = 5
EXCHANGEINFO_TTL = 30 * 60

_exchangeinfo_cache: Dict[str, Any] = {"binance": {"expires": 0, "data": None}, "kucoin": {"expires": 0, "data": None}}
PREFERRED_QUOTES = ["USDT", "FDUSD", "USDC"]

# Interval mappings
VALID_INTERVALS = {
    "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w"
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
# Retry Decorator
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
# HTTP Helpers
# -----------------------
def _http_get_binance(path: str, params: dict) -> Optional[requests.Response]:
    """HTTP GET for Binance with multi-host failover"""
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
# Symbol Resolution
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
    """Resolve symbol with smart fallback"""
    is_xaut = "XAUT" in symbol.upper()
    
    if is_xaut:
        if _kucoin_symbol_exists(symbol):
            log.info(f"Found {symbol} on KuCoin")
            return symbol, "KuCoin", None
    
    if _binance_symbol_exists(symbol):
        log.info(f"Found {symbol} on Binance")
        return symbol, "Binance", None

    for q in sorted(PREFERRED_QUOTES, key=len, reverse=True):
        if symbol.endswith(q):
            base = symbol[:-len(q)]
            if not base:
                break
            for alt in PREFERRED_QUOTES:
                alt_sym = f"{base}{alt}"
                if is_xaut and _kucoin_symbol_exists(alt_sym):
                    log.info(f"Resolved {symbol} to {alt_sym} on KuCoin")
                    return alt_sym, "KuCoin", f"Resolved to {alt_sym}"
                if _binance_symbol_exists(alt_sym):
                    log.info(f"Resolved {symbol} to {alt_sym} on Binance")
                    return alt_sym, "Binance", f"Resolved to {alt_sym}"
            break

    for q in PREFERRED_QUOTES:
        candidate = f"{symbol}{q}"
        if is_xaut and _kucoin_symbol_exists(candidate):
            log.info(f"Found {candidate} on KuCoin")
            return candidate, "KuCoin", None
        if _binance_symbol_exists(candidate):
            log.info(f"Found {candidate} on Binance")
            return candidate, "Binance", None

    return None, None, None

# -----------------------
# Unified Data Fetching
# -----------------------
def fetch_ticker(symbol: str, exchange: str) -> Optional[dict]:
    """Fetch ticker data from specified exchange"""
    if exchange == "Binance":
        return fetch_binance_ticker(symbol)
    elif exchange == "KuCoin":
        return fetch_kucoin_ticker(symbol)
    return None

def fetch_klines(symbol: str, exchange: str, interval: str, limit: int = 100) -> Optional[List[List]]:
    """Fetch klines data from specified exchange"""
    if exchange == "Binance":
        return fetch_binance_klines(symbol, interval, limit)
    elif exchange == "KuCoin":
        return fetch_kucoin_klines(symbol, interval, limit)
    return None

# -----------------------
# Enhanced Technical Analysis with Bollinger Bands
# -----------------------
class TechnicalAnalysis:
    """Technical analysis calculator with Bollinger Bands"""
    
    def __init__(self, closes: List[float]):
        if len(closes) < 30:
            raise ValueError("Need at least 30 closing prices")
        self.prices = np.array(closes, dtype=float)

    def _ema(self, data: np.ndarray, period: int) -> np.ndarray:
        """Calculate Exponential Moving Average"""
        alpha = 2 / (period + 1)
        ema = np.zeros_like(data, dtype=float)
        ema[0] = data[0]
        for i in range(1, len(data)):
            ema[i] = alpha * data[i] + (1 - alpha) * ema[i - 1]
        return ema

    def calculate_rsi(self, period: int = 14) -> float:
        """Calculate Relative Strength Index"""
        delta = np.diff(self.prices)
        gains = np.where(delta > 0, delta, 0.0)
        losses = np.where(delta < 0, -delta, 0.0)
        avg_gain = float(np.mean(gains[-period:]))
        avg_loss = float(np.mean(losses[-period:]))
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        rsi_value = 100 - (100 / (1 + rs))
        return round(rsi_value, 2)

    def calculate_macd(self, short: int = 12, long: int = 26, signal: int = 9):
        """Calculate MACD indicator"""
        macd = self._ema(self.prices, short) - self._ema(self.prices, long)
        signal_line = self._ema(macd, signal)
        macd_val = round(macd[-1], 4)
        signal_val = round(signal_line[-1], 4)
        trend = "Bullish" if macd_val > signal_val else "Bearish"
        return macd_val, trend

    def calculate_sma(self, period: int) -> float:
        """Calculate Simple Moving Average"""
        return round(float(np.mean(self.prices[-period:])), 4)

    def get_moving_average_signal(self) -> str:
        """Get trend signal from moving averages"""
        sma7, sma25 = self.calculate_sma(7), self.calculate_sma(25)
        if sma7 > sma25 * 1.02:
            return "Strong Uptrend"
        if sma7 > sma25:
            return "Mild Uptrend"
        if sma7 < sma25 * 0.98:
            return "Strong Downtrend"
        if sma7 < sma25:
            return "Mild Downtrend"
        return "Neutral"

    def calculate_bollinger_bands(self, period: int = 20, std_dev: float = 2.0):
        """Calculate Bollinger Bands and price position"""
        if len(self.prices) < period:
            return None, None, None, "Insufficient data"
        
        # Calculate middle band (SMA)
        sma = float(np.mean(self.prices[-period:]))
        
        # Calculate standard deviation
        std = float(np.std(self.prices[-period:]))
        
        # Calculate upper and lower bands
        upper_band = sma + (std_dev * std)
        lower_band = sma - (std_dev * std)
        
        # Current price
        current_price = float(self.prices[-1])
        
        # Determine position and signal
        band_width = upper_band - lower_band
        price_position = (current_price - lower_band) / band_width if band_width > 0 else 0.5
        
        if current_price >= upper_band:
            signal = "Overbought 🔴"
        elif current_price <= lower_band:
            signal = "Oversold 🟢"
        elif price_position > 0.8:
            signal = "Near Upper Band"
        elif price_position < 0.2:
            signal = "Near Lower Band"
        else:
            signal = "Mid-Range ⚪️"
        
        return round(upper_band, 2), round(lower_band, 2), round(sma, 2), signal

    def rating(self) -> str:
        """Get overall market rating based on multiple indicators"""
        rsi = self.calculate_rsi()
        _, macd_trend = self.calculate_macd()
        ma_signal = self.get_moving_average_signal()
        _, _, _, bb_signal = self.calculate_bollinger_bands()

        bullish = bearish = 0
        
        # RSI scoring
        if rsi > 70:
            bearish += 1
        elif rsi < 30:
            bullish += 1
        
        # MACD scoring
        if macd_trend == "Bullish":
            bullish += 1
        else:
            bearish += 1
        
        # MA scoring
        if "Uptrend" in ma_signal:
            bullish += 1
        elif "Downtrend" in ma_signal:
            bearish += 1
        
        # Bollinger Bands scoring (bonus indicator)
        if "Oversold" in bb_signal:
            bullish += 0.5
        elif "Overbought" in bb_signal:
            bearish += 0.5

        if bullish >= 3:
            return "Strong Buy 🟢"
        if bullish >= 2:
            return "Buy 🟢"
        if bearish >= 3:
            return "Strong Sell 🔴"
        if bearish >= 2:
            return "Sell 🔴"
        return "Neutral ⚪️"

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
    """Build formatted market update message with Bollinger Bands"""
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

    ta_lines = ""
    bb_line = ""
    if kl and len(kl) >= 30:
        closes = [float(row[4]) for row in kl]
        try:
            ta = TechnicalAnalysis(closes)
            ta_rating = ta.rating()
            sentiment = "Bullish 🚀" if "Buy" in ta_rating else "Bearish 🔻" if "Sell" in ta_rating else "Neutral ⚪️"
            
            # Get Bollinger Bands
            upper, lower, middle, bb_signal = ta.calculate_bollinger_bands()
            if upper and lower and middle:
                bb_line = f"- Bollinger Bands: {bb_signal}\n"
            
            ta_lines = f"- Market Sentiment: {sentiment}\n- Market Trend (TA): {ta_rating}\n{bb_line}"
        except Exception as e:
            log.warning("TA failed for %s: %s", resolved, e)

    last_price = float(t.get("lastPrice", 0))
    pct = float(t.get("priceChangePercent", 0.0))
    high = float(t.get("highPrice", last_price))
    low = float(t.get("lowPrice", last_price))
    arrow = "▲" if pct >= 0 else "▼"

    header = f"🔸 {resolved} — Market Update"
    if note:
        header += f"\n(Your input `{symbol}` {note})"

    return (
        f"{header}\n"
        f"——————————————\n"
        f"{ta_lines}"
        f"- Last Price: {fmt_price(last_price)}\n"
        f"- 24h Change: {arrow} {abs(pct):.2f}%\n"
        f"- Day High: {fmt_price(high)}\n"
        f"- Day Low: {fmt_price(low)}\n"
        f"——————————————\n"
        f"Powered by [Mudrex Market Intelligence](https://mudrex.go.link/f8PJF)"
    )

# -----------------------
# Telegram Handlers
# -----------------------
@retry_on_telegram_error(max_retries=3, delay=5)
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    if update.message:
        await update.message.reply_text(
            "Welcome to Mudrex MI Bot! 🚀\n\n"
            "Get real-time crypto market analysis with advanced technical indicators.\n\n"
            "Quick Start:\n"
            "• `/BTCUSDT` - Get Bitcoin market update\n"
            "• `/ETHUSDT 15m` - Get Ethereum with 15-minute timeframe\n"
            "• `/XAUTUSDT` - Get XAUT (Gold token)\n"
            "• `/help` - View detailed usage guide\n\n"
            "🆕 Now with Bollinger Bands!\n\n"
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
            "*Technical Indicators:*\n"
            "✅ RSI (Relative Strength Index)\n"
            "✅ MACD (Moving Average Convergence Divergence)\n"
            "✅ Moving Averages (7/25 SMA)\n"
            "✅ Bollinger Bands (Overbought/Oversold)\n"
            "✅ Market sentiment analysis\n"
            "✅ Buy/Sell signals\n\n"
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
# Graceful Shutdown
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
# Main Entry Point
# -----------------------
def main():
    """Main entry point for Railway deployment"""
    global application
    
    if not TELEGRAM_TOKEN:
        log.error("❌ TELEGRAM_TOKEN not set! Add to Railway environment variables.")
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
        log.info("🚂 MUDREX MI BOT - ENHANCED WITH BOLLINGER BANDS")
        log.info("=" * 60)
        log.info(f"✅ Environment: Railway")
        log.info(f"✅ Port: {PORT}")
        log.info(f"✅ Exchanges: Binance (primary) + KuCoin (XAUT)")
        log.info(f"✅ Indicators: RSI, MACD, MA, Bollinger Bands")
        log.info(f"✅ Timeouts: {TELEGRAM_CONNECT_TIMEOUT}s")
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
