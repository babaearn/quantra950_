import os
import time
import logging
import signal
import sys
from typing import List, Optional, Dict, Any, Tuple
from functools import wraps

import requests
import numpy as np
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    ContextTypes, filters
)

# -----------------------
# Logging Configuration for Railway
# -----------------------
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout)  # Railway captures stdout
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
TELEGRAM_READ_TIMEOUT = 30
TELEGRAM_CONNECT_TIMEOUT = 30
TELEGRAM_WRITE_TIMEOUT = 30
TELEGRAM_POOL_TIMEOUT = 30

# -----------------------
# KuCoin Configuration
# -----------------------
KUCOIN_HOSTS = [
    "https://api.kucoin.com",
    "https://api-futures.kucoin.com"
]
HTTP_TIMEOUT = 5

VALID_INTERVALS = {
    "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w"
}
DEFAULT_INTERVAL = "1h"

# Map intervals to KuCoin format
INTERVAL_TO_KUCOIN = {
    "1m": "1min",
    "3m": "3min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1hour",
    "2h": "2hour",
    "4h": "4hour",
    "6h": "6hour",
    "8h": "6hour",
    "12h": "12hour",
    "1d": "1day",
    "3d": "1day",
    "1w": "1week"
}

# -----------------------
# Binance Configuration (Fallback)
# -----------------------
BINANCE_HOSTS = [
    "https://api.binance.com",
    "https://api-gcp.binance.com",
    "https://api1.binance.com",
]

# Global application instance
application = None

# -----------------------
# HTTP Helper with Failover
# -----------------------
def _http_get(hosts: List[str], path: str, params: dict) -> Optional[requests.Response]:
    """HTTP GET with multi-host failover and retry logic"""
    last_exc = None
    for host in hosts:
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

    log.error(f"HTTP GET FAILED: {path} | Last error: {last_exc}")
    return None

# -----------------------
# KuCoin API Functions
# -----------------------
def fetch_kucoin_ticker(symbol: str) -> Optional[dict]:
    """Fetch 24-hour ticker from KuCoin"""
    try:
        r = _http_get(KUCOIN_HOSTS, f"/api/v1/market/stats", params={"symbol": symbol})
        if not r or r.status_code != 200:
            return None
        
        data = r.json()
        if data.get("code") != "200000" or not data.get("data"):
            return None
        
        ticker_data = data["data"]
        
        # Convert to Binance-like format
        last_price = float(ticker_data.get("last", 0))
        open_price = float(ticker_data.get("open", last_price))
        price_change = last_price - open_price
        price_change_percent = (price_change / open_price * 100) if open_price > 0 else 0
        
        return {
            "symbol": symbol,
            "lastPrice": str(last_price),
            "priceChange": str(price_change),
            "priceChangePercent": price_change_percent,
            "highPrice": ticker_data.get("high", str(last_price)),
            "lowPrice": ticker_data.get("low", str(last_price)),
            "volume": ticker_data.get("vol", "0")
        }
    except Exception as e:
        log.error(f"KuCoin ticker fetch error for {symbol}: {e}")
        return None

def fetch_kucoin_klines(symbol: str, interval: str, limit: int = 100) -> Optional[List[List]]:
    """Fetch klines from KuCoin"""
    try:
        kucoin_interval = INTERVAL_TO_KUCOIN.get(interval, "1hour")
        end_time = int(time.time())
        
        interval_seconds = {
            "1min": 60, "3min": 180, "5min": 300, "15min": 900, "30min": 1800,
            "1hour": 3600, "2hour": 7200, "4hour": 14400, "6hour": 21600,
            "12hour": 43200, "1day": 86400, "1week": 604800
        }
        seconds = interval_seconds.get(kucoin_interval, 3600)
        start_time = end_time - (seconds * limit)
        
        r = _http_get(KUCOIN_HOSTS, f"/api/v1/market/candles", params={
            "symbol": symbol,
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
        for candle in reversed(data["data"]):
            klines.append([
                int(candle[0]) * 1000,
                candle[1],
                candle[3],
                candle[4],
                candle[2],
                candle[5],
                0, 0, 0, 0, 0, 0
            ])
        
        return klines
    except Exception as e:
        log.error(f"KuCoin klines fetch error for {symbol}: {e}")
        return None

# -----------------------
# Binance API Functions (Fallback)
# -----------------------
def fetch_binance_ticker(symbol: str) -> Optional[dict]:
    """Fetch 24-hour ticker from Binance"""
    r = _http_get(BINANCE_HOSTS, "/api/v3/ticker/24hr", params={"symbol": symbol})
    if not r or r.status_code != 200:
        return None
    return r.json()

def fetch_binance_klines(symbol: str, interval: str, limit: int = 100) -> Optional[List[List]]:
    """Fetch klines from Binance"""
    r = _http_get(BINANCE_HOSTS, "/api/v3/klines", params={
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    })
    if not r or r.status_code != 200:
        return None
    return r.json()

# -----------------------
# Unified API Functions
# -----------------------
def fetch_ticker(symbol: str, exchange: str = "kucoin") -> Optional[dict]:
    """Fetch ticker from specified exchange with fallback"""
    if exchange == "kucoin":
        ticker = fetch_kucoin_ticker(symbol)
        if ticker:
            return ticker
        log.warning(f"KuCoin failed for {symbol}, trying Binance...")
        return fetch_binance_ticker(symbol)
    else:
        return fetch_binance_ticker(symbol)

def fetch_klines(symbol: str, exchange: str = "kucoin", interval: str = "1h", limit: int = 100) -> Optional[List[List]]:
    """Fetch klines from specified exchange with fallback"""
    if exchange == "kucoin":
        klines = fetch_kucoin_klines(symbol, interval, limit)
        if klines:
            return klines
        log.warning(f"KuCoin klines failed for {symbol}, trying Binance...")
        return fetch_binance_klines(symbol, interval, limit)
    else:
        return fetch_binance_klines(symbol, interval, limit)

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
    """Resolve symbol with smart fallback - returns (resolved_symbol, exchange, note)"""
    
    # Try KuCoin first
    test_symbol = symbol if symbol.endswith("USDT") else f"{symbol}USDT"
    ticker = fetch_kucoin_ticker(test_symbol)
    if ticker and "lastPrice" in ticker:
        note = f"resolved to {test_symbol}" if symbol != test_symbol else None
        return test_symbol, "kucoin", note
    
    # Try Binance as fallback
    ticker = fetch_binance_ticker(test_symbol)
    if ticker and "lastPrice" in ticker:
        note = f"resolved to {test_symbol}" if symbol != test_symbol else None
        return test_symbol, "binance", note
    
    return None, None, None

# -----------------------
# Technical Analysis
# -----------------------
class TechnicalAnalysis:
    """Simple technical analysis for market sentiment"""
    
    def __init__(self, closes: List[float]):
        if len(closes) < 30:
            raise ValueError("Need at least 30 closing prices for analysis")
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

    def rating(self) -> str:
        """Simple rating based on RSI and trend"""
        rsi = self.calculate_rsi()
        
        # Simple trend check
        short_ema = self._ema(self.prices, 9)
        long_ema = self._ema(self.prices, 21)
        trend_up = short_ema[-1] > long_ema[-1]
        
        if rsi < 30:
            return "Strong Buy 🟢🟢"
        elif rsi < 40 and trend_up:
            return "Buy 🟢"
        elif rsi > 70:
            return "Strong Sell 🔴🔴"
        elif rsi > 60 and not trend_up:
            return "Sell 🔴"
        else:
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
    """Build formatted market update message with improved format"""
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

    # Calculate all metrics first
    last_price = float(t.get("lastPrice", 0))
    pct = float(t.get("priceChangePercent", 0.0))
    high = float(t.get("highPrice", last_price))
    low = float(t.get("lowPrice", last_price))
    current_volume = float(t.get("volume", 0))
    
    # Technical Analysis
    ta_rating = "N/A"
    sentiment = "N/A"
    volume_signal = "N/A"
    volume_explanation = ""
    
    if kl and len(kl) >= 30:
        closes = [float(row[4]) for row in kl]
        try:
            ta = TechnicalAnalysis(closes)
            ta_rating = ta.rating()
            sentiment = "Bullish 🚀" if "Buy" in ta_rating else "Bearish 🔻" if "Sell" in ta_rating else "Neutral ⚪️"
        except Exception as e:
            log.warning("TA failed for %s: %s", resolved, e)
    
    # Volume calculation with enhanced explanations
    volume_change_pct = 0
    volume_trend = ""
    
    if kl and len(kl) >= 2:
        try:
            volumes = [float(k[5]) for k in kl[:-1]]
            avg_volume = sum(volumes) / len(volumes) if volumes else 0
            
            if avg_volume > 0:
                volume_change_pct = ((current_volume - avg_volume) / avg_volume) * 100
                
                # Determine price and volume directions
                price_up = pct > 0
                volume_up = volume_change_pct > 0
                
                # Enhanced volume signal with clear explanations
                if price_up and volume_up:
                    volume_signal = "Strong Bullish 🟢"
                    volume_explanation = "(Price ↑ + Volume ↑)"
                    volume_trend = "bullish"
                elif price_up and not volume_up:
                    volume_signal = "Weak Bullish 🟢"
                    volume_explanation = "(Price ↑ + Volume ↓)"
                    volume_trend = "bullish"
                elif not price_up and volume_up:
                    volume_signal = "Weak Bearish 🔻"
                    volume_explanation = "(Price ↓ + Volume ↑)"
                    volume_trend = "bearish"
                else:
                    volume_signal = "Strong Bearish 🔻"
                    volume_explanation = "(Price ↓ + Volume ↓)"
                    volume_trend = "bearish"
                    
        except Exception as e:
            log.warning("Volume calc failed for %s: %s", resolved, e)
    
    # Format volume change for display (cap at ±1000%)
    if abs(volume_change_pct) > 1000:
        volume_display = f"{'▲' if volume_change_pct > 0 else '▼'} 1000%+"
    else:
        volume_display = f"{'▲' if volume_change_pct >= 0 else '▼'} {abs(volume_change_pct):.1f}%"
    
    # Add simple explanation for volume change
    volume_change_explanation = f"({volume_trend} change)" if volume_trend else ""
    
    # Price change arrow
    price_arrow = "▲" if pct >= 0 else "▼"
    
    # Build message with NEW FORMAT - Signals section + Stats section
    header = f"🔸 {resolved} — Market Update ([Mudrex](https://mudrex.go.link/f8PJF))"
    if note:
        header += f"\n(Your input `{symbol}` {note})"

    message = (
        f"{header}\n"
        f"{'─'*26}\n"
        f"**Signals**\n"
        f"• Market Sentiment: {sentiment}\n"
        f"• Technical Rating: {ta_rating}\n"
        f"• Volume Signal: {volume_signal} {volume_explanation}\n"
        f"\n"
        f"**Stats**\n"
        f"• Last Price: {fmt_price(last_price)}\n"
        f"• 24h Change: {price_arrow} {abs(pct):.2f}%\n"
        f"• Day High: {fmt_price(high)}\n"
        f"• Day Low: {fmt_price(low)}\n"
        f"• 24h Volume Change: {volume_display} {volume_change_explanation}\n"
        f"{'═'*26}\n"
        f"Powered by Mudrex Market Intelligence"
    )
    
    return message

# -----------------------
# Telegram Handlers
# -----------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    if update.message:
        await update.message.reply_text(
            "Welcome to Mudrex MI Bot! 🚀\n\n"
            "Get real-time crypto market analysis with technical indicators.\n\n"
            "Quick Start:\n"
            "• `/BTCUSDT` - Get Bitcoin market update\n"
            "• `/ETHUSDT 15m` - Get Ethereum with 15-minute timeframe\n"
            "• `/help` - View detailed usage guide\n\n"
            "Try it now! Send any crypto symbol like `/BTC` or `/ETH`"
        )

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /help command"""
    if update.message:
        await update.message.reply_text(
            "📖 *Mudrex MI Bot - Usage Guide*\n\n"
            "*Basic Usage:*\n"
            "• `/BTCUSDT` – Get market update (default 1h timeframe)\n"
            "• `/BTCUSDT 15m` – Specify custom timeframe\n"
            "• `/ETH` – Auto-resolves to ETHUSDT\n"
            "• `/BTC 1d` – Bitcoin with daily timeframe\n\n"
            "*Supported Timeframes:*\n"
            "`1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`, `3d`, `1w`\n\n"
            "*What You Get:*\n"
            "✅ Real-time price data\n"
            "✅ 24-hour price change\n"
            "✅ Technical analysis (RSI, EMA)\n"
            "✅ Volume analysis\n"
            "✅ Buy/Sell signals\n\n"
            "*Examples:*\n"
            "• `/SOLUSDT` - Solana market update\n"
            "• `/ATOM 4h` - Cosmos with 4-hour chart\n\n"
            "💡 *Tip:* Use short forms like `/BTC` instead of `/BTCUSDT`\n\n"
            "Powered by [Mudrex Market Intelligence](https://mudrex.go.link/f8PJF)",
            parse_mode="Markdown",
            disable_web_page_preview=True
        )

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
        sys.exit(1)

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
        log.info("🚂 MUDREX MI BOT - PRODUCTION EDITION")
        log.info("=" * 60)
        log.info(f"✅ Environment: Railway")
        log.info(f"✅ Port: {PORT}")
        log.info(f"✅ Bot initialized successfully")
        log.info(f"✅ Multi-source API: KuCoin + Binance fallback")
        log.info(f"✅ Technical Analysis: RSI + EMA")
        log.info(f"✅ Volume Analysis: Enhanced signals")
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
        sys.exit(1)
    finally:
        if application:
            log.info("🧹 Cleaning up resources...")

if __name__ == "__main__":
    main()
