# services/dhan_service.py

import os
import requests
from dotenv import load_dotenv
from config.db_config import db
from datetime import datetime, timedelta
from dhanhq import dhanhq
import pandas as pd
from utils.logger import logger
import time


load_dotenv()

class DhanService:
    """Handles all Dhan API interactions (fetch data, place orders, etc.)"""

    def __init__(self):
        # Try from .env first
        self.client_id = os.getenv("DHAN_CLIENT_ID")
        self.access_token = os.getenv("DHAN_ACCESS_TOKEN")
        self.base_url = os.getenv("DHAN_BASE_URL", "https://api.dhan.co")

        # If not found, try from MongoDB (you said creds are saved there)
        if not self.client_id or not self.access_token:
            creds = db["config"].find_one({"type": "dhan_creds"})
            if creds:
                self.client_id = creds.get("client_id")
                self.access_token = creds.get("access_token")
                logger.info("Loaded Dhan credentials from MongoDB")
            else:
                raise ValueError("❌ Dhan credentials missing in both .env and DB")

        self.client = dhanhq(self.client_id,  self.access_token)

        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "access-token": self.access_token,
            "client-id": self.client_id
        }

        logger.info("✅ DhanService initialized")

    def fetch_intraday_minute_data(self, symbol_id, from_date, to_date):


       # Call Dhan API
        resp = self.client.intraday_minute_data(
            security_id=symbol_id,
            exchange_segment=dhanhq.NSE,
            instrument_type="EQUITY",
            interval=5,
            from_date=from_date.strftime("%Y-%m-%d"),
            to_date=to_date.strftime("%Y-%m-%d %H:%M:%S")
        )
      

        if resp.get("status") != "success":
            print(resp["data"])
            time.sleep(10)
            return None
        d = resp["data"]
        df = pd.DataFrame({
            "Datetime": pd.to_datetime(d["timestamp"], unit='s', utc=True),
            "Open": d["open"],
            "High": d["high"],
            "Low": d["low"],
            "Close": d["close"],
            "Volume": d["volume"]
        })
        df["SMA20"] = df["Close"].rolling(20).mean()
        df["SMA200"] = df["Close"].rolling(200).mean()
        hl = df["High"] - df["Low"]
        hc = abs(df["High"] - df["Close"].shift())
        lc = abs(df["Low"] - df["Close"].shift())
        df["ATR"] = pd.concat([hl, hc, lc], axis=1).max(axis=1).rolling(14).mean()
        #df.dropna(inplace=True)
        last_candle = df.tail(1).iloc[0].to_dict()
        return last_candle
    
    def fetch_candles(self, symbol_id, symbol, date):
        if isinstance(date, str):
            date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")

        from_date = date - timedelta(days=5)

        resp = self.client.intraday_minute_data(
            security_id=symbol_id,
            exchange_segment="NSE_EQ",
            instrument_type="EQUITY",
            interval=5,
            from_date=from_date.strftime("%Y-%m-%d"),
            to_date=date.strftime("%Y-%m-%d %H:%M:%S")
        )
        time.sleep(0.25) # to avoid rate limits

        if resp.get("status") != "success":
            print(resp.get("data"))
            time.sleep(5)
            return None

        d = resp["data"]
        df = pd.DataFrame({
            "Datetime": pd.to_datetime(d["timestamp"], unit='s', utc=True),
            "Open": d["open"],
            "High": d["high"],
            "Low": d["low"],
            "Close": d["close"],
            "Volume": d["volume"]
        })

        df["dsecurityid"] = symbol_id;


        # Indicators
        df["SMA20"] = df["Close"].rolling(20).mean()
        df["SMA200"] = df["Close"].rolling(200).mean()

        hl = df["High"] - df["Low"]
        hc = abs(df["High"] - df["Close"].shift())
        lc = abs(df["Low"] - df["Close"].shift())
        df["ATR"] = pd.concat([hl, hc, lc], axis=1).max(axis=1).rolling(14).mean()
        atr_mean = df["ATR"].mean()

        # Convert timezone
        df["Datetime"] = df["Datetime"].dt.tz_convert("Asia/Kolkata")

        # Get last candle <= given date
        df_filtered = df[df["Datetime"] <= date.astimezone()]
        if df_filtered.empty:
            return None

        last_candle = df_filtered.tail(1).iloc[0]
        close = float(last_candle["Close"])
        open_ = float(last_candle["Open"])
        sma20 = float(last_candle["SMA20"])
        sma200 = float(last_candle["SMA200"])
        atr = float(last_candle["ATR"])

        # Distance from SMA20
        near_sma = abs(close - sma20) <= (0.005 * close)  # within 0.5%

        # SMA20 trend check
        last_n = df["SMA20"].iloc[-3:].tolist()
        is_sma20_rising = all(x < y for x, y in zip(last_n, last_n[1:]))
        is_sma20_falling = all(x > y for x, y in zip(last_n, last_n[1:]))

        # Volume analysis
        last6 = df.iloc[-6:]
        avg_vol = last6["Volume"].mean()
        strong_bearish_vol = any(
            (last6["Close"] < last6["Open"]) & (last6["Volume"] > avg_vol)
        )
        strong_bullish_vol = any(
            (last6["Close"] > last6["Open"]) & (last6["Volume"] > avg_vol)
        )

        # Trend conditions
        isBearish = (
            (close < sma20)
            and (sma20 < sma200)
            and (atr > 0.5)
            and is_sma20_falling
            and near_sma
            and strong_bearish_vol
        )

        isBullish = (
            (close > sma20)
            and (sma20 > sma200)
            and (atr > 0.5)
            and is_sma20_rising
            and near_sma
            and strong_bullish_vol
        )

        # Stoploss + Target logic
        if len(df_filtered) >= 2:
            prev_candle = df_filtered.iloc[-2]
        else:
            prev_candle = last_candle

        if isBullish:
            signal = "bullish"
            stoploss = float(prev_candle["Low"])
            target = round(close + (2 * atr), 2)
        elif isBearish:
            signal = "bearish"
            stoploss = float(prev_candle["High"])
            target = round(close - (2 * atr), 2)
        else:
            signal = None
            stoploss = None
            target = None

        # Price Structure (HH / LL)
        last_highs = df["High"].iloc[-3:].tolist()
        last_lows = df["Low"].iloc[-3:].tolist()
        is_higher_highs = all(x < y for x, y in zip(last_highs, last_highs[1:]))
        is_lower_lows = all(x > y for x, y in zip(last_lows, last_lows[1:]))

        # Enhanced Trend Strength
        if isBullish:
            if is_sma20_rising and strong_bullish_vol and is_higher_highs:
                trend_strength = "strong_bullish"
            elif is_sma20_rising and (strong_bullish_vol or is_higher_highs):
                trend_strength = "moderate_bullish"
            else:
                trend_strength = "weak_bullish"

        elif isBearish:
            if is_sma20_falling and strong_bearish_vol and is_lower_lows:
                trend_strength = "strong_bearish"
            elif is_sma20_falling and (strong_bearish_vol or is_lower_lows):
                trend_strength = "moderate_bearish"
            else:
                trend_strength = "weak_bearish"
        else:
            trend_strength = "neutral"
        
        tradeStatus = "ready" if signal and ("strong" in trend_strength or "moderate" in trend_strength) else "not_ready"

        # Final candle dict
        candle_data = last_candle.to_dict()
        candle_data.update({
            "symbol": symbol,
            "signal": signal,
            "stoploss": stoploss,
            "target": target,
            "tradeStatus": tradeStatus,
            "atr_mean": round(float(atr_mean), 2),
            "avg_vol": round(float(avg_vol), 2),
            "is_sma20_rising": bool(is_sma20_rising),
            "is_sma20_falling": bool(is_sma20_falling),
            "strong_bullish_vol": bool(strong_bullish_vol),
            "strong_bearish_vol": bool(strong_bearish_vol),
            "near_sma": bool(near_sma),
            "isBullish": bool(isBullish),
            "isBearish": bool(isBearish),
            "is_higher_highs": bool(is_higher_highs),
            "is_lower_lows": bool(is_lower_lows),
            "trend_strength": trend_strength
        })

        return candle_data



    
    def fetch_dhan_data(self, symbol_id, date):
        # Parse the given date if it's a string (e.g., "2025-10-29")
        if isinstance(date, str):
            date = datetime.strptime(date, "%Y-%m-%d")

        # Calculate 5 days back
        from_date = date - timedelta(days=5)

        # Call Dhan API
        resp = self.client.intraday_minute_data(
            security_id=symbol_id,
            exchange_segment=dhanhq.NSE,
            instrument_type="EQUITY",
            interval=5,
            from_date=from_date.strftime("%Y-%m-%d"),
            to_date=date.strftime("%Y-%m-%d")
        )

        if resp.get("status") != "success":
            print(resp["data"])
            return None
        d = resp["data"]
        df = pd.DataFrame({
            "Datetime": pd.to_datetime(d["timestamp"], unit='s', utc=True),
            "Open": d["open"],
            "High": d["high"],
            "Low": d["low"],
            "Close": d["close"],
            "Volume": d["volume"]
        })
        df["SMA20"] = df["Close"].rolling(20).mean()
        df["SMA200"] = df["Close"].rolling(200).mean()
        hl = df["High"] - df["Low"]
        hc = abs(df["High"] - df["Close"].shift())
        lc = abs(df["Low"] - df["Close"].shift())
        df["ATR"] = pd.concat([hl, hc, lc], axis=1).max(axis=1).rolling(14).mean()
        #df.dropna(inplace=True)
        return df

    # --- Example: Fetch 5-min candles ---
    def fetch_5min_candles(self, security_id: str, from_date: str, to_date: str):
        """Fetch 5-min candle data from Dhan"""
        url = f"{self.base_url}/market/v1/quotes/intraday-candle"
        params = {
            "securityId": security_id,
            "interval": "5MIN",
            "fromDate": from_date,
            "toDate": to_date
        }

        try:
            resp = requests.get(url, headers=self.headers, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            logger.info(f"Fetched 5-min candles for {security_id} ({len(data.get('data', []))} candles)")
            return data
        except requests.RequestException as e:
            logger.exception(f"Error fetching candles for {security_id}: {e}")
            return None

    # --- Example: Place an Order ---
    def place_order(self, symbol: str, side: str, quantity: int, price: float):
        """Place a simple order on Dhan"""
        url = f"{self.base_url}/orders"
        payload = {
            "symbol": symbol,
            "transactionType": side,  # "BUY" or "SELL"
            "orderType": "MARKET",
            "quantity": quantity,
            "price": price
        }

        try:
            resp = requests.post(url, headers=self.headers, json=payload, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            logger.info(f"✅ Order placed: {symbol} | {side} | {quantity}")
            return data
        except requests.RequestException as e:
            logger.exception(f"❌ Order placement failed for {symbol}: {e}")
            return None

