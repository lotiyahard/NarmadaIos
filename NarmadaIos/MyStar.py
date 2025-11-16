from dhanhq import dhanhq
import pandas as pd
import pytz
from datetime import datetime

from patterns import is_bullish_candle, is_bearish_candle
from mongoTest import save_setups_to_mongo, get_stocks

CLIENT_ID = "1104563589"
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzYxODkxMjIzLCJpYXQiOjE3NjE4MDQ4MjMsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA0NTYzNTg5In0.bJNLGMozQXKAMixiLLKsdUkL8DXiBrJgW8L8399kZcnUrutJRZKMXEKRCkrcUXVR1XLVfjcc9nWkUbW57NvFdg"

# Dhan client
client_dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)

from datetime import datetime, timedelta
from dhan_repo import get_token

def fetch_dhan_data(symbol_id, date):
    get_token
    # Parse the given date if it's a string (e.g., "2025-10-29")
    if isinstance(date, str):
        date = datetime.strptime(date, "%Y-%m-%d")

    # Calculate 5 days back
    from_date = date - timedelta(days=5)

    # Call Dhan API
    resp = client_dhan.intraday_minute_data(
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

def process_symbol(symbol, sec_id, date):
    df = fetch_dhan_data(sec_id, date)

    if df is None or df.empty:
        print(f"No data for {symbol}")
        return
    
    # 🧠 Fix: Handle timestamp or Datetime column safely
    if "Datetime" in df.columns:
        if pd.api.types.is_numeric_dtype(df["Datetime"]):
            # convert numeric timestamp → datetime
            unit = "ms" if df["Datetime"].max() > 1e12 else "s"
            df["Datetime"] = pd.to_datetime(df["Datetime"], unit=unit, utc=True)
        elif not pd.api.types.is_datetime64_any_dtype(df["Datetime"]):
            # convert string datetime → datetime
            df["Datetime"] = pd.to_datetime(df["Datetime"], utc=True)

        # set index
        df.set_index("Datetime", inplace=True)

    # Convert string to datetime if needed
    if isinstance(date, str):
        date = datetime.strptime(date, "%Y-%m-%d")

    # Create start and end time strings
    start_str = date.strftime("%Y-%m-%d") + " 09:15"
    end_str   = date.strftime("%Y-%m-%d") + " 15:30"

    # Localize to IST and convert to UTC
    ist = pytz.timezone("Asia/Kolkata")
    start_dt = ist.localize(datetime.strptime(start_str, "%Y-%m-%d %H:%M")).astimezone(pytz.UTC)
    end_dt   = ist.localize(datetime.strptime(end_str, "%Y-%m-%d %H:%M")).astimezone(pytz.UTC)

    df_range = df[(df.index >= start_dt) & (df.index <= end_dt)]
    if df_range.empty:
        return

    setups = []
    for idx, row in df.iterrows():
        df_up_to = df_range.loc[:idx]
        ts_str = idx.tz_convert(ist).strftime("%Y-%m-%d %H:%M")
        is_first = idx == df_range.index[0]


        if is_bullish_candle(row, df_up_to):
            prev_low = df_up_to["Low"].iloc[-2] if len(df_up_to) >= 2 else row["Low"]
            if is_first:
                target, stoploss = "", ""
            else:
                target = round(row["Close"] + (2 * row["ATR"]), 2)
                stoploss = prev_low
                targetachieved = "YES" if row["Close"] > target else ""
                stoplosshit = "YES" if row["Close"] < stoploss else ""

            setups.append({
                "stock": symbol,
                "timestamp": ts_str,
                "price": row["Close"],
                "high": row["High"],
                "low": row["Low"],
                "close": row["Close"],
                "change": row["Close"] - df_up_to["Close"].iloc[-2],
                "sma20": row["SMA20"],
                "sma200": row["SMA200"],
                "atr": row["ATR"],
                "target": target,
                "stoploss": stoploss,
                "direction": "BULLISH",
                "isstar": False,
                "targetachieved": targetachieved,
                "stoplosshit": stoplosshit
            })


        elif is_bearish_candle(row, df_up_to):
            prev_high = df_up_to["High"].iloc[-2] if len(df_up_to) >= 2 else row["High"]
            if is_first:
                target, stoploss = "", ""
            else:
                target = round(row["Close"] - (2 * row["ATR"]), 2)
                stoploss = prev_high
                targetachieved = "YES" if row["Close"] < target else ""
                stoplosshit = "YES" if row["Close"] > stoploss else ""

            setups.append({
                "stock": symbol,
                "timestamp":  ts_str,
                "price": row["Close"],
                "high": row["High"],
                "low": row["Low"],
                "change": row["Close"] - df_up_to["Close"].iloc[-2],
                "sma20": row["SMA20"],
                "sma200": row["SMA200"],
                "atr": row["ATR"],
                "target": target,
                "stoploss": stoploss,
                "direction": "BEARISH",
                "isstar": False,
                "targetachieved": targetachieved,
                "stoplosshit": stoplosshit
            })

    if setups:
        save_setups_to_mongo(setups)
        print(f"✅ Saved {len(setups)} setups for {symbol}")

# Example run
for stock in get_stocks():
    symbol = stock.get("UNDERLYING_SYMBOL")
    sec_id = stock.get("SECURITY_ID")
    if symbol and sec_id:
        process_symbol(symbol, sec_id, "2025-10-30")
        print(f"Processed {symbol}")



