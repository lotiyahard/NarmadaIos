# task.py

from services.dhan_service import DhanService
from services.stock_service import StockService
from services.scan_service import ScanService
from utils.logger import logger
from utils.patterns import is_bullish_candle, is_bearish_candle
from services.setup_service import save_setups_to_mongo, fetch_setups_from_mongo
import pandas as pd
import pytz
from datetime import datetime


def fetch_setups():
    """Fetch setups for all stocks in the database."""
    dhanService = DhanService()
    stockService = StockService()
    scanService =  ScanService()

    stocks = stockService.get_stocks()
    logger.info(f"🟢 Found {len(stocks)} stocks to process")

    # Filter stock that already been start for the day
    # Step 1: Fetch all documents already processed for this date
    query =  { "date": "2025-11-11", "tradeStatus": { "$ne": "not_ready" } }
    documents = fetch_setups_from_mongo(query)

    processed_stocks = {doc["symbol"] for doc in documents}
    logger.info(f"✅ Found {len(processed_stocks)} documents for {query['date']}")


    # Step 2: Exclude already processed stocks
    stocks_to_process = [s for s in stocks if s["UNDERLYING_SYMBOL"] not in processed_stocks]

    logger.info(f"🧹 Skipping {len(processed_stocks)} stocks already processed")
    logger.info(f"🚀 {len(stocks_to_process)} stocks left to process")

    current_time = scanService.get_next_scan_time()
    logger.info(f"✅ Process for {current_time}")

    for stock in stocks_to_process:
        symbol = stock.get("UNDERLYING_SYMBOL")
        sec_id = stock.get("SECURITY_ID")

        if not symbol or not sec_id:
            logger.warning(f"⚠️ Skipping stock with missing fields: {stock}")
            continue

        try:
            ist = pytz.timezone("Asia/Kolkata")
            current_time = datetime.now(ist)
            #current_time = "2025-11-07 9:20:00"  # For testing purpose

            #setups = process_symbol(dhanService, symbol, sec_id, "2025-10-30")
            #logger.info(f"✅ Process {symbol} {current_time}")
            setups = process_stock(dhanService, symbol, sec_id, current_time)
            #logger.info(f"✅ Processed {symbol}")
            if setups:
                save_setups_to_mongo(setups)
                #print(f"✅ Saved {len(setups)} setups for {symbol}")


        except Exception as e:
            logger.exception(f"❌ Failed to process {symbol}: {e}")



def process_stock(dhanService, symbol, sec_id, date):
    df = dhanService.fetch_candles(sec_id, symbol, date)
    return df
   


def process_symbol(dhanService, symbol, sec_id, date):
    df = dhanService.fetch_dhan_data(sec_id, date)

    if df is None or df.empty:
        logger.info(f"No data for {symbol}")
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
                "status": "ready",
                "stock": symbol,
                "dsecurityid" :sec_id,
                "timestamp": ts_str,
                "price": row["Close"],
                "high": row["High"],
                "low": row["Low"],
                "change": row["Close"] - df_up_to["Close"].iloc[-2],
                "close": row["Close"],
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
                "status": "ready",
                "stock": symbol,
                "dsecurityid" :sec_id,
                "timestamp":  ts_str,
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
                "direction": "BEARISH",
                "isstar": False,
                "targetachieved": targetachieved,
                "stoplosshit": stoplosshit
            })
    return setups


if __name__ == "__main__":
    fetch_setups()
