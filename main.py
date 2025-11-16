# main.py

import time
from datetime import datetime, timedelta, time as dtime
import pytz

import schedule

from utils.logger import logger
from config.db_config import db
from services.dhan_service import DhanService
from services.setup_service import fetch_setups_from_mongo

from tasks.task import fetch_setups

# --- Utility Functions ---

def is_market_open():
    """Check if market is open (Mon–Fri, 9:15–15:25)."""
    now = datetime.now()
    market_start = dtime(9, 15)
    market_end = dtime(15, 25)
    is_open = now.weekday() < 5 and market_start <= now.time() <= market_end
    return is_open


# --- Trading Logic Placeholders ---

def check_for_setups_and_trade():
    """Fetch setups from DB and trade if conditions met."""
    if not is_market_open():
        logger.info("⏳ Market closed — skipping this cycle.")
        #return

    dhan = DhanService()

    logger.info("🔍 Checking for setups...")

    ist = pytz.timezone("Asia/Kolkata")
    current_time = datetime.now(ist)
    today_str = current_time.strftime("%Y-%m-%d")
    query = { "date": today_str, "tradeStatus": "ready" }
    setups = fetch_setups_from_mongo(query)
    for setup in setups:
        symbol = setup["symbol"]

        # Skip if already in progress
        existing_trade = db["trades"].find_one({"symbol": symbol, "status": "in_progress"})
        if existing_trade:
            logger.info(f"⛔ Trade already in progress for {symbol}, skipping.")
            continue

        # Example: Place order (stub)
        logger.info(f"🚀 Starting trade for {symbol}")
        #order = dhan.place_order(symbol, "BUY", 1, 0.0)
        first_candle = setup["candleData"][0]
        first_close = float(first_candle["Close"])
        from datetime import timezone, timedelta

        IST = timezone(timedelta(hours=5, minutes=30))
        entry_dt_ist = setup["Datetime"].astimezone(IST)
       
        entryTimeUTC = setup["Datetime"].replace(tzinfo=timezone.utc)
        entryTimeIST = entryTimeUTC.astimezone(IST)

        print(entryTimeIST) 
       
        #if order:
        db["trades"].insert_one({
            "price": first_close,
            "signal": setup["signal"],
            "dsecurityid" :setup["dSecurityId"],
            "target": setup["target"],
            "stoploss": setup["stoploss"],
            "symbol": symbol,
            "fentry_time": entryTimeIST.strftime("%Y-%m-%d %H:%M"),
            "entry_time": entryTimeIST.strftime("%Y-%m-%d %H:%M"),
            "exit_time": None,
            "status": "in_progress",
        })
        db["setups"].update_one({"_id": setup["_id"]}, {"$set": {"tradeStatus": "traded"}})
        logger.info(f"✅ Trade started for {symbol}")
        #else:
            #logger.error(f"❌ Failed to place order for {symbol}")


def monitor_open_trades():
    """Check ongoing trades for target/stoploss."""
    if not is_market_open():
        logger.info("⏳ Market closed — skipping this cycle.")
        #return

    dhan = DhanService()

    trades = db["trades"].find({"status": "in_progress"})
    for trade in trades:
        symbol = trade["symbol"]
        dsecurityid = trade["dsecurityid"]
        entryTime = trade["entry_time"]  # e.g. "2025-10-30 11:05"

        if isinstance(entryTime, str):
            entry_dt = datetime.strptime(entryTime, "%Y-%m-%d %H:%M")
        else:
            entry_dt = entryTime

        to_date = entry_dt + timedelta(minutes=5)
        entry_dt = entry_dt - timedelta(days=5)
        logger.info(f"📊 Checking trade: {symbol} from {entry_dt} to {to_date}")

        # Fetch live price
        #live_data = dhan.fetch_5min_candles(trade["order_details"]["securityId"],
         #                                   datetime.now().strftime("%Y-%m-%d"),
          #                                  datetime.now().strftime("%Y-%m-%d"))
        #if not live_data:
            #continue TODO its temp
        live_data = dhan.fetch_candles(dsecurityid, symbol, to_date)

        # Safely get values

        # signal lost if not ready
        if live_data.get("tradeStatus") != "ready":
            pl = "profit" if trade.get("current_price") and trade["current_price"] > trade["price"] else "loss"
            logger.info("Signal lost or stoploss hit, closing trade.")
            db["trades"].update_one(
                {"_id": trade["_id"]},
                {"$set": {"current_price": trade.get("stoploss"), "status": "closed", "exit_reason": "Signal lost sl hit", "pnl": pl}}
            )
            continue

        current_price = live_data["Close"]
        target = live_data["target"]
        stoploss = live_data["stoploss"]
        signal = trade.get("signal")
        symbol = trade.get("symbol")

        # Convert to float only if not None
        if current_price is not None:
            current_price = float(current_price)
        else:
            current_price = 0  # or handle appropriately

        if stoploss is not None:
            stoploss = float(stoploss)
        else:
            stoploss = 0  # or handle appropriately

        # target can also be float if needed
        if target is not None:
            target = float(target)
        else:
            target = 0


        # Update for next time
        db["trades"].update_one(
                    {"_id": trade["_id"]},
                    {"$set": {"entry_time": to_date}}
                )

        # -------------------------------
        # ✅ Bullish Trade Logic
        # -------------------------------
        if signal == "bullish":

            pl = "profit" if trade.get("current_price") and trade["current_price"] > trade["price"] else "loss"

            if current_price <= stoploss:
                logger.info(f"🛑 Stoploss hit for {symbol} (bullish), closing tradew ith Price: {current_price}")
               
                db["trades"].update_one(
                    {"_id": trade["_id"]},
                    {"$set": {"current_price":current_price, "status": "closed", "exit_reason": "stoploss_hit", "pnl": pl}}
                )

            else:
                logger.info(f"🔄 Bullish trade for {symbol} active. Price: {current_price}")
                db["trades"].update_one(
                    {"_id": trade["_id"]},
                    {"$set": {"current_price":current_price, "target": live_data["target"], "stoploss": live_data["stoploss"], "pnl": pl}}
                )

        # -------------------------------
        # ✅ Bearish Trade Logic
        # -------------------------------
        elif signal == "bearish":
            pl = "profit" if trade.get("current_price") and trade["current_price"] < trade["price"] else "loss"

            if current_price >= stoploss:
                logger.info(f"🛑 Stoploss hit for {symbol} (bearish), closing trade with Price: {current_price}")
                db["trades"].update_one(
                    {"_id": trade["_id"]},
                    {"$set": {"current_price":current_price, "status": "closed", "exit_reason": "stoploss_hit", "pnl": pl}}
                )

            else:
                logger.info(f"🔄 Bearish trade for {symbol} active. Price: {current_price}")
                db["trades"].update_one(
                    {"_id": trade["_id"]},
                    {"$set": {"current_price":current_price, "target": live_data["target"], "stoploss": live_data["stoploss"], "pnl": pl}}
                )

        # if not target or not stoploss then check current candle for tail stop loss and target adjustments



def close_trades_before_market_close():
    """Exit all trades 5 min before close."""
    now = datetime.now().time()
    if now >= dtime(15, 20):
        logger.info("🏁 Closing all open trades before market close...")
        #db["trades"].update_many({"status": "in_progress"}, {"$set": {"status": "closed", "exit_reason": "EOD"}})
        logger.info("✅ All trades closed for the day.")


# --- Scheduler Setup ---

# schedule.every(1).minutes.do(fetch_setups)
# schedule.every(1).minutes.do(check_for_setups_and_trade)
# schedule.every(1).minutes.do(monitor_open_trades)
# schedule.every(100).minutes.do(close_trades_before_market_close)

# logger.info("🚀 Scheduler started... (Ctrl+C to stop)")

# --- Main Loop ---
# while True:
#     try:
#         schedule.run_pending()
#         time.sleep(1)
#     except KeyboardInterrupt:
#         logger.info("🛑 Scheduler stopped by user.")
#         break
#     except Exception as e:
#         logger.exception(f"Unexpected error: {e}")
#         time.sleep(5)


import time
import logging

logger = logging.getLogger(__name__)

def run_chain():
    try:
        logger.info("▶️ Starting fetch_setups()")
        fetch_setups()
        logger.info("✅ Completed fetch_setups()")

        logger.info("▶️ Starting check_for_setups_and_trade()")
        check_for_setups_and_trade()
        logger.info("✅ Completed check_for_setups_and_trade()")

        logger.info("▶️ Starting monitor_open_trades()")
        monitor_open_trades()
        logger.info("✅ Completed monitor_open_trades()")

        #logger.info("▶️ Starting close_trades_before_market_close()")
        #close_trades_before_market_close()
        #logger.info("✅ Completed close_trades_before_market_close()")

    except Exception as e:
        logger.exception(f"❌ Error in job chain: {e}")


logger.info("🚀 Serial Scheduler started... (Ctrl+C to stop)")

while True:
    try:
        start_time = time.time()

        run_chain()  # run them in FIXED ORDER, one after another

        # Ensure 1-minute interval between cycles
        elapsed = time.time() - start_time
        sleep_time = max(0, 60 - elapsed)
        time.sleep(sleep_time)

    except KeyboardInterrupt:
        logger.info("🛑 Scheduler stopped by user.")
        break
    except Exception as e:
        logger.exception(f"Unexpected error in main loop: {e}")
        time.sleep(5)
