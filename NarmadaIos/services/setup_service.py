from config.db_config import db
from utils.logger import logger
import pytz



collection = db["setups"]

class SetupService:
    """Handles all setupService API interactions """

    def __init__(self):
          logger.info("✅ setup initialized")


def fetch_setups_from_mongo(query):
    """
    Fetch setups from MongoDB based on the provided query.
    Returns a list of setups.
    """
    try:
        setups = list(collection.find(query))
        logger.info(f"🟢 Fetched {len(setups)} setups from MongoDB")
        return setups

    except Exception as e:
        print(f"❌ Error fetching setups from MongoDB: {e}")
        logger.exception(f"❌ Error fetching setups from MongoDB: {e}")
        return []


def save_setups_to_mongo(candle_data):
    """
    - One document per stock per day
    - Candles stored inside `candles` array
    - First candle's target/stoploss/stoplosshit saved once (if not already present)
    - Last candle's target/targetachieved/stoplosshit updated each time
    - Prevent duplicate timestamps
    """
    if not candle_data:
        return
    if candle_data["tradeStatus"] != "ready":
        #logger.info(f"❌ No setups generated for {candle_data['symbol']}")
        return
    
    logger.info(f"setups generated for {candle_data['symbol']} {candle_data['Datetime']}")
    
    # Convert candle datetime to IST
    ist = pytz.timezone("Asia/Kolkata")
    ist_dt = candle_data["Datetime"].astimezone(ist)
    candle_data["Datetime"] = ist_dt
    # Extract only the date (YYYY-MM-DD) for uniqueness
    candle_date = ist_dt.date()
    
        # Outer + candle info
    outer_data = {
        "symbol": candle_data["symbol"],
        "dSecurityId" : candle_data["dsecurityid"],
        "signal": candle_data["signal"],
        "stoploss": candle_data["stoploss"],
        "target": candle_data["target"],
        "tradeStatus": candle_data["tradeStatus"],
        "date": str(candle_date),  # use only date for uniqueness
        "Datetime": candle_data["Datetime"]  # main timestamp
    }

    # Update or append candle_data in array, unique by symbol + Datetime + signal
    collection.update_one(
        {
            "symbol": outer_data["symbol"],
            "Datetime": outer_data["date"],
            "signal": outer_data["signal"]   # ensure uniqueness per signal
        },
        {
            "$set": outer_data,                # update main fields
            "$push": {"candleData": candle_data}  # append new candle
        },
        upsert=True  # create if not exists
    )

    #print("✅ Candle added/updated uniquely based on symbol + date + signal")

def save_setups_to_mongo2(setups):
    """
    - One document per stock per day
    - Candles stored inside `candles` array
    - First candle's target/stoploss/stoplosshit saved once (if not already present)
    - Last candle's target/targetachieved/stoplosshit updated each time
    - Prevent duplicate timestamps
    """
    if not setups:
        return

    stock = setups[0]["stock"]
    date_str = setups[0]["timestamp"].split(" ")[0]
    price= setups[0]["price"]
    status= setups[0]["status"]
    dSecurityId = setups[0]["dsecurityid"]
    first_candle = setups[0]
    last_candle = setups[-1]

    for setup in setups:
        timestamp = setup["timestamp"]
        candle = {
            "timestamp": timestamp,
            "price": setup["price"],
            "high": setup["high"],
            "low": setup["low"],
            "change": setup["change"],
            "sma20": setup["sma20"],
            "sma200": setup["sma200"],
            "atr": setup["atr"],
            "target": setup["target"],
            "stoploss": setup["stoploss"],
            "direction": setup["direction"],
            "isstar": setup["isstar"],
            "targetachieved": setup["targetachieved"],
            "stoplosshit": setup["stoplosshit"],
        }

        # Push only if timestamp not present
        collection.update_one(
            {"stock": stock, "status":status, "date": date_str, "candles.timestamp": {"$ne": timestamp}},
            {
                "$push": {"candles": candle},
                "$setOnInsert": {"stock": stock, "dSecurityId":dSecurityId,  "date": date_str, "price":price,}
            },
            upsert=True
        )

    # Update first_* fields only if not already present
    collection.update_one(
        {"stock": stock, "date": date_str, "first_target": {"$exists": False}},
        {
            "$set": {
                "fprice": first_candle["price"],
                "first_target": first_candle["target"],
                "first_stoploss": first_candle["stoploss"],
                "ftradetime": first_candle["timestamp"],
               
            }
        }
    )

    # Always update last_* fields with most recent candle
    collection.update_one(
        {"stock": stock, "date": date_str},
        {
            "$set": {
                "trailing_stoploss": last_candle["stoploss"],
                "trailing_target": last_candle["target"],
                "targetachieved": last_candle["targetachieved"],
                "ltradetime": last_candle["timestamp"],
                "stoplosshit": last_candle["stoplosshit"]
            }
        }
    )