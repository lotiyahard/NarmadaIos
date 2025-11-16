from config.db_config import db
from utils.logger import logger
import pytz
from datetime import datetime, timedelta, time

scans = db["scandetails"]
IST = pytz.timezone("Asia/Kolkata")


class ScanService:
    """Handles all ScanService API interactions """

    def __init__(self):
        logger.info("✅ scandetails initialized")


    def save_scan_time(self, dt):
        """Save last scan time (overwrite)."""
        scans.update_one(
            {"_id": "intraday-scan"},
            {"$set": {"lastscanat": dt}},
            upsert=True
        )

    def get_next_scan_time(self):
        """Return next scan time (always +5 min, starting from 9:15)."""

        today = datetime.now(IST).date()
        today_str = "2025-11-07"
        today_date = datetime.strptime(today_str, "%Y-%m-%d").date()

        initial_time = IST.localize(datetime.combine(today_date, time(9, 20)))
        doc = scans.find_one({"_id": "intraday-scan"})

        # ✅ FIRST TIME OF THE DAY
        if not doc:
            self.save_scan_time(initial_time)
            return initial_time

        last_scan = doc["lastscanat"]
        
        # If datetime is naive → assume stored in UTC, then convert to IST
        if last_scan.tzinfo is None:
            last_scan = pytz.utc.localize(last_scan).astimezone(IST)
        else:
            # If already timezone-aware → convert to IST directly
            last_scan = last_scan.astimezone(IST)


        # ✅ IF date changed → market new day → reset to 9:20
        #if last_scan.date() != today:
         #   self.save_scan_time(initial_time)
         #   return initial_time

        # ✅ NORMAL FLOW → ADD 5 MINUTES
        next_scan = last_scan + timedelta(minutes=5)
        self.save_scan_time(next_scan)

        return next_scan