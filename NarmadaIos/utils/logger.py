import logging
import os

# Create logs directory if not exists
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Configure logger
logging.basicConfig(
    level=logging.INFO,  # Can be changed to DEBUG for more details
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(f"{LOG_DIR}/app.log"),
        logging.StreamHandler()
    ]
)

# Create a named logger for the app
logger = logging.getLogger("TradingApp")
