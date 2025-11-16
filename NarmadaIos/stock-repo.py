from db import db

stocks = db["stocks"]

nifty50_symbols = [
    "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "HINDUNILVR.NS",
    "ICICIBANK.NS", "KOTAKBANK.NS", "SBIN.NS", "BHARTIARTL.NS", "ITC.NS",
    "HCLTECH.NS", "ASIANPAINT.NS", "LT.NS", "AXISBANK.NS", "MARUTI.NS",
    "SUNPHARMA.NS", "TECHM.NS", "WIPRO.NS", "BAJFINANCE.NS", "BAJAJFINSV.NS",
    "DIVISLAB.NS", "TITAN.NS", "ULTRACEMCO.NS", "NESTLEIND.NS", "ONGC.NS",
    "POWERGRID.NS", "NTPC.NS", "HDFCLIFE.NS", "COALINDIA.NS", "BRITANNIA.NS",
    "JSWSTEEL.NS", "GRASIM.NS", "ADANIPORTS.NS", "TATASTEEL.NS", "BPCL.NS",
    "HINDALCO.NS", "EICHERMOT.NS", "HEROMOTOCO.NS", "M&M.NS", "DRREDDY.NS",
    "SHREECEM.NS", "SBILIFE.NS", "ICICIPRULI.NS", "CIPLA.NS", "TATAMOTORS.NS",
    "INDUSINDBK.NS", "HDFCAMC.NS", "VEDL.NS", "HINDPETRO.NS", "ADANIGREEN.NS",
    "ADANIPOWER.NS", "AMBUJACEM.NS", "BAJAJHLDNG.NS", "BANKBARODA.NS", "BEL.NS",
    "CANBK.NS", "CHOLAFIN.NS", "DABUR.NS", "DLF.NS", "EXIDEIND.NS", "GAIL.NS",
    "GLENMARK.NS", "HAVELLS.NS", "HINDZINC.NS", "ICICIGI.NS", "IDFCFIRSTB.NS",
    "INDHOTEL.NS", "INDIACEM.NS", "INFIBEAM.NS", "IOC.NS", "JUBLFOOD.NS", "KNRCON.NS",
    "LICHSGFIN.NS", "MCDHOLDING.NS", "LTIM.NS", "MSUMI.NS", "MPHASIS.NS",
    "NATIONALUM.NS", "NMDC.NS", "OIL.NS", "PEL.NS", "PIDILITIND.NS", "PNB.NS", "RECLTD.NS",
    "SAIL.NS", "SBICARD.NS", "SHRIRAMFIN.NS", "SIEMENS.NS", "SRF.NS", "SUNTV.NS", "TATACONSUM.NS",
    "TATACOMM.NS", "TATAPOWER.NS", "UPL.NS", "VBL.NS", "ZEEL.NS", "YATHARTH.NS","KEI"
]


def get_stocks():
    """
    Fetches all stock symbols from the 'stocks' collection in MongoDB.
    Returns a list of stock symbols.
    """
    try:
        stocks = list(stocks.find())
        print(f"Total stocks: {len(stocks)}")

        # Filter only Nifty50
        nifty50_stocks = [
            stock for stock in stocks
            if stock.get("UNDERLYING_SYMBOL", "").upper() in [s.replace(".NS","") for s in nifty50_symbols]
        ]
        print(f"nifty50_stocks: {len(nifty50_stocks)}")

        return nifty50_stocks

    except Exception as e:
        print(f"❌ Error fetching stocks from MongoDB: {e}")
        return []