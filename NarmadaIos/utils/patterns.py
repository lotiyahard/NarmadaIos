import pandas as pd

def detect_bullish_engulfing(df, volume_ma_period=20):
    """
    Detect Bullish Engulfing patterns in OHLCV DataFrame.
    df must have columns: ['Open', 'High', 'Low', 'Close', 'Volume']
    Returns DataFrame with boolean column 'BullishEngulfing'
    """
    df = df.copy()
    
    # Fix MultiIndex columns if present
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df["BullishEngulfing"] = False
    
    # Optional: compute volume moving average
    df["VolMA"] = df["Volume"].rolling(volume_ma_period).mean()
    
    for i in range(1, len(df)):
        prev = df.iloc[i-1]
        curr = df.iloc[i]
        
        # Ensure scalar values
        prev_open = float(prev["Open"])
        prev_close = float(prev["Close"])
        curr_open = float(curr["Open"])
        curr_close = float(curr["Close"])
        curr_volume = float(curr["Volume"])
        vol_ma = float(df["VolMA"].iloc[i])
        
        # Check Bullish Engulfing conditions
        bearish_prev = prev_close < prev_open
        bullish_curr = curr_close > curr_open
        engulfing = (curr_open < prev_close) and (curr_close > prev_open)
        volume_ok = curr_volume > vol_ma  # optional
        
        if bearish_prev and bullish_curr and engulfing and volume_ok:
            df.at[df.index[i], "BullishEngulfing"] = True
            
    return df

# --- Candle logic ---
def is_bullish_candle(row, df_up_to_row):
    close, open_, sma20, sma200, atr = (
        float(row["Close"]),
        float(row["Open"]),
        float(row["SMA20"]),
        float(row["SMA200"]),
        float(row["ATR"]),
    )
    sma20_up = is_sma20_rising(df_up_to_row, n=5)
    near_sma = abs(close - sma20) <= 5
    vol_ma = df_up_to_row["Volume"].tail(20).mean()
    strong_bull_vol = (close > open_) and (row["Volume"] > vol_ma)
    return (
        (close > sma20)
        and (sma20 > sma200)
        and (atr > 0.5)
        and (sma20_up or near_sma)
        and strong_bull_vol
    )
def is_bearish_candle(row, df_up_to_row):
    close = float(row["Close"])
    sma20 = float(row["SMA20"])
    sma200 = float(row["SMA200"])
    atr = float(row["ATR"])

    sma20_down = is_sma20_falling(df_up_to_row, n=5)
    near_sma = abs(close - sma20) <= 5

    last6 = df_up_to_row.iloc[-6:]
    avg_vol = last6["Volume"].mean()
    bearish_volume = any((last6["Close"] < last6["Open"]) & (last6["Volume"] > avg_vol))

    result = (close < sma20) and (sma20 < sma200) and (atr > 0.5) and sma20_down and near_sma and bearish_volume
    # print(
      #      f"Close={close:.2f}, SMA20={sma20:.2f}, SMA200={sma200:.2f}, ATR={atr:.2f}, "
       #     f"SMA20↓={sma20_down}, NearSMA={near_sma}, BearVol={bearish_volume} → Bearish={result}"
        #)
    return result

def is_sma20_rising(df, n=3):
    last_n = df["SMA20"].iloc[-n:].tolist()
    is_rising = all(x < y for x, y in zip(last_n, last_n[1:]))
    return is_rising

def is_sma20_falling(df, n=3):
    last_n = df["SMA20"].iloc[-n:].tolist()
    is_falling = all(x > y for x, y in zip(last_n, last_n[1:]))
    return is_falling

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

nifty50_symbols2 = [ 
    "ADANIPORTS.NS"
]


def is_nifty50_stock(symbol: str) -> bool:
    """
    Check if a stock symbol belongs to Nifty 50.
    symbol: stock symbol (like "RELIANCE", "TCS", "INFY")
    Returns True if in Nifty 50, else False.
    """
    # Normalize the symbol
    symbol_clean = symbol.replace(".NS", "").upper()
    return symbol_clean in nifty50_symbols