import requests
import pyodbc
from datetime import datetime
from decimal import Decimal

url = "https://api.coingecko.com/api/v3/simple/price"
params = {
    "ids": "bitcoin,ethereum",
    "vs_currencies": "usd",
    "include_market_cap": "true",
    "include_24hr_vol": "true",
    "include_24hr_change": "true"
}

response = requests.get(url, params=params)
data = response.json()

# SQL Server connection
conn = pyodbc.connect(
    "DRIVER={SQL Server};SERVER=mohsen2;DATABASE=CryptoDW_;Trusted_Connection=yes"
)
cursor = conn.cursor()

for coin, info in data.items():
    snapshot_date = datetime.now().date().isoformat()
    coin_symbol = coin
    # Ensure values fit SQL types
    try:
        pu_val = float(info["usd"])
        price_usd = round(pu_val, 8)
    except:
        price_usd = None

    try:
        mc_val = float(info["usd_market_cap"])
        market_cap = round(mc_val, 2)
    except:
        market_cap = None

    try:
        v24_val = float(info["usd_24h_vol"])
        volume_24h = round(v24_val, 2)
    except:
        volume_24h = None

    change_24h = float(info["usd_24h_change"]) if info["usd_24h_change"] is not None else None

    print(f"Prepared for insert: snapshot_date={snapshot_date}, coin_symbol={coin_symbol}, price_usd={price_usd}, market_cap={market_cap}, volume_24h={volume_24h}, change_24h={change_24h}")

    cursor.execute(
        """
        INSERT INTO StgMarketLive (snapshot_date, coin_symbol, price_usd, market_cap, volume_24h, change_24h)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        snapshot_date,
        coin_symbol,
        price_usd,
        market_cap,
        volume_24h,
        change_24h
    )
    print(f"Inserted {coin_symbol} for {snapshot_date}")

conn.commit()
cursor.close()
conn.close()
print("✅ API DATA INSERTED TO SQL SERVER")
