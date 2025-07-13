import requests
import time
import pymongo
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient

# MongoDB Connection
MONGODB_URI = "mongodb://localhost:27017/ETH10K5Min"
client = MongoClient(MONGODB_URI)
db = client["ETH10K5Min"]

# Bitget API Endpoint for Klines
BITGET_URL = "https://api.bitget.com/api/v2/spot/market/history-candles"

# Parameters
TOKENS = ["ETHUSDT"]
INTERVAL = "5min"  # Changed to 5min
LIMIT = 200  # Max per request
TOTAL_RECORDS = 10000  # Maintain only the latest XXXX records per token
SLEEP_TIME = 1  # Sleep to avoid rate limits

def fetch_price_data(symbol, end_time, limit):
    """Fetch historical price data within given end time"""
    params = {
        "symbol": symbol,
        "granularity": INTERVAL,
        "endTime": end_time,
        "limit": limit
    }
    try:
        response = requests.get(BITGET_URL, params=params)
        response.raise_for_status()
        data = response.json()["data"]
        return data if isinstance(data, list) else []
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {symbol}: {e}")
        return []

def save_to_mongodb(collection, data, token):
    """Save all items in response to MongoDB and ensure size limit."""
    if not data:
        return

    # Get last 9 closes from DB for this token
    last_9_docs = list(collection.find({"token": token})
                       .sort("timestamp", -1)
                       .limit(9))
    last_9_closes = [doc["close"] for doc in reversed(last_9_docs)]  # oldest to newest

    records = []
    for entry in data:
        timestamp = datetime.fromtimestamp(int(entry[0]) / 1000, timezone.utc)
        close = float(entry[4])
        
        # Compute 10-period average if we have 9 previous closes
        closes_for_avg = last_9_closes + [close] if len(last_9_closes) == 9 else []
        ten_avg = sum(closes_for_avg) / 10 if closes_for_avg else None

        record = {
            "token": token,
            "timestamp": timestamp,
            "open": float(entry[1]),
            "high": float(entry[2]),
            "low": float(entry[3]),
            "close": close,
            "base_volume": float(entry[5]),
            "quote_volume": float(entry[6]),
            "ten_avg": ten_avg,
        }
        records.append(record)

    if records:
        # Avoid duplicates
        existing_timestamps = {
            doc["timestamp"] for doc in collection.find(
                {"token": token, "timestamp": {"$in": [r["timestamp"] for r in records]}},
                {"timestamp": 1}
            )
        }
        filtered_records = [r for r in records if r["timestamp"] not in existing_timestamps]

        if filtered_records:
            collection.insert_many(filtered_records)
            print(f"Saved {len(filtered_records)} new records to {collection.name}")
            delete_oldest_records(collection)
        else:
            print("No new records to save.")

def delete_oldest_records(collection):
    """Ensure only the latest TOTAL_RECORDS are kept."""
    record_count = collection.count_documents({})
    if record_count > TOTAL_RECORDS:
        excess = record_count - TOTAL_RECORDS
        oldest_records = collection.find().sort("timestamp", pymongo.ASCENDING).limit(excess)
        ids_to_delete = [record["_id"] for record in oldest_records]
        collection.delete_many({"_id": {"$in": ids_to_delete}})
        print(f"Deleted {excess} oldest records from {collection.name}")

def live_update():
    """Continuously fetch new data at 5-minute intervals."""
    while True:
        current_time = datetime.now(timezone.utc)
        current_ts = current_time.timestamp()
        # Calculate next 5-minute boundary
        next_5min_ts = ((current_ts // 300) + 1) * 300
        sleep_sec = next_5min_ts - current_ts
        print(f"Sleeping {sleep_sec:.1f} seconds until next 5-minute interval.")
        time.sleep(max(0, sleep_sec))
        
        time.sleep(2)  # Wait for data availability
        
        for token in TOKENS:
            collection = db[f"{token}_10MA_timeseries"]
            # Get previous 5-minute interval's end time
            prev_5min_ts = (datetime.now(timezone.utc).timestamp() // 300) * 300
            prev_5min = datetime.fromtimestamp(prev_5min_ts, timezone.utc)
            end_time = int(prev_5min_ts * 1000)
            
            price_data = fetch_price_data(token, end_time, 1)
            retries = 0
            while not price_data and retries < 3:
                print(f"Retrying {token} at {prev_5min}...")
                time.sleep(2)
                price_data = fetch_price_data(token, end_time, 1)
                retries += 1
            
            if price_data:
                save_to_mongodb(collection, price_data, token)
                print(f"Updated {token} at {prev_5min}")
            else:
                print(f"Failed to update {token} at {prev_5min}")
            
            delete_oldest_records(collection)

if __name__ == "__main__":
    try:
        client.admin.command('ping')
        print("Successfully connected to MongoDB!")
        
        live_update()
    except Exception as e:
        print(f"Error: {e}")