# Crypto Price Data Collector || Bitget API

A Python script that collects and stores cryptocurrency price data from the Bitget API into MongoDB time-series collections. This tool is designed to maintain a rolling window of the latest 3,000 records for specified trading pairs.

## Features

- **Historical Data Fetching**: Initial bulk fetch of up to 3,000 historical candlestick records per token.
- **Time-Series Collections**: Utilizes MongoDB time-series collections for efficient storage and querying.
- **Gap Filling**: Detects and fills missing data between the last recorded entry and the current time.
- **Live Updates**: Continuously fetches new data at the start of each minute.
- **Duplicate Prevention**: Checks for existing timestamps to avoid redundant data.
- **Automatic Cleanup**: Ensures only the latest 3,000 records are retained per token.

## Supported Tokens & Intervals
- **Tokens**: `ETHUSDT`, `ADAUSDT` (configurable).
- **Interval**: 1-minute candlesticks.

## Prerequisites

- Python 3.8+
- MongoDB Atlas cluster (or local MongoDB instance with time-series support).
- Python Libraries: 
  ```bash
  pip install requests pymongo python-dotenv
  ```

## Configuration

1. **MongoDB Connection**:
   - Replace the `MONGODB_URI` in the script with your MongoDB connection string.
   - Format: `mongodb+srv://<username>:<password>@cluster0.iittg.mongodb.net/alert3`.

2. **Script Parameters** (Modify in Code):
   ```python
   TOKENS = ["ETHUSDT", "ADAUSDT"]  # Add/remove trading pairs
   INTERVAL = "1min"                 # Supported: 1min, 5min, 15min, etc. (check Bitget API)
   LIMIT = 200                       # Max records per API request (do not exceed API limits)
   TOTAL_RECORDS = 3000              # Records to retain per token
   SLEEP_TIME = 2                    # Seconds between API requests to avoid rate limits
   ```

## Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/yourusername/crypto-data-collector.git
   cd crypto-data-collector
   ```

2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt  # Create a requirements.txt with the libraries
   ```

## Usage

### Step 1: Run the Script
```bash
python data_collector.py
```

### Workflow
1. **MongoDB Connection Check**: Verifies connectivity to your database.
2. **Create Time-Series Collections**: If they don't exist.
3. **Fetch Initial Data**: Populates up to 3,000 historical records per token.
4. **Fill Gaps**: Ensures no missing data between the oldest record and the current time.
5. **Live Updates**: Runs indefinitely, fetching new data every minute.

## Database Structure
Each token has a dedicated time-series collection named `<TOKEN>_timeseries` (e.g., `ETHUSDT_timeseries`). Documents include:
```javascript
{
  "token": "ETHUSDT",           // Trading pair
  "timestamp": ISODate("2023-10-01T00:00:00Z"),  // UTC time
  "open": 1700.5,               // Opening price
  "high": 1712.3,               // Highest price during the interval
  "low": 1698.2,                // Lowest price
  "close": 1705.7,              // Closing price
  "base_volume": 450.2,         // Volume in the base currency (e.g., ETH)
  "quote_volume": 765432.1      // Volume in the quote currency (e.g., USDT)
}
```

## Troubleshooting

- **Connection Errors**:
  - Ensure the MongoDB URI is correct and whitelisted in Atlas.
  - Check network connectivity.

- **No Data Fetched**:
  - Verify the token symbols match Bitget's supported pairs.
  - Check Bitget API status for outages.

- **Gap Filling Issues**:
  - The loop condition in `fill_gaps()` may contain a bug. Replace `&` with `and`:
    ```python
    while fetched < total_minutes and total_minutes > 0:
    ```

- **Rate Limits**:
  - Increase `SLEEP_TIME` if encountering HTTP 429 errors.

## License
MIT License. Replace with your preferred license.

---

**Note**: Avoid committing sensitive data (e.g., MongoDB credentials). Use environment variables in production.