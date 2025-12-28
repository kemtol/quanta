# fut-fetchers Worker

Cloudflare Worker for fetching and storing raw market data (US10Y, VIX, DP_PROXY) to R2 bucket.

## 📊 Data Sources

| Instrument | Source | API | Frequency |
|------------|--------|-----|-----------|
| **US10Y** | FRED (Federal Reserve) | DGS10 | Daily |
| **VIX** | FRED | VIXCLS | Daily |
| **DP_PROXY** | FINRA RegSHO | QQQ short data | Daily |

##  Features

- ✅ Automated hourly cron fetching
- ✅ Historical backfill support
- ✅ Duplicate detection (skip existing files)
- ✅ Official API sources (FRED, FINRA)
- ✅ Deep value verification
- ✅ **Auto-triggers fut-features consolidation** (piggybacks on cron)

## 🚀 Endpoints

### `POST /run`
Manually trigger data fetch for latest values.

```bash
curl -X POST "https://fut-fetchers.mkemalw.workers.dev/run"
```

**Response:**
```json
{
  "ok": true,
  "wrote": 3,
  "keys": [
    "raw_US10Y/daily/2024/12/20241216.json",
    "raw_VIX/daily/2024/12/20241216.json",
    "raw_DP_PROXY/daily/2024/12/20241216.json"
  ]
}
```

### `POST /backfill`
Backfill historical data for a date range.

```bash
curl -X POST "https://fut-fetchers.mkemalw.workers.dev/backfill?start=2024-12-01&end=2024-12-31"
```

**Query Parameters:**
- `start` - Start date (YYYY-MM-DD)
- `end` - End date (YYYY-MM-DD)
- `days` (optional) - Number of days to backfill if start/end not provided

**Response:**
```json
{
  "ok": true,
  "backfill": {
    "start": "2024-12-01",
    "end": "2024-12-31",
    "summary": {
      "US10Y": { "wrote": 20, "skipped": 11, "total": 31 },
      "VIX": { "wrote": 20, "skipped": 11, "total": 31 },
      "DP_PROXY": { "wrote": 20, "skipped": 11, "notFound": 0 }
    }
  }
}
```

### `GET /sanity-check`
Verify backfilled data quality.

```bash
curl "https://fut-fetchers.mkemalw.workers.dev/sanity-check?start=2024-12-01&end=2024-12-31&samples=20&deep=true"
```

**Query Parameters:**
- `start` - Start date
- `end` - End date
- `samples` - Number of random samples
- `deep` - Set to `true` for value verification against live APIs

**Response:**
```json
{
  "ok": true,
  "sanity_check": {
    "summary": {
      "US10Y": { "total": 20, "exists": 20, "valid": 20, "success_rate": "100.0%" }
    },
    "deep_verification": {
      "US10Y_summary": { "total": 10, "verified": 10, "accuracy": "100.0%" }
    }
  }
}
```

## 📁 R2 Storage Structure

```
tape-data-futures/
└── raw_{INSTRUMENT}/daily/YYYY/MM/YYYYMMDD.json
    ├── raw_US10Y/daily/2024/12/20241216.json
    ├── raw_VIX/daily/2024/12/20241216.json
    └── raw_DP_PROXY/daily/2024/12/20241216.json
```

### Data Format

**US10Y / VIX Example:**
```json
{
  "instrument": "US10Y",
  "source": "FRED:DGS10:API",
  "tf": "daily",
  "date": "2024-12-16",
  "value": 4.22,
  "ts_utc": "2024-12-16T10:00:00.000Z"
}
```

**DP_PROXY Example:**
```json
{
  "instrument": "DP_PROXY",
  "proxy_symbol": "QQQ",
  "source": "FINRA:RegSHO:CNMSshvol",
  "tf": "daily",
  "date": "2024-12-16",
  "short_vol": 5500000,
  "total_vol": 10000000,
  "short_ratio": 0.55,
  "source_url": "https://cdn.finra.org/equity/regsho/daily/CNMSshvol20241216.txt",
  "ts_utc": "2024-12-16T10:00:00.000Z"
}
```

## ⚙️ Configuration

Environment variables in `wrangler.toml`:

```toml
[vars]
DP_PROXY_SYMBOL = "QQQ"
FINRA_SLEEP_MS = "50"
FRED_API_KEY = "your_api_key_here"

[[r2_buckets]]
binding = "TAPE_DATA_FUTURES"
bucket_name = "tape-data-futures"

[triggers]
crons = ["0 * * * *"]  # Runs hourly
```

### Getting FRED API Key
1. Visit https://fred.stlouisfed.org/
2. Create free account
3. Generate API key at https://fredaccount.stlouisfed.org/apikeys
4. Update `FRED_API_KEY` in wrangler.toml

## 📜 Scripts

### `backfill_monthly.sh`
Batch backfill script for multiple months.

```bash
./backfill_monthly.sh
```

Consolidates 2 years of data (Jan 2024 - Dec 2025) with 2-second delays between requests.

### `sanity_check.sh`
Quick sanity check script.

```bash
./sanity_check.sh [start_date] [end_date] [samples]

# Examples
./sanity_check.sh                           # Default: 2024-01-01 to 2025-12-16, 20 samples
./sanity_check.sh 2024-12-01 2024-12-31 30  # Custom range
```

## 🔍 Data Quality

- **Structure Validation**: 96-100% (missing = market holidays)
- **Value Verification**: 100% match with source APIs
- **FINRA Deep Check**: 100% accuracy
- **FRED Deep Check**: 100% accuracy (with correct User-Agent)

## 🚨 Rate Limiting

- **FRED**: Official API, generous limits
- **FINRA**: File-based, `FINRA_SLEEP_MS` prevents throttling
- Backfill uses delays between requests
- Deep verification samples to avoid API limits

## 📝 Notes

- Hourly cron fetches latest data automatically
- Duplicate files are skipped
- Deep verification requires working FRED API key
- Market holidays result in missing data (expected)
- Worker automatically handles weekends and invalid dates
