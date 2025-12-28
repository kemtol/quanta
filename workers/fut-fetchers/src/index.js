// fut-fetchers/src/index.js

function pad2(n) { return String(n).padStart(2, "0"); }
function yyyymmdd(d) {
    const y = d.getUTCFullYear();
    const m = pad2(d.getUTCMonth() + 1);
    const day = pad2(d.getUTCDate());
    return `${y}${m}${day}`;
}
function keyDaily(prefix, d /* Date */) {
    const y = String(d.getUTCFullYear());
    const m = pad2(d.getUTCMonth() + 1);
    const dd = yyyymmdd(d);
    // contoh: raw_US10Y/daily/2025/12/20251211.json
    return `${prefix}/daily/${y}/${m}/${dd}.json`;
}

async function existsR2(env, key) {
    const obj = await env.TAPE_DATA_FUTURES.head(key);
    return !!obj;
}

async function putJson(env, key, data) {
    await env.TAPE_DATA_FUTURES.put(
        key,
        JSON.stringify(data),
        { httpMetadata: { contentType: "application/json" } }
    );
}

async function fetchText(url) {
    const r = await fetch(url, {
        headers: { "User-Agent": "Mozilla/5.0" },
    });
    return { status: r.status, text: await r.text() };
}

// -------- FRED (daily) --------
// Official FRED API, requires API key (free)
// Get key at: https://fred.stlouisfed.org/docs/api/api_key.html
async function fetchFredLatest(seriesId, apiKey) {
    if (!apiKey) {
        throw new Error(`FRED API key not configured. Set FRED_API_KEY environment variable.`);
    }

    // Use official FRED API endpoint
    const url = `https://api.stlouisfed.org/fred/series/observations?series_id=${seriesId}&api_key=${apiKey}&file_type=json&sort_order=desc&limit=10`;

    const r = await fetch(url, {
        headers: {
            "User-Agent": "Mozilla/5.0 (compatible; FutFetchers/1.0)"
        }
    });

    if (!r.ok) throw new Error(`FRED ${seriesId} HTTP ${r.status}`);
    const data = await r.json();

    // Response format: { observations: [{ date: "YYYY-MM-DD", value: "4.25" }, ...] }
    if (!data.observations || data.observations.length === 0) {
        throw new Error(`FRED ${seriesId} no observations returned`);
    }

    // Find the latest valid value (sometimes recent values are ".")
    for (const obs of data.observations) {
        const ds = obs.date;
        const vs = obs.value;

        if (!ds || !vs || vs === "." || vs.toLowerCase() === "nan") continue;

        const value = Number(vs);
        if (!Number.isFinite(value)) continue;

        return { date: ds, value };
    }

    throw new Error(`FRED ${seriesId} no valid latest value`);
}

// Fetch FRED historical data for date range
async function fetchFredHistorical(seriesId, apiKey, startDate /* YYYY-MM-DD */, endDate /* YYYY-MM-DD */) {
    if (!apiKey) {
        throw new Error(`FRED API key not configured. Set FRED_API_KEY environment variable.`);
    }

    const url = `https://api.stlouisfed.org/fred/series/observations?series_id=${seriesId}&api_key=${apiKey}&file_type=json&observation_start=${startDate}&observation_end=${endDate}`;

    const r = await fetch(url, {
        headers: {
            "User-Agent": "Mozilla/5.0 (compatible; FutFetchers/1.0)"
        }
    });

    if (!r.ok) throw new Error(`FRED ${seriesId} historical HTTP ${r.status}`);
    const data = await r.json();

    if (!data.observations || data.observations.length === 0) {
        return [];
    }

    // Filter valid values only
    const results = [];
    for (const obs of data.observations) {
        const ds = obs.date;
        const vs = obs.value;

        if (!ds || !vs || vs === "." || vs.toLowerCase() === "nan") continue;

        const value = Number(vs);
        if (!Number.isFinite(value)) continue;

        results.push({ date: ds, value });
    }

    return results;
}

// -------- FINRA RegSHO (daily file) --------
// CNMSshvolYYYYMMDD.txt, pipe-delimited, trailer line di akhir
async function fetchFinraRegshoForDate(dateUTC /* Date */, symbol /* QQQ */) {
    const base = "https://cdn.finra.org/equity/regsho/daily/CNMSshvol";
    const ds = yyyymmdd(dateUTC);
    const url = `${base}${ds}.txt`;

    const { status, text } = await fetchText(url);
    if (status !== 200) return { ok: false, status };

    // quick sanity check
    if (!text.includes("|") || !text.includes("Symbol")) {
        return { ok: false, status: 200, error: "unexpected_format" };
    }

    const lines = text.trim().split("\n");
    if (lines.length < 3) return { ok: false, status: 200, error: "too_short" };

    const header = lines[0].split("|");
    const idxSymbol = header.indexOf("Symbol");
    const idxShort = header.indexOf("ShortVolume");
    const idxTotal = header.indexOf("TotalVolume");
    if (idxSymbol === -1 || idxShort === -1 || idxTotal === -1) {
        return { ok: false, status: 200, error: "missing_columns" };
    }

    // skip trailer line: biasanya baris terakhir "Trailer|..."
    for (let i = 1; i < lines.length - 1; i++) {
        const cols = lines[i].split("|");
        if ((cols[idxSymbol] || "").trim() !== symbol) continue;

        const shortVol = Number(cols[idxShort]);
        const totalVol = Number(cols[idxTotal]);
        const ratio = (totalVol > 0) ? (shortVol / totalVol) : null;

        return {
            ok: true,
            date: `${dateUTC.getUTCFullYear()}-${pad2(dateUTC.getUTCMonth() + 1)}-${pad2(dateUTC.getUTCDate())}`,
            symbol,
            shortVol: Number.isFinite(shortVol) ? shortVol : null,
            totalVol: Number.isFinite(totalVol) ? totalVol : null,
            shortRatio: (ratio !== null && Number.isFinite(ratio)) ? ratio : null,
            source_url: url
        };
    }

    return { ok: false, status: 200, error: "symbol_not_found" };
}

// Cari FINRA daily terbaru (hari ini mundur max N hari)
async function fetchFinraLatest(symbol, maxLookbackDays = 10, sleepMs = 50) {
    const now = new Date(); // UTC by default in getUTC*
    for (let i = 0; i <= maxLookbackDays; i++) {
        const d = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
        d.setUTCDate(d.getUTCDate() - i);

        // skip weekend (UTC) biar hemat request
        const wd = d.getUTCDay(); // 0 Sun .. 6 Sat
        if (wd === 0 || wd === 6) continue;

        const r = await fetchFinraRegshoForDate(d, symbol);
        if (r.ok) return r;

        // kalau 404 wajar (holiday / belum publish)
        await new Promise(res => setTimeout(res, sleepMs));
    }
    throw new Error(`FINRA latest not found within ${maxLookbackDays} trading-ish days`);
}

// -------- Yahoo Finance (futures daily OHLCV) --------
// Free API - no key required
// Tickers: MNQ=F (Micro Nasdaq), MGC=F (Micro Gold)
async function fetchYahooQuote(ticker) {
    // Yahoo Finance chart API (undocumented butopen/public)
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${ticker}?interval=1d&range=5d`;

    const r = await fetch(url, {
        headers: {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
    });

    if (!r.ok) {
        throw new Error(`Yahoo ${ticker} HTTP ${r.status}`);
    }

    const data = await r.json();

    if (!data.chart || !data.chart.result || data.chart.result.length === 0) {
        throw new Error(`Yahoo ${ticker} no chart data`);
    }

    const result = data.chart.result[0];
    const timestamps = result.timestamp || [];
    const quote = result.indicators?.quote?.[0];

    if (!quote || timestamps.length === 0) {
        throw new Error(`Yahoo ${ticker} no quote data`);
    }

    // Get latest bar (last element)
    const idx = timestamps.length - 1;
    const ts = timestamps[idx];
    const date = new Date(ts * 1000);

    return {
        date: `${date.getUTCFullYear()}-${pad2(date.getUTCMonth() + 1)}-${pad2(date.getUTCDate())}`,
        timestamp: date.toISOString(),
        open: quote.open?.[idx] || null,
        high: quote.high?.[idx] || null,
        low: quote.low?.[idx] || null,
        close: quote.close?.[idx] || null,
        volume: quote.volume?.[idx] || null
    };
}

// Write futures OHLCV directly to final structure (no raw layer)
async function putFuturesOHLCV(env, ticker, dateStr, ohlcv) {
    const date = new Date(dateStr + 'T00:00:00Z');
    const year = date.getUTCFullYear();
    const month = pad2(date.getUTCMonth() + 1);
    const day = pad2(date.getUTCDate());
    const yyyymmdd = `${year}${month}${day}`;

    // Build consolidated JSON directly
    const consolidated = {
        meta: {
            ticker,
            date: dateStr,
            timeframes: ["1m", "5m", "15m", "1h", "4h", "daily"],
            fields: ["open", "high", "low", "close", "volume"],
            count: {
                "1m": 0,
                "5m": 0,
                "15m": 0,
                "1h": 0,
                "4h": 0,
                daily: 1
            },
            generated_at: new Date().toISOString()
        },
        timeseries: {
            daily: [{
                date: dateStr,
                timestamp: ohlcv.timestamp || date.toISOString(),
                open: ohlcv.open,
                high: ohlcv.high,
                low: ohlcv.low,
                close: ohlcv.close,
                volume: ohlcv.volume
            }],
            "4h": [],
            "1h": [],
            "15m": [],
            "5m": [],
            "1m": []
        }
    };

    // Write directly to ticker/YYYYMMDD.json
    const key = `${ticker}/${yyyymmdd}.json`;
    await env.TAPE_DATA_FUTURES.put(
        key,
        JSON.stringify(consolidated, null, 2),
        { httpMetadata: { contentType: 'application/json' } }
    );

    return key;
}

// Fetch Yahoo Finance intraday data (1m, 5m, 15m, 1h)
// Note: Yahoo limits: 1m/5m/15m max 60 days, 1h max 730 days
// Yahoo does NOT have 4h native, we resample from 1h
async function fetchYahooIntraday(ticker, interval, daysBack = 7) {
    // Yahoo interval mapping
    const intervalMap = {
        '1m': '1m',
        '5m': '5m',
        '15m': '15m',
        '1h': '60m'
    };

    const yahooInterval = intervalMap[interval];
    if (!yahooInterval) {
        throw new Error(`Unsupported interval: ${interval}`);
    }

    const now = Math.floor(Date.now() / 1000);
    const start = now - (daysBack * 24 * 60 * 60);

    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${ticker}=F?interval=${yahooInterval}&period1=${start}&period2=${now}`;

    const r = await fetch(url, {
        headers: {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
    });

    if (!r.ok) {
        throw new Error(`Yahoo ${ticker} intraday HTTP ${r.status}`);
    }

    const data = await r.json();

    if (!data.chart || !data.chart.result || data.chart.result.length === 0) {
        throw new Error(`Yahoo ${ticker} no intraday data`);
    }

    const result = data.chart.result[0];
    const timestamps = result.timestamp || [];
    const quote = result.indicators?.quote?.[0];

    if (!quote || timestamps.length === 0) {
        throw new Error(`Yahoo ${ticker} no intraday quote`);
    }

    // Build bars array
    const bars = [];
    for (let i = 0; i < timestamps.length; i++) {
        const ts = timestamps[i];
        const date = new Date(ts * 1000);

        bars.push({
            timestamp: date.toISOString(),
            open: quote.open?.[i] || null,
            high: quote.high?.[i] || null,
            low: quote.low?.[i] || null,
            close: quote.close?.[i] || null,
            volume: quote.volume?.[i] || null
        });
    }

    return bars;
}

// Resample 1h bars to 4h bars
function resample1hTo4h(bars1h) {
    // Sort by timestamp
    const sorted = [...bars1h].sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

    const bars4h = [];
    const buckets = new Map(); // "YYYY-MM-DD-HH" (4h bucket) -> bars

    for (const bar of sorted) {
        const d = new Date(bar.timestamp);
        const hour = d.getUTCHours();
        const bucket4h = Math.floor(hour / 4) * 4; // 0, 4, 8, 12, 16, 20
        const key = `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())}-${pad2(bucket4h)}`;

        if (!buckets.has(key)) buckets.set(key, []);
        buckets.get(key).push(bar);
    }

    for (const [key, chunk] of buckets.entries()) {
        if (chunk.length === 0) continue;

        const open = chunk[0].open;
        const close = chunk[chunk.length - 1].close;
        const high = Math.max(...chunk.map(b => b.high).filter(v => v !== null));
        const low = Math.min(...chunk.map(b => b.low).filter(v => v !== null));
        const volume = chunk.reduce((sum, b) => sum + (b.volume || 0), 0);

        // Use first bar's timestamp as 4h bar timestamp
        bars4h.push({
            timestamp: chunk[0].timestamp,
            open,
            high,
            low,
            close,
            volume
        });
    }

    return bars4h.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
}

// Ingest intraday bars into existing daily files
async function ingestIntraday(env, ticker, interval, bars) {
    // Group bars by date
    const byDate = new Map(); // YYYYMMDD -> bars[]

    for (const bar of bars) {
        const d = new Date(bar.timestamp);
        const dateKey = `${d.getUTCFullYear()}${pad2(d.getUTCMonth() + 1)}${pad2(d.getUTCDate())}`;

        if (!byDate.has(dateKey)) byDate.set(dateKey, []);
        byDate.get(dateKey).push(bar);
    }

    let updated = 0;
    let skipped = 0;

    for (const [dateKey, dateBars] of byDate.entries()) {
        const fileKey = `${ticker}/${dateKey}.json`;

        // Check if file exists
        const existing = await env.TAPE_DATA_FUTURES.get(fileKey);
        if (!existing) {
            skipped++;
            continue; // Daily file doesn't exist, skip
        }

        // Parse existing file
        const existingData = JSON.parse(await existing.text());

        // Add date field to each bar for consistency
        const dateStr = `${dateKey.slice(0, 4)}-${dateKey.slice(4, 6)}-${dateKey.slice(6, 8)}`;
        const formattedBars = dateBars.map(bar => ({
            date: dateStr,
            ...bar
        }));

        // Update timeseries for this interval
        existingData.timeseries[interval] = formattedBars;
        existingData.meta.count[interval] = formattedBars.length;
        existingData.meta.generated_at = new Date().toISOString();

        // Write back
        await env.TAPE_DATA_FUTURES.put(
            fileKey,
            JSON.stringify(existingData, null, 2),
            { httpMetadata: { contentType: 'application/json' } }
        );

        updated++;
    }

    return { ticker, interval, updated, skipped };
}

// Backfill intraday data for a ticker (all intervals)
async function backfillYahooIntraday(env, ticker, daysBack = 7) {
    const results = [];

    // 1. Fetch 1h data (we'll resample to 4h)
    console.log(`[Intraday] Fetching 1h for ${ticker}...`);
    const bars1h = await fetchYahooIntraday(ticker, '1h', daysBack);
    const ingest1h = await ingestIntraday(env, ticker, '1h', bars1h);
    results.push(ingest1h);

    // 2. Resample to 4h and ingest
    console.log(`[Intraday] Resampling 1h → 4h for ${ticker}...`);
    const bars4h = resample1hTo4h(bars1h);
    const ingest4h = await ingestIntraday(env, ticker, '4h', bars4h);
    results.push(ingest4h);

    // 3. Fetch 15m data
    console.log(`[Intraday] Fetching 15m for ${ticker}...`);
    const bars15m = await fetchYahooIntraday(ticker, '15m', Math.min(daysBack, 60));
    const ingest15m = await ingestIntraday(env, ticker, '15m', bars15m);
    results.push(ingest15m);

    // 4. Fetch 5m data
    console.log(`[Intraday] Fetching 5m for ${ticker}...`);
    const bars5m = await fetchYahooIntraday(ticker, '5m', Math.min(daysBack, 60));
    const ingest5m = await ingestIntraday(env, ticker, '5m', bars5m);
    results.push(ingest5m);

    // 5. Fetch 1m data (most granular, shortest history available)
    console.log(`[Intraday] Fetching 1m for ${ticker}...`);
    const bars1m = await fetchYahooIntraday(ticker, '1m', Math.min(daysBack, 7)); // 1m typically max 7 days
    const ingest1m = await ingestIntraday(env, ticker, '1m', bars1m);
    results.push(ingest1m);

    return { ticker, daysBack, results };
}



// =========================
// BACKFILL FUNCTIONS
// =========================

// Backfill Yahoo Finance futures data (maximum available history)
async function backfillYahooFutures(env, ticker, yearsBack = 5) {
    const now = Math.floor(Date.now() / 1000);
    const start = now - (yearsBack * 365 * 24 * 60 * 60); // N years ago

    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${ticker}=F?interval=1d&period1=${start}&period2=${now}`;

    const r = await fetch(url, {
        headers: {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
    });

    if (!r.ok) {
        throw new Error(`Yahoo ${ticker} historical HTTP ${r.status}`);
    }

    const data = await r.json();

    if (!data.chart || !data.chart.result || data.chart.result.length === 0) {
        throw new Error(`Yahoo ${ticker} no chart data`);
    }

    const result = data.chart.result[0];
    const timestamps = result.timestamp || [];
    const quote = result.indicators?.quote?.[0];

    if (!quote || timestamps.length === 0) {
        throw new Error(`Yahoo ${ticker} no quote data`);
    }

    let wrote = 0;
    let skipped = 0;

    // Process all bars
    for (let i = 0; i < timestamps.length; i++) {
        const ts = timestamps[i];
        const date = new Date(ts * 1000);
        const dateStr = `${date.getUTCFullYear()}-${pad2(date.getUTCMonth() + 1)}-${pad2(date.getUTCDate())}`;
        const yyyymmdd = `${date.getUTCFullYear()}${pad2(date.getUTCMonth() + 1)}${pad2(date.getUTCDate())}`;

        // Check if final file exists (ticker/YYYYMMDD.json)
        const finalKey = `${ticker}/${yyyymmdd}.json`;
        if (await existsR2(env, finalKey)) {
            skipped++;
            continue;
        }

        // Write directly to final structure
        const ohlcv = {
            timestamp: date.toISOString(),
            open: quote.open?.[i] || null,
            high: quote.high?.[i] || null,
            low: quote.low?.[i] || null,
            close: quote.close?.[i] || null,
            volume: quote.volume?.[i] || null
        };

        await putFuturesOHLCV(env, ticker, dateStr, ohlcv);
        wrote++;
    }

    return {
        ticker,
        total: timestamps.length,
        wrote,
        skipped,
        oldest: timestamps.length > 0 ? new Date(timestamps[0] * 1000).toISOString().split('T')[0] : null,
        newest: timestamps.length > 0 ? new Date(timestamps[timestamps.length - 1] * 1000).toISOString().split('T')[0] : null
    };
}

// Backfill Yahoo Finance futures data for a specific month (chunked)
// month format: "YYYY-MM"
async function backfillYahooFuturesMonth(env, ticker, month) {
    const [year, mon] = month.split('-').map(Number);

    // Calculate date range for this month
    const startDate = new Date(Date.UTC(year, mon - 1, 1));
    const endDate = new Date(Date.UTC(year, mon, 0)); // Last day of month

    const period1 = Math.floor(startDate.getTime() / 1000);
    const period2 = Math.floor(endDate.getTime() / 1000) + 86400; // Include last day

    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${ticker}=F?interval=1d&period1=${period1}&period2=${period2}`;

    const r = await fetch(url, {
        headers: {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
    });

    if (!r.ok) {
        throw new Error(`Yahoo ${ticker} HTTP ${r.status} for ${month}`);
    }

    const data = await r.json();

    if (!data.chart || !data.chart.result || data.chart.result.length === 0) {
        return { ticker, month, total: 0, wrote: 0, skipped: 0, error: "no data" };
    }

    const result = data.chart.result[0];
    const timestamps = result.timestamp || [];
    const quote = result.indicators?.quote?.[0];

    if (!quote || timestamps.length === 0) {
        return { ticker, month, total: 0, wrote: 0, skipped: 0, error: "no quotes" };
    }

    let wrote = 0;
    let skipped = 0;

    for (let i = 0; i < timestamps.length; i++) {
        const ts = timestamps[i];
        const date = new Date(ts * 1000);
        const dateStr = `${date.getUTCFullYear()}-${pad2(date.getUTCMonth() + 1)}-${pad2(date.getUTCDate())}`;
        const yyyymmdd = `${date.getUTCFullYear()}${pad2(date.getUTCMonth() + 1)}${pad2(date.getUTCDate())}`;

        // Check if file exists
        const finalKey = `${ticker}/${yyyymmdd}.json`;
        if (await existsR2(env, finalKey)) {
            skipped++;
            continue;
        }

        const ohlcv = {
            timestamp: date.toISOString(),
            open: quote.open?.[i] || null,
            high: quote.high?.[i] || null,
            low: quote.low?.[i] || null,
            close: quote.close?.[i] || null,
            volume: quote.volume?.[i] || null
        };

        await putFuturesOHLCV(env, ticker, dateStr, ohlcv);
        wrote++;
    }

    return {
        ticker,
        month,
        total: timestamps.length,
        wrote,
        skipped,
        range: {
            start: startDate.toISOString().split('T')[0],
            end: endDate.toISOString().split('T')[0]
        }
    };
}

// Backfill FRED data for a date range
async function backfillFredData(env, seriesId, prefix, instrumentName, startDate, endDate) {
    const observations = await fetchFredHistorical(seriesId, env.FRED_API_KEY, startDate, endDate);

    let wrote = 0;
    let skipped = 0;

    for (const obs of observations) {
        const d = new Date(`${obs.date}T00:00:00Z`);
        const key = keyDaily(prefix, d);

        // Skip if already exists
        if (await existsR2(env, key)) {
            skipped++;
            continue;
        }

        const payload = {
            instrument: instrumentName,
            source: `FRED:${seriesId}:API`,
            tf: "daily",
            date: obs.date,
            value: obs.value,
            ts_utc: new Date().toISOString()
        };

        await putJson(env, key, payload);
        wrote++;
    }

    return { instrument: instrumentName, total: observations.length, wrote, skipped };
}

// Backfill FINRA data for a date range
async function backfillFinraData(env, symbol, startDate, endDate, sleepMs = 100) {
    const start = new Date(startDate);
    const end = new Date(endDate);

    let wrote = 0;
    let skipped = 0;
    let notFound = 0;

    // Iterate through each day
    for (let d = new Date(start); d <= end; d.setUTCDate(d.getUTCDate() + 1)) {
        const wd = d.getUTCDay();
        // Skip weekends
        if (wd === 0 || wd === 6) continue;

        const key = keyDaily("raw_DP_PROXY", d);

        // Skip if already exists
        if (await existsR2(env, key)) {
            skipped++;
            continue;
        }

        const r = await fetchFinraRegshoForDate(new Date(d), symbol);

        if (!r.ok) {
            notFound++;
            await new Promise(res => setTimeout(res, sleepMs));
            continue;
        }

        const payload = {
            instrument: "DP_PROXY",
            proxy_symbol: symbol,
            source: "FINRA:RegSHO:CNMSshvol",
            tf: "daily",
            date: r.date,
            short_vol: r.shortVol,
            total_vol: r.totalVol,
            short_ratio: r.shortRatio,
            source_url: r.source_url,
            ts_utc: new Date().toISOString()
        };

        await putJson(env, key, payload);
        wrote++;

        // Rate limiting
        await new Promise(res => setTimeout(res, sleepMs));
    }

    return { instrument: "DP_PROXY", wrote, skipped, notFound };
}

// =========================
// SANITY CHECK FUNCTIONS
// =========================

// Generate random trading dates within a range
function generateRandomDates(startDate, endDate, count) {
    const start = new Date(startDate);
    const end = new Date(endDate);
    const dates = [];
    const allDates = [];

    // Collect all weekdays in range
    for (let d = new Date(start); d <= end; d.setUTCDate(d.getUTCDate() + 1)) {
        const wd = d.getUTCDay();
        if (wd !== 0 && wd !== 6) { // Skip weekends
            allDates.push(new Date(d));
        }
    }

    // Random sampling
    const sampleSize = Math.min(count, allDates.length);
    const indices = new Set();

    while (indices.size < sampleSize) {
        const idx = Math.floor(Math.random() * allDates.length);
        indices.add(idx);
    }

    for (const idx of indices) {
        dates.push(allDates[idx]);
    }

    return dates.sort((a, b) => a - b);
}

// Check if data exists and validate content
async function checkDataPoint(env, prefix, instrument, date) {
    const key = keyDaily(prefix, date);
    const dateStr = `${date.getUTCFullYear()}-${pad2(date.getUTCMonth() + 1)}-${pad2(date.getUTCDate())}`;

    try {
        const obj = await env.TAPE_DATA_FUTURES.get(key);

        if (!obj) {
            return {
                date: dateStr,
                key,
                exists: false,
                valid: false,
                error: "not_found"
            };
        }

        const text = await obj.text();
        const data = JSON.parse(text);

        // Validate required fields
        let valid = true;
        let issues = [];

        if (data.instrument !== instrument) {
            valid = false;
            issues.push(`wrong_instrument: ${data.instrument}`);
        }

        if (data.date !== dateStr) {
            valid = false;
            issues.push(`wrong_date: ${data.date}`);
        }

        // Check instrument-specific fields
        if (instrument === "DP_PROXY") {
            if (typeof data.short_ratio !== 'number' || data.short_ratio === null) {
                valid = false;
                issues.push("missing_short_ratio");
            }
        } else {
            if (typeof data.value !== 'number' || !Number.isFinite(data.value)) {
                valid = false;
                issues.push("invalid_value");
            }
        }

        return {
            date: dateStr,
            key,
            exists: true,
            valid,
            issues: issues.length > 0 ? issues : undefined,
            sample: instrument === "DP_PROXY" ?
                { short_ratio: data.short_ratio, short_vol: data.short_vol } :
                { value: data.value }
        };

    } catch (err) {
        return {
            date: dateStr,
            key,
            exists: false,
            valid: false,
            error: err.message
        };
    }
}

// Deep verification - compare stored data with live API data
async function verifyDataValue(env, prefix, instrument, date, seriesId = null) {
    const dateStr = `${date.getUTCFullYear()}-${pad2(date.getUTCMonth() + 1)}-${pad2(date.getUTCDate())}`;
    const key = keyDaily(prefix, date);

    try {
        // Get stored data
        const obj = await env.TAPE_DATA_FUTURES.get(key);
        if (!obj) {
            return { date: dateStr, verified: false, error: "stored_data_not_found" };
        }

        const storedData = JSON.parse(await obj.text());
        let liveValue = null;
        let source = "unknown";

        // Fetch live value from source API
        if (instrument === "US10Y" || instrument === "VIX") {
            // FRED API
            const seriesMap = { "US10Y": "DGS10", "VIX": "VIXCLS" };
            const sid = seriesId || seriesMap[instrument];

            const url = `https://api.stlouisfed.org/fred/series/observations?series_id=${sid}&api_key=${env.FRED_API_KEY}&file_type=json&observation_start=${dateStr}&observation_end=${dateStr}`;
            const r = await fetch(url, {
                headers: {
                    "User-Agent": "Mozilla/5.0 (compatible; FutFetchers/1.0)"
                }
            });

            if (!r.ok) {
                return { date: dateStr, verified: false, error: `fred_http_${r.status}`, debug: { url, status: r.status } };
            }

            const data = await r.json();

            if (!data.observations || data.observations.length === 0) {
                return { date: dateStr, verified: false, error: "no_observations", debug: { hasData: !!data } };
            }

            const obs = data.observations[0];

            if (!obs.value || obs.value === "." || String(obs.value).toLowerCase() === "nan") {
                return { date: dateStr, verified: false, error: "invalid_value", debug: { obsValue: obs.value, obsDate: obs.date } };
            }

            const parsedValue = Number(obs.value);
            if (!Number.isFinite(parsedValue)) {
                return { date: dateStr, verified: false, error: "non_finite_value", debug: { parsedValue, original: obs.value } };
            }

            liveValue = parsedValue;
            source = `FRED:${sid}`;

            // Compare values
            const stored = storedData.value;
            const match = Math.abs(stored - liveValue) < 0.01; // tolerance for floating point

            return {
                date: dateStr,
                verified: match,
                stored_value: stored,
                live_value: liveValue,
                difference: Math.abs(stored - liveValue),
                source,
                match
            };

        } else if (instrument === "DP_PROXY") {
            // FINRA RegSHO
            const symbol = env.DP_PROXY_SYMBOL || "QQQ";
            const finraData = await fetchFinraRegshoForDate(date, symbol);

            if (!finraData.ok) {
                return { date: dateStr, verified: false, error: `finra_error: ${finraData.error || finraData.status}` };
            }

            // Compare values
            const stored = storedData.short_ratio;
            const live = finraData.shortRatio;
            const match = Math.abs(stored - live) < 0.0001; // tolerance

            return {
                date: dateStr,
                verified: match,
                stored_value: stored,
                live_value: live,
                difference: Math.abs(stored - live),
                source: "FINRA:RegSHO",
                match
            };
        }

        return { date: dateStr, verified: false, error: "unknown_instrument" };

    } catch (err) {
        return { date: dateStr, verified: false, error: err.message };
    }
}

// Run sanity check for all instruments
async function runSanityCheck(env, startDate, endDate, sampleCount = 10, deepVerify = false) {
    const dates = generateRandomDates(startDate, endDate, sampleCount);

    const results = {
        US10Y: [],
        VIX: [],
        DP_PROXY: []
    };

    for (const date of dates) {
        const [us10y, vix, dpProxy] = await Promise.all([
            checkDataPoint(env, "raw_US10Y", "US10Y", date),
            checkDataPoint(env, "raw_VIX", "VIX", date),
            checkDataPoint(env, "raw_DP_PROXY", "DP_PROXY", date)
        ]);

        results.US10Y.push(us10y);
        results.VIX.push(vix);
        results.DP_PROXY.push(dpProxy);
    }

    // Deep verification: sample subset for value verification
    let deepVerification = null;
    if (deepVerify) {
        const verifyCount = Math.min(10, dates.length);
        const verifyDates = dates.slice(0, verifyCount);

        deepVerification = {
            US10Y: [],
            VIX: [],
            DP_PROXY: []
        };

        for (const date of verifyDates) {
            const [us10y, vix, dpProxy] = await Promise.all([
                verifyDataValue(env, "raw_US10Y", "US10Y", date),
                verifyDataValue(env, "raw_VIX", "VIX", date),
                verifyDataValue(env, "raw_DP_PROXY", "DP_PROXY", date)
            ]);

            deepVerification.US10Y.push(us10y);
            deepVerification.VIX.push(vix);
            deepVerification.DP_PROXY.push(dpProxy);
        }

        // Add deep verification summary
        for (const [instrument, checks] of Object.entries(deepVerification)) {
            const total = checks.length;
            const verified = checks.filter(c => c.verified && c.match).length;
            const mismatched = checks.filter(c => c.verified === false || (c.verified && !c.match)).length;

            deepVerification[`${instrument}_summary`] = {
                total,
                verified,
                mismatched,
                accuracy: total > 0 ? ((verified / total) * 100).toFixed(1) + '%' : '0%'
            };
        }
    }

    // Calculate summary stats
    const summary = {};
    for (const [instrument, checks] of Object.entries(results)) {
        const total = checks.length;
        const exists = checks.filter(c => c.exists).length;
        const valid = checks.filter(c => c.valid).length;
        const missing = checks.filter(c => !c.exists).length;
        const invalid = checks.filter(c => c.exists && !c.valid).length;

        summary[instrument] = {
            total,
            exists,
            valid,
            missing,
            invalid,
            success_rate: total > 0 ? ((valid / total) * 100).toFixed(1) + '%' : '0%'
        };
    }

    return {
        summary,
        details: results,
        deep_verification: deepVerification,
        config: {
            start_date: startDate,
            end_date: endDate,
            sample_count: dates.length,
            deep_verify: deepVerify,
            sampled_dates: dates.map(d => `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())}`)
        }
    };
}

// =========================
// CRON ENTRY
// =========================
async function runCron(env) {
    const writes = [];

    // 1) US10Y (DGS10) - daily
    {
        const { date, value } = await fetchFredLatest("DGS10", env.FRED_API_KEY);
        const d = new Date(`${date}T00:00:00Z`);
        const key = keyDaily("raw_US10Y", d);
        if (!(await existsR2(env, key))) {
            const payload = {
                instrument: "US10Y",
                source: "FRED:DGS10:API",
                tf: "daily",
                date,
                value,
                ts_utc: new Date().toISOString()
            };
            await putJson(env, key, payload);
            writes.push(key);
        }
    }

    // 2) VIX (VIXCLS) - daily
    {
        const { date, value } = await fetchFredLatest("VIXCLS", env.FRED_API_KEY);
        const d = new Date(`${date}T00:00:00Z`);
        const key = keyDaily("raw_VIX", d);
        if (!(await existsR2(env, key))) {
            const payload = {
                instrument: "VIX",
                source: "FRED:VIXCLS:API",
                tf: "daily",
                date,
                value,
                ts_utc: new Date().toISOString()
            };
            await putJson(env, key, payload);
            writes.push(key);
        }
    }

    // 3) DP_PROXY (FINRA RegSHO for QQQ) - daily
    {
        const sym = (env.DP_PROXY_SYMBOL || "QQQ").toUpperCase();
        const sleepMs = Number(env.FINRA_SLEEP_MS || "50");
        const finra = await fetchFinraLatest(sym, 12, sleepMs);

        const d = new Date(`${finra.date}T00:00:00Z`);
        const key = keyDaily("raw_DP_PROXY", d);
        if (!(await existsR2(env, key))) {
            const payload = {
                instrument: "DP_PROXY",
                proxy_symbol: sym,
                source: "FINRA:RegSHO:CNMSshvol",
                tf: "daily",
                date: finra.date,
                short_vol: finra.shortVol,
                total_vol: finra.totalVol,
                short_ratio: finra.shortRatio,
                source_url: finra.source_url,
                ts_utc: new Date().toISOString()
            };
            await putJson(env, key, payload);
            writes.push(key);
        }
    }

    // 4) MNQ (Micro E-mini Nasdaq) - daily OHLCV (direct write)
    {
        const quote = await fetchYahooQuote("MNQ=F");
        const yyyymmdd = quote.date.replace(/-/g, '');
        const finalKey = `MNQ/${yyyymmdd}.json`;

        if (!(await existsR2(env, finalKey))) {
            await putFuturesOHLCV(env, "MNQ", quote.date, quote);
            writes.push(finalKey);
        }
    }

    // 5) MGC (Micro Gold) - daily OHLCV (direct write)
    {
        const quote = await fetchYahooQuote("MGC=F");
        const yyyymmdd = quote.date.replace(/-/g, '');
        const finalKey = `MGC/${yyyymmdd}.json`;

        if (!(await existsR2(env, finalKey))) {
            await putFuturesOHLCV(env, "MGC", quote.date, quote);
            writes.push(finalKey);
        }
    }

    return { ok: true, wrote: writes.length, keys: writes };
}

// =========================
// FETCH HANDLER
// =========================
export default {
    async fetch(request, env, ctx) {
        const url = new URL(request.url);

        // Schedule metadata endpoint
        if (request.method === "GET" && url.pathname === "/schedule") {
            let scheduleInfo = null;

            // Read from R2 (cron-checker now stores in monitoring-fut-saham bucket)
            try {
                // Note: This worker doesn't have STATE_BUCKET binding
                // So we can't read the state directly
                // Return static info instead
            } catch (e) {
                // Ignore errors
            }

            return Response.json({
                ok: true,
                worker: 'fut-fetchers',
                schedule: {
                    interval: '15m',
                    cron_expression: '*/15 * * * *'
                },
                last_trigger: scheduleInfo?.last_trigger || null,
                next_trigger: scheduleInfo?.next_trigger || null,
                status: scheduleInfo?.status || 'unknown',
                trigger_count: scheduleInfo?.trigger_count || 0,
                note: 'State tracked by cron-checker in R2 (monitoring-fut-saham)'
            });
        }

        // Manual trigger (biar bisa test)
        if (request.method === "POST" && url.pathname === "/run") {
            const out = await runCron(env);
            return Response.json(out);
        }

        // Backfill historical data (chunked by month internally)
        if (request.method === "POST" && url.pathname === "/backfill") {
            // Accept start/end dates or calculate from days parameter
            let startStr = url.searchParams.get("start");
            let endStr = url.searchParams.get("end");

            if (!startStr || !endStr) {
                const days = parseInt(url.searchParams.get("days") || "730");
                const endDate = new Date();
                const startDate = new Date(endDate);
                startDate.setUTCDate(startDate.getUTCDate() - days);
                startStr = startDate.toISOString().split('T')[0];
                endStr = endDate.toISOString().split('T')[0];
            }

            // Process the entire date range directly (no chunking)
            // User should provide reasonable date ranges (1-2 months max)
            const results = await Promise.all([
                backfillFredData(env, "DGS10", "raw_US10Y", "US10Y", startStr, endStr),
                backfillFredData(env, "VIXCLS", "raw_VIX", "VIX", startStr, endStr),
                backfillFinraData(env, env.DP_PROXY_SYMBOL || "QQQ", startStr, endStr, Number(env.FINRA_SLEEP_MS || "100"))
            ]);

            return Response.json({
                ok: true,
                backfill: {
                    start: startStr,
                    end: endStr,
                    summary: {
                        US10Y: { wrote: results[0].wrote, skipped: results[0].skipped, total: results[0].total },
                        VIX: { wrote: results[1].wrote, skipped: results[1].skipped, total: results[1].total },
                        DP_PROXY: { wrote: results[2].wrote, skipped: results[2].skipped, notFound: results[2].notFound }
                    }
                }
            });
        }

        // Backfill futures (Yahoo Finance historical) - CHUNKED by month
        // Usage: POST /backfill-futures?ticker=MNQ&month=2024-01
        // Or for full backfill: POST /backfill-futures?ticker=MNQ&years=5 (will return list of months to process)
        if (request.method === "POST" && url.pathname === "/backfill-futures") {
            const ticker = url.searchParams.get("ticker") || "MNQ";
            const month = url.searchParams.get("month"); // YYYY-MM format

            if (month) {
                // Process single month
                const result = await backfillYahooFuturesMonth(env, ticker, month);
                return Response.json({
                    ok: true,
                    backfill_futures: result
                });
            } else {
                // Return list of months to process
                const yearsBack = parseInt(url.searchParams.get("years") || "5");
                const months = [];
                const now = new Date();

                for (let i = 0; i < yearsBack * 12; i++) {
                    const d = new Date(now);
                    d.setUTCMonth(d.getUTCMonth() - i);
                    const m = `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}`;
                    months.push(m);
                }

                return Response.json({
                    ok: true,
                    message: "Use ?month=YYYY-MM to backfill one month at a time",
                    ticker,
                    months_to_process: months,
                    example: `/backfill-futures?ticker=${ticker}&month=${months[months.length - 1]}`,
                    total_months: months.length
                });
            }
        }

        // Backfill intraday data (4h, 1h, 15m, 5m) into existing daily files
        if (request.method === "POST" && url.pathname === "/backfill-intraday") {
            const ticker = url.searchParams.get("ticker") || "MNQ";
            const daysBack = parseInt(url.searchParams.get("days") || "7");

            const result = await backfillYahooIntraday(env, ticker, daysBack);

            return Response.json({
                ok: true,
                backfill_intraday: result
            });
        }

        // Sanity check - verify backfilled data
        if (request.method === "GET" && url.pathname === "/sanity-check") {
            const startStr = url.searchParams.get("start") || "2024-01-01";
            const endStr = url.searchParams.get("end") || "2025-12-16";
            const sampleCount = parseInt(url.searchParams.get("samples") || "10");
            const deepVerify = url.searchParams.get("deep") === "true";

            const results = await runSanityCheck(env, startStr, endStr, sampleCount, deepVerify);
            return Response.json({ ok: true, sanity_check: results });
        }

        // =========================
        // RITHMIC RAW DATA CAPTURE
        // =========================
        // POST /rithmic-raw - Save raw websocket frame to R2 for learning
        // Body: { raw: "<base64 or text>", source: "apex", symbol: "MNQ", meta: {} }
        if (request.method === "POST" && url.pathname === "/rithmic-raw") {
            try {
                const body = await request.json();
                const { raw, source = "unknown", symbol = "MNQ", meta = {} } = body;

                if (!raw) {
                    return Response.json({ ok: false, error: "missing 'raw' field" }, { status: 400 });
                }

                // Build path: raw_rtmc/{symbol}/{YYYY}/{MM}/{DD}/{HH}/{timestamp}.json
                const now = new Date();
                const y = now.getUTCFullYear();
                const m = pad2(now.getUTCMonth() + 1);
                const d = pad2(now.getUTCDate());
                const h = pad2(now.getUTCHours());
                const ts = now.getTime();

                const key = `raw_rtmc/${symbol}/${y}/${m}/${d}/${h}/${ts}.json`;

                const payload = {
                    ts_utc: now.toISOString(),
                    source,
                    symbol,
                    raw,
                    raw_length: raw.length,
                    meta
                };

                await env.TAPE_DATA_FUTURES.put(
                    key,
                    JSON.stringify(payload, null, 2),
                    { httpMetadata: { contentType: "application/json" } }
                );

                return Response.json({
                    ok: true,
                    saved: key,
                    size: raw.length,
                    ts: now.toISOString()
                });
            } catch (err) {
                return Response.json({ ok: false, error: err.message }, { status: 500 });
            }
        }

        // POST /rithmic-raw-bulk - Save multiple raw frames at once
        if (request.method === "POST" && url.pathname === "/rithmic-raw-bulk") {
            try {
                const body = await request.json();
                const { frames = [], source = "unknown", symbol = "MNQ" } = body;

                if (!Array.isArray(frames) || frames.length === 0) {
                    return Response.json({ ok: false, error: "missing or empty 'frames' array" }, { status: 400 });
                }

                const now = new Date();
                const y = now.getUTCFullYear();
                const m = pad2(now.getUTCMonth() + 1);
                const d = pad2(now.getUTCDate());
                const h = pad2(now.getUTCHours());
                const mm = pad2(now.getUTCMinutes());

                // Save as JSONL (one frame per line)
                const key = `raw_rtmc/${symbol}/${y}/${m}/${d}/${h}/${now.getTime()}_bulk.jsonl`;

                const lines = frames.map((frame, idx) => JSON.stringify({
                    idx,
                    ts_utc: now.toISOString(),
                    source,
                    symbol,
                    raw: frame.raw || frame,
                    raw_length: (frame.raw || frame).length,
                    meta: frame.meta || {}
                }));

                await env.TAPE_DATA_FUTURES.put(
                    key,
                    lines.join("\n") + "\n",
                    { httpMetadata: { contentType: "application/x-ndjson" } }
                );

                return Response.json({
                    ok: true,
                    saved: key,
                    count: frames.length,
                    ts: now.toISOString()
                });
            } catch (err) {
                return Response.json({ ok: false, error: err.message }, { status: 500 });
            }
        }

        // GET /rithmic-raw/list - List saved raw frames
        if (request.method === "GET" && url.pathname === "/rithmic-raw/list") {
            try {
                const symbol = url.searchParams.get("symbol") || "MNQ";
                const date = url.searchParams.get("date"); // YYYY-MM-DD format

                let prefix = `raw_rtmc/${symbol}/`;
                if (date) {
                    const [y, m, d] = date.split("-");
                    prefix = `raw_rtmc/${symbol}/${y}/${m}/${d}/`;
                }

                const list = await env.TAPE_DATA_FUTURES.list({ prefix, limit: 100 });

                return Response.json({
                    ok: true,
                    prefix,
                    count: list.objects.length,
                    files: list.objects.map(o => ({
                        key: o.key,
                        size: o.size,
                        uploaded: o.uploaded
                    }))
                });
            } catch (err) {
                return Response.json({ ok: false, error: err.message }, { status: 500 });
            }
        }

        // GET /rithmic-raw/view - View a specific raw frame
        if (request.method === "GET" && url.pathname === "/rithmic-raw/view") {
            try {
                const key = url.searchParams.get("key");
                if (!key) {
                    return Response.json({ ok: false, error: "missing 'key' param" }, { status: 400 });
                }

                const obj = await env.TAPE_DATA_FUTURES.get(key);
                if (!obj) {
                    return Response.json({ ok: false, error: "not found" }, { status: 404 });
                }

                const content = await obj.text();
                return new Response(content, {
                    headers: { "Content-Type": obj.httpMetadata?.contentType || "application/json" }
                });
            } catch (err) {
                return Response.json({ ok: false, error: err.message }, { status: 500 });
            }
        }

        return Response.json({
            ok: true,
            service: "fut-fetchers",
            endpoints: [
                "POST /run",
                "POST /backfill?start=YYYY-MM-DD&end=YYYY-MM-DD",
                "POST /backfill-futures?ticker=MNQ&years=5",
                "POST /backfill-intraday?ticker=MNQ&days=7",
                "GET /sanity-check?start=YYYY-MM-DD&end=YYYY-MM-DD&samples=N&deep=true",
                "--- RITHMIC RAW CAPTURE ---",
                "POST /rithmic-raw  (body: {raw, source, symbol, meta})",
                "POST /rithmic-raw-bulk  (body: {frames[], source, symbol})",
                "GET /rithmic-raw/list?symbol=MNQ&date=YYYY-MM-DD",
                "GET /rithmic-raw/view?key=..."
            ],
            note: "Cron runs hourly. /backfill-intraday ingests 4h/1h/15m/5m into existing files",
            rithmic_note: "raw_rtmc/ folder in R2 stores captured Rithmic websocket data for learning"
        });
    },

    async scheduled(event, env, ctx) {
        ctx.waitUntil((async () => {
            // 1. Fetch raw data
            await runCron(env);

            // 2. Trigger consolidation (piggyback on this cron)
            try {
                const now = new Date();
                const currentMonth = `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}`;

                // Call fut-features consolidate endpoint
                const consolidateUrl = `https://fut-features.mkemalw.workers.dev/consolidate?month=${currentMonth}`;
                const response = await fetch(consolidateUrl, { method: 'POST' });

                if (response.ok) {
                    console.log(`Auto-consolidated features for ${currentMonth}`);
                } else {
                    console.error(`Failed to consolidate ${currentMonth}: HTTP ${response.status}`);
                }
            } catch (err) {
                console.error('Consolidation error:', err.message);
            }
        })());
    }
};
