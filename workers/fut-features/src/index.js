// fut-features/src/index.js
// Consolidates raw timeseries data into monthly feature files with z-scores

// =============================
// HELPER FUNCTIONS
// =============================

function pad2(n) { return String(n).padStart(2, "0"); }

// Parse YYYY-MM format to year and month
function parseMonth(monthStr) {
    const [year, month] = monthStr.split('-').map(Number);
    return { year, month };
}

// Get all days in a month
function getDaysInMonth(year, month) {
    const days = [];
    const date = new Date(Date.UTC(year, month - 1, 1));
    const lastDay = new Date(Date.UTC(year, month, 0)).getUTCDate();

    for (let day = 1; day <= lastDay; day++) {
        days.push(new Date(Date.UTC(year, month - 1, day)));
    }
    return days;
}

// Generate R2 key for raw daily data
function rawDailyKey(instrument, date) {
    const y = String(date.getUTCFullYear());
    const m = pad2(date.getUTCMonth() + 1);
    const d = `${y}${m}${pad2(date.getUTCDate())}`;
    return `raw_${instrument}/daily/${y}/${m}/${d}.json`;
}

// Generate R2 key for consolidated features
function featuresKey(year, month) {
    return `features/${year}${pad2(month)}.json`;
}

// =============================
// Z-SCORE CALCULATION
// =============================

function calculateZScore(series, index, window) {
    if (index < 0 || index >= series.length) return null;

    const start = Math.max(0, index - window + 1);
    const values = series.slice(start, index + 1).filter(v => v !== null && Number.isFinite(v));

    if (values.length < 2) return null; // Need at least 2 values

    const mean = values.reduce((a, b) => a + b, 0) / values.length;
    const variance = values.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / values.length;
    const stdDev = Math.sqrt(variance);

    if (stdDev === 0) return 0;

    const currentValue = series[index];
    if (currentValue === null || !Number.isFinite(currentValue)) return null;

    return (currentValue - mean) / stdDev;
}

// Calculate all z-scores for an instrument across timeframe
function calculateAllZScores(values, windows = [20, 50, 100]) {
    const result = [];

    for (let i = 0; i < values.length; i++) {
        const zscores = {};
        for (const window of windows) {
            zscores[`z_${window}`] = calculateZScore(values, i, window);
        }
        result.push(zscores);
    }

    return result;
}

// =============================
// DATA FETCHING
// =============================

async function fetchRawDailyData(env, instrument, date) {
    const key = rawDailyKey(instrument, date);

    try {
        const obj = await env.TAPE_DATA_FUTURES.get(key);
        if (!obj) return null;

        const data = JSON.parse(await obj.text());
        return data;
    } catch (err) {
        console.error(`Error fetching ${key}:`, err.message);
        return null;
    }
}

async function fetchMonthRawData(env, year, month, instruments) {
    const days = getDaysInMonth(year, month);
    const results = [];

    for (const date of days) {
        const dateStr = `${date.getUTCFullYear()}-${pad2(date.getUTCMonth() + 1)}-${pad2(date.getUTCDate())}`;
        const record = {
            date: dateStr,
            timestamp: date.toISOString()
        };

        // Fetch data for each instrument
        for (const inst of instruments) {
            const data = await fetchRawDailyData(env, inst, date);

            if (data) {
                if (inst === 'DP_PROXY') {
                    record[inst] = data.short_ratio;
                } else {
                    record[inst] = data.value;
                }
            } else {
                record[inst] = null;
            }
        }

        results.push(record);
    }

    return results;
}

// =============================
// CONSOLIDATION
// =============================

async function consolidateMonth(env, monthStr) {
    const { year, month } = parseMonth(monthStr);
    const instruments = ['US10Y', 'VIX', 'DP_PROXY'];
    const windows = [20, 50, 100];

    // Fetch raw data for the month
    const rawData = await fetchMonthRawData(env, year, month, instruments);

    // Calculate z-scores for each instrument
    const dailyTimeseries = [];

    // Extract value series for each instrument
    const series = {};
    for (const inst of instruments) {
        series[inst] = rawData.map(r => r[inst]);
    }

    // Calculate z-scores
    const zscores = {};
    for (const inst of instruments) {
        zscores[inst] = calculateAllZScores(series[inst], windows);
    }

    // Build daily timeseries with z-scores
    for (let i = 0; i < rawData.length; i++) {
        const record = { ...rawData[i] };

        // Add z-scores for each instrument
        for (const inst of instruments) {
            for (const window of windows) {
                const zKey = `z_${inst}_${window}`;
                record[zKey] = zscores[inst][i][`z_${window}`];
            }
        }

        dailyTimeseries.push(record);
    }

    // Build consolidated JSON
    const consolidated = {
        meta: {
            month: monthStr,
            instruments,
            timeframes: ['daily', '15m', '5m', '1m'],
            zscore_windows: windows,
            count: {
                daily: dailyTimeseries.length,
                '15m': 0,
                '5m': 0,
                '1m': 0
            },
            generated_at: new Date().toISOString()
        },
        timeseries: {
            daily: dailyTimeseries,
            '15m': [],
            '5m': [],
            '1m': []
        }
    };

    // Write to R2
    const key = featuresKey(year, month);
    await env.TAPE_DATA_FUTURES.put(
        key,
        JSON.stringify(consolidated, null, 2),
        { httpMetadata: { contentType: 'application/json' } }
    );

    return {
        ok: true,
        month: monthStr,
        key,
        count: dailyTimeseries.length
    };
}

// =============================
// BATCH CONSOLIDATION
// =============================

async function consolidateRange(env, startMonth, endMonth) {
    const start = parseMonth(startMonth);
    const end = parseMonth(endMonth);

    const results = [];
    let current = new Date(Date.UTC(start.year, start.month - 1, 1));
    const endDate = new Date(Date.UTC(end.year, end.month - 1, 1));

    while (current <= endDate) {
        const monthStr = `${current.getUTCFullYear()}-${pad2(current.getUTCMonth() + 1)}`;

        try {
            const result = await consolidateMonth(env, monthStr);
            results.push(result);
        } catch (err) {
            results.push({
                ok: false,
                month: monthStr,
                error: err.message
            });
        }

        // Next month
        current.setUTCMonth(current.getUTCMonth() + 1);
    }

    return results;
}

// =============================
// FUTURES CONSOLIDATION (DAILY)
// =============================

async function consolidateFuturesDaily(env, ticker, dateStr) {
    // Parse date (YYYY-MM-DD format)
    const date = new Date(dateStr + 'T00:00:00Z');
    const year = date.getUTCFullYear();
    const month = pad2(date.getUTCMonth() + 1);
    const day = pad2(date.getUTCDate());
    const yyyymmdd = `${year}${month}${day}`;

    // Fetch raw daily data
    const rawKey = `raw_${ticker}/daily/${year}/${month}/${yyyymmdd}.json`;
    const obj = await env.TAPE_DATA_FUTURES.get(rawKey);

    if (!obj) {
        return {
            ok: false,
            ticker,
            date: dateStr,
            error: 'Raw data not found'
        };
    }

    const rawData = JSON.parse(await obj.text());

    // Build consolidated JSON (1 day = 1 file)
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
                timestamp: rawData.ts_utc || date.toISOString(),
                open: rawData.open,
                high: rawData.high,
                low: rawData.low,
                close: rawData.close,
                volume: rawData.volume
            }],
            "4h": [],
            "1h": [],
            "15m": [],
            "5m": [],
            "1m": []
        }
    };

    // Write to R2 as ticker/YYYYMMDD.json
    const key = `${ticker}/${yyyymmdd}.json`;
    await env.TAPE_DATA_FUTURES.put(
        key,
        JSON.stringify(consolidated, null, 2),
        { httpMetadata: { contentType: 'application/json' } }
    );

    return {
        ok: true,
        ticker,
        date: dateStr,
        key,
        bars: 1
    };
}



// =============================
// FETCH HANDLER
// =============================

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
                worker: 'fut-features',
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

        // Manual trigger (called by cron-checker)
        if (request.method === "POST" && url.pathname === "/run") {
            const now = new Date();
            const currentMonth = `${now.getUTCFullYear()}-${pad2(now.getUTCMonth() + 1)}`;
            const prevDate = new Date(now);
            prevDate.setUTCMonth(prevDate.getUTCMonth() - 1);
            const prevMonth = `${prevDate.getUTCFullYear()}-${pad2(prevDate.getUTCMonth() + 1)}`;

            const results = { currentMonth: null, prevMonth: null };

            try {
                await consolidateMonth(env, currentMonth);
                results.currentMonth = 'success';
            } catch (err) {
                results.currentMonth = err.message;
            }

            try {
                await consolidateMonth(env, prevMonth);
                results.prevMonth = 'success';
            } catch (err) {
                results.prevMonth = err.message;
            }

            return Response.json({ ok: true, results });
        }

        // GET features file
        if (request.method === 'GET' && url.pathname.startsWith('/features/')) {
            // Extract path like /features/202412.json
            const key = url.pathname.slice(1); // Remove leading /

            const obj = await env.TAPE_DATA_FUTURES.get(key);
            if (!obj) {
                return Response.json({
                    ok: false,
                    error: 'Features file not found'
                }, { status: 404 });
            }

            return new Response(obj.body, {
                headers: { 'Content-Type': 'application/json' }
            });
        }

        // Consolidate single month
        if (request.method === 'POST' && url.pathname === '/consolidate') {
            const month = url.searchParams.get('month');

            if (!month || !/^\d{4}-\d{2}$/.test(month)) {
                return Response.json({
                    ok: false,
                    error: 'Invalid month format. Use YYYY-MM'
                }, { status: 400 });
            }

            const result = await consolidateMonth(env, month);
            return Response.json(result);
        }

        // Consolidate range of months
        if (request.method === 'POST' && url.pathname === '/consolidate-all') {
            const start = url.searchParams.get('start');
            const end = url.searchParams.get('end');

            if (!start || !end) {
                return Response.json({
                    ok: false,
                    error: 'Missing start or end parameter'
                }, { status: 400 });
            }

            const results = await consolidateRange(env, start, end);
            const success = results.filter(r => r.ok).length;

            return Response.json({
                ok: true,
                total: results.length,
                success,
                failed: results.length - success,
                results
            });
        }

        // Sanity check - verify consolidated features
        if (request.method === 'GET' && url.pathname === '/sanity-check') {
            const month = url.searchParams.get('month') || '2024-12';
            const sampleCount = parseInt(url.searchParams.get('samples') || '5');

            try {
                const { year, month: m } = parseMonth(month);
                const key = featuresKey(year, m);

                // Get consolidated features
                const obj = await env.TAPE_DATA_FUTURES.get(key);
                if (!obj) {
                    return Response.json({
                        ok: false,
                        error: 'Features file not found',
                        month
                    }, { status: 404 });
                }

                const features = JSON.parse(await obj.text());
                const daily = features.timeseries.daily;

                // Sample random dates
                const validDates = daily.filter(d => d.US10Y !== null || d.VIX !== null || d.DP_PROXY !== null);
                const sampleSize = Math.min(sampleCount, validDates.length);
                const samples = [];

                for (let i = 0; i < sampleSize; i++) {
                    const idx = Math.floor(Math.random() * validDates.length);
                    samples.push(validDates[idx]);
                }

                // Verify against raw data
                const verification = [];
                for (const sample of samples) {
                    const date = new Date(sample.date + 'T00:00:00Z');

                    // Fetch raw data for verification
                    const rawUS10Y = await fetchRawDailyData(env, 'US10Y', date);
                    const rawVIX = await fetchRawDailyData(env, 'VIX', date);
                    const rawDP = await fetchRawDailyData(env, 'DP_PROXY', date);

                    const check = {
                        date: sample.date,
                        US10Y: {
                            consolidated: sample.US10Y,
                            raw: rawUS10Y?.value || null,
                            match: sample.US10Y === (rawUS10Y?.value || null)
                        },
                        VIX: {
                            consolidated: sample.VIX,
                            raw: rawVIX?.value || null,
                            match: sample.VIX === (rawVIX?.value || null)
                        },
                        DP_PROXY: {
                            consolidated: sample.DP_PROXY,
                            raw: rawDP?.short_ratio || null,
                            match: sample.DP_PROXY === (rawDP?.short_ratio || null)
                        }
                    };

                    verification.push(check);
                }

                // Summary
                const allChecks = verification.flatMap(v => [v.US10Y, v.VIX, v.DP_PROXY]);
                const matches = allChecks.filter(c => c.match).length;
                const total = allChecks.length;

                return Response.json({
                    ok: true,
                    month,
                    summary: {
                        total_checks: total,
                        matches,
                        mismatches: total - matches,
                        accuracy: total > 0 ? ((matches / total) * 100).toFixed(1) + '%' : '0%'
                    },
                    samples: verification
                });

            } catch (err) {
                return Response.json({
                    ok: false,
                    error: err.message
                }, { status: 500 });
            }
        }

        // Consolidate futures (MNQ, MGC) - DAILY
        if (request.method === 'POST' && url.pathname === '/consolidate-futures') {
            const ticker = url.searchParams.get('ticker'); // MNQ or MGC
            const date = url.searchParams.get('date'); // YYYY-MM-DD

            if (!ticker || !date) {
                return Response.json({
                    ok: false,
                    error: 'Missing ticker or date parameter'
                }, { status: 400 });
            }

            if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
                return Response.json({
                    ok: false,
                    error: 'Invalid date format. Use YYYY-MM-DD'
                }, { status: 400 });
            }

            const result = await consolidateFuturesDaily(env, ticker, date);
            return Response.json(result);
        }

        // Service info
        return Response.json({
            ok: true,
            status: 'OK', // Added for dashboard compatibility
            service: 'fut-features',
            endpoints: [
                'GET /schedule',
                'POST /run',
                'POST /consolidate?month=YYYY-MM',
                'POST /consolidate-all?start=YYYY-MM&end=YYYY-MM',
                'POST /consolidate-futures?ticker=MNQ&date=YYYY-MM-DD',
                'GET /features/{YYYYMM}.json',
                'GET /sanity-check?month=YYYY-MM&samples=N'
            ],
            note: 'Consolidates raw timeseries into monthly features with z-scores. Triggered by cron-checker every 15 minutes.'
        });
    },

    async scheduled(event, env, ctx) {
        // Auto-consolidate current and previous month
        const now = new Date();
        const currentMonth = `${now.getUTCFullYear()}-${pad2(now.getUTCMonth() + 1)}`;

        // Also consolidate previous month in case of late data
        const prevDate = new Date(now);
        prevDate.setUTCMonth(prevDate.getUTCMonth() - 1);
        const prevMonth = `${prevDate.getUTCFullYear()}-${pad2(prevDate.getUTCMonth() + 1)}`;

        // Run consolidation asynchronously
        ctx.waitUntil((async () => {
            try {
                await consolidateMonth(env, currentMonth);
                console.log(`Auto-consolidated ${currentMonth}`);
            } catch (err) {
                console.error(`Failed to consolidate ${currentMonth}:`, err.message);
            }

            try {
                await consolidateMonth(env, prevMonth);
                console.log(`Auto-consolidated ${prevMonth}`);
            } catch (err) {
                console.error(`Failed to consolidate ${prevMonth}:`, err.message);
            }
        })());
    }
};
