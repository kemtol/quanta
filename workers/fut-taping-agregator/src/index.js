// fut-taping-agregator/src/index.js
export default {
    async fetch(request, env, ctx) {
        const url = new URL(request.url);
        const path = url.pathname;

        // Health check
        if (path === "/" || path === "/health") {
            return Response.json({ status: "OK", service: "fut-taping-agregator" });
        }

        // --- HOUSEKEEPING MANUAL TRIGGERS ---
        if (request.method === "POST" && path === "/compress") {
            const date = url.searchParams.get("date");
            if (!date) return new Response("Missing ?date=YYYY-MM-DD", { status: 400 });
            try {
                const result = await compressAndPruneRawTns(env, date);
                return Response.json(result);
            } catch (e) {
                return Response.json({ error: e.message }, { status: 500 });
            }
        }

        if (request.method === "POST" && path === "/prune") {
            const retention = parseInt(url.searchParams.get("retention") || "7", 10);
            try {
                const result = await deleteOldRawTns(env, retention);
                return Response.json(result);
            } catch (e) {
                return Response.json({ error: e.message }, { status: 500 });
            }
        }

        if (request.method === "POST" && path === "/run-cron") {
            const force = url.searchParams.get("force") === "true";
            const mode = url.searchParams.get("mode") || "all";

            // 1. Housekeeping (Daily/Backlog)
            if (mode === "all" || mode === "housekeeping") {
                ctx.waitUntil(runDailyCron(env, force));
            }

            // 2. Footprint Aggregation (Hourly/Minutely)
            if (mode === "all" || mode === "aggregation") {
                ctx.waitUntil(executeAggregation(env));
            }

            return Response.json({ message: "Cron triggered", mode, force });
        }

        // Patch existing footprint data to fill ladder gaps
        if (request.method === "POST" && path === "/patch-ladder") {
            const date = url.searchParams.get("date"); // YYYY-MM-DD
            const hour = url.searchParams.get("hour"); // optional, 00-23
            if (!date) return new Response("Missing ?date=YYYY-MM-DD", { status: 400 });

            try {
                const result = await patchLadderGaps(env, date, hour);
                return Response.json(result);
            } catch (e) {
                return Response.json({ error: e.message }, { status: 500 });
            }
        }

        // List all dates that have footprint data
        if (request.method === "GET" && path === "/list-footprint-dates") {
            const symbol = url.searchParams.get("symbol") || "ENQ";
            const tf = url.searchParams.get("tf") || "1m";

            try {
                const prefix = `footprint/${symbol}/${tf}/`;
                let listed = await env.DATA_LAKE.list({ prefix, limit: 1000 });
                let files = listed.objects;

                // Extract unique dates from file paths
                const dates = new Set();
                for (const obj of files) {
                    // Key: footprint/ENQ/1m/2025/12/19/15.jsonl
                    const parts = obj.key.split('/');
                    if (parts.length >= 7) {
                        const date = `${parts[3]}-${parts[4]}-${parts[5]}`;
                        dates.add(date);
                    }
                }

                return Response.json({
                    symbol,
                    tf,
                    dates: Array.from(dates).sort(),
                    count: dates.size
                });
            } catch (e) {
                return Response.json({ error: e.message }, { status: 500 });
            }
        }

        // Backup footprint data to compressed format before patching
        if (request.method === "POST" && path === "/backup-footprint") {
            const date = url.searchParams.get("date"); // YYYY-MM-DD
            if (!date) return new Response("Missing ?date=YYYY-MM-DD", { status: 400 });

            try {
                const result = await backupFootprintData(env, date);
                return Response.json(result);
            } catch (e) {
                return Response.json({ error: e.message }, { status: 500 });
            }
        }

        if (request.method === "GET" && path === "/r2-monitor-futures-comp") {
            const key = "raw_tns_compressed/sanity-info.json";
            const obj = await env.DATA_LAKE.get(key);
            if (obj) {
                return new Response(obj.body, {
                    headers: {
                        "Content-Type": "application/json",
                        "Cache-Control": "no-cache"
                    }
                });
            } else {
                return Response.json({ status: "EMPTY", message: "Sanity info not found" }, { status: 404 });
            }
        }

        if (request.method === "GET" && path === "/list-parts") {
            const symbol = url.searchParams.get("symbol") || "ENQ";
            const date = url.searchParams.get("date");
            if (!date) return new Response("Missing ?date=YYYY-MM-DD", { status: 400 });

            const prefix = `raw_tns_compressed/${symbol}/${date}_`;
            const listed = await env.DATA_LAKE.list({ prefix });

            const parts = listed.objects.map(obj => ({
                key: obj.key,
                size: obj.size,
                uploaded: obj.uploaded,
                verified: obj.customMetadata?.verified
            }));

            return Response.json({
                symbol,
                date,
                count: parts.length,
                parts
            });
        }

        if (request.method === "GET" && path === "/count-raw") {
            const symbol = url.searchParams.get("symbol") || "ENQ";
            const date = url.searchParams.get("date");
            if (!date) return new Response("Missing ?date=YYYY-MM-DD", { status: 400 });

            const [y, m, d] = date.split('-');
            const prefix = `raw_tns/${symbol}/${y}/${m}/${d}/`;

            let allFiles = [];
            let cursor;
            let truncated = true;

            while (truncated) {
                const listed = await env.DATA_LAKE.list({ prefix, cursor, limit: 1000 });
                allFiles.push(...listed.objects);
                cursor = listed.cursor;
                truncated = listed.truncated;
                if (allFiles.length > 10000) break; // Safety limit
            }

            return Response.json({
                symbol,
                date,
                prefix,
                count: allFiles.length,
                total_size: allFiles.reduce((sum, f) => sum + f.size, 0),
                sample: allFiles.slice(0, 5).map(f => f.key)
            });
        }

        if (request.method === "GET" && path.startsWith("/daily/")) {
            // Path: /daily/SYMBOL/YYYY/MM/DD (or just /daily/YYYY/MM/DD if symbol inferred?)
            // Let's support /daily/YYYY/MM/DD and default to ENQ, OR /daily/SYMBOL/YYYY/MM/DD
            // Current plan said: /daily/:y/:m/:d

            const pathParts = url.pathname.split('/').filter(Boolean);
            // expect ["daily", "2025", "12", "19"]
            if (pathParts.length !== 4) {
                return new Response("Invalid path for daily. Use /daily/YYYY/MM/DD", { status: 400 });
            }
            const [_, y, m, d] = pathParts;
            const symbol = "ENQ"; // Default for now, or read from query param?
            const dateStr = `${y}${m}${d}`;
            const key = `tape-data-futures/${symbol}/${dateStr}.json`;
            // WAIT - Screenshot showed "tape-data-futures / MNQ / ..." as directory in R2 dashboard?
            // "tape-data-futures" represents the bucket name in screenshot tab title?
            // "tape-data-futures / MNQ / " is the prefix.
            // So object key is `MNQ/20251216.json` inside the bucket.

            const r2Key = `${symbol}/${dateStr}.json`;

            try {
                const obj = await env.DATA_LAKE.get(r2Key);
                if (!obj) {
                    return new Response(JSON.stringify({ vol: 0, turnover: 0 }), {
                        headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" }
                    });
                }
                return new Response(obj.body, {
                    headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" }
                });
            } catch (e) {
                return new Response(e.message, { status: 500, headers: { "Access-Control-Allow-Origin": "*" } });
            }
        }

        // Manual/Default trigger for Aggregation
        // Example: /?symbol=ENQ&date=2025-12-17&hour=13
        let targetDate = new Date();
        if (url.searchParams.get("date")) {
            const dateStr = url.searchParams.get("date");
            const hourStr = url.searchParams.get("hour");
            if (dateStr && hourStr) {
                targetDate = new Date(`${dateStr}T${hourStr}:00:00Z`);
            }
        }

        const result = await executeAggregation(env, targetDate);
        return Response.json(result);
    },

    async scheduled(event, env, ctx) {
        console.log("[Cron] Triggered");
        // Run aggregation (footprint generation) every minute
        ctx.waitUntil(executeAggregation(env));
    }
};

// --- AGGREGATION LOGIC ---
async function executeAggregation(env, targetDate = new Date()) {
    const TIME_FRAME = "1m"; // Fixed for now
    const SYMBOL = "ENQ";    // Target symbol
    const BAR_MS = 60000;

    // Construct paths
    const y = targetDate.getUTCFullYear();
    const m = String(targetDate.getUTCMonth() + 1).padStart(2, '0');
    const d = String(targetDate.getUTCDate()).padStart(2, '0');
    const h = String(targetDate.getUTCHours()).padStart(2, '0');

    const rawPrefix = `raw_tns/${SYMBOL}/${y}/${m}/${d}/${h}`;
    const outputPath = `footprint/${SYMBOL}/${TIME_FRAME}/${y}/${m}/${d}/${h}.json`;

    console.log(`[Agregator] Processing ${rawPrefix} -> ${outputPath}`);

    // 1. List all raw files in that hour
    let listed = await env.DATA_LAKE.list({ prefix: rawPrefix });
    let files = listed.objects;
    while (listed.truncated) {
        listed = await env.DATA_LAKE.list({ prefix: rawPrefix, cursor: listed.cursor });
        files = [...files, ...listed.objects];
    }

    if (files.length === 0) {
        return { message: "No raw files found", prefix: rawPrefix };
    }

    console.log(`[Agregator] Found ${files.length} raw files`);

    // 2. Read and Aggregate
    // 2. Read and Aggregate (Parallel Batch Processing)
    const candles = {};
    let bestBid = 0;
    let bestAsk = 0;

    // Concurrency limit
    const BATCH_SIZE = 20;
    let processedCount = 0;

    // Helper to process a single file's content
    const processFileContent = (content) => {
        const lines = content.split('\n').filter(l => l.trim());
        for (const line of lines) {
            try {
                const wrapper = JSON.parse(line);
                const rawMsg = wrapper.raw;
                if (!rawMsg) continue;

                const payloads = rawMsg.split('\u001e').filter(p => p.trim());
                for (const p of payloads) {
                    const msg = JSON.parse(p);

                    // Quote Context
                    if (msg.target === 'RealTimeSymbolQuote') {
                        const quote = msg.arguments[0];
                        if (quote.symbol === `F.US.${SYMBOL}` || quote.symbol === SYMBOL) {
                            if (quote.bestBid) bestBid = quote.bestBid;
                            if (quote.bestAsk) bestAsk = quote.bestAsk;
                        }
                    }

                    // Trade Data
                    if (msg.target === 'RealTimeTradeLogWithSpeed') {
                        const tradeData = msg.arguments[1];
                        if (Array.isArray(tradeData)) {
                            for (const trade of tradeData) {
                                processTrade(trade, bestBid, bestAsk, candles, SYMBOL, BAR_MS);
                            }
                        }
                    }
                }
            } catch (e) { }
        }
    };

    // Process in batches
    for (let i = 0; i < files.length; i += BATCH_SIZE) {
        const batch = files.slice(i, i + BATCH_SIZE);
        const promises = batch.map(async (file) => {
            try {
                const object = await env.DATA_LAKE.get(file.key);
                if (!object) return;
                const content = await object.text();
                processFileContent(content);
            } catch (err) {
                console.error(`Failed to process ${file.key}:`, err);
            }
        });

        await Promise.all(promises);
        processedCount += batch.length;
        // console.log(`[Agregator] Processed ${processedCount}/${files.length}`);
    }

    // 3. Finalize and Output
    const resultCandles = [];
    const sortedTimes = Object.keys(candles).sort();

    for (const t of sortedTimes) {
        const c = candles[t];
        finalizeCandle(c);
        resultCandles.push(c);
    }

    // 4. Save to R2
    let saved = false;
    if (resultCandles.length > 0) {
        await env.DATA_LAKE.put(outputPath, JSON.stringify(resultCandles));

        // --- SANITY INFO ---
        const sanityPath = outputPath.replace('.json', '_sanity.json');
        const sanityData = {
            last_update: Date.now(),
            status: "OK",
            count: resultCandles.length,
            service: "fut-taping-agregator"
        };
        await env.DATA_LAKE.put(sanityPath, JSON.stringify(sanityData));
        saved = true;
        console.log(`[Agregator] Saved ${resultCandles.length} candles to ${outputPath}`);
    }

    return {
        status: saved ? "OK" : "NO_DATA_GENERATED",
        path: outputPath,
        candles: resultCandles.length
    };
}

// --- HOUSEKEEPING FUNCTIONS ---

async function runDailyCron(env, force = false) {
    const now = new Date();

    // DEBUG: Mark start
    await env.DATA_LAKE.put("raw_tns_compressed/sanity-info.json", JSON.stringify({
        last_update: Date.now(),
        status: "RUNNING",
        service: "fut-housekeeping"
    }));

    // 1. SAFETY CHECK: Ensure "Today's" data exists before closing out "Yesterday".
    const y = now.getUTCFullYear();
    const m = String(now.getUTCMonth() + 1).padStart(2, "0");
    const d = String(now.getUTCDate()).padStart(2, "0");
    const todayStr = `${y}-${m}-${d}`;

    if (!force) {
        const hasTodayData = await checkDataExistsForDate(env, todayStr);

        if (!hasTodayData) {
            console.log(`[Cron] Today (${todayStr}) has no data yet. Skipping housecleaning for safety.`);
            return;
        }
        console.log(`[Cron] Today (${todayStr}) has data. Proceeding to houseclean past days...`);
    } else {
        console.log(`[Cron] FORCE mode enabled. Skipping safety check for ${todayStr}.`);
    }

    // 2. ATOMIC BATCH PROCESS (Compress -> Verify -> Prune)
    const LOOKBACK_DAYS = 3;
    let yesterdayResult = null;

    for (let i = 1; i <= LOOKBACK_DAYS; i++) {
        const targetDate = new Date(now.getTime() - i * 24 * 60 * 60 * 1000);
        const tY = targetDate.getUTCFullYear();
        const tM = String(targetDate.getUTCMonth() + 1).padStart(2, "0");
        const tD = String(targetDate.getUTCDate()).padStart(2, "0");
        const targetStr = `${tY}-${tM}-${tD}`;

        console.log(`[Cron] Checking/Cleaning ${targetStr}...`);

        try {
            // Atomic function does compression AND pruning per batch
            const result = await compressAndPruneRawTns(env, targetStr);
            if (i === 1) yesterdayResult = result;

            if (result.results && result.results.length > 0) {
                console.log(`[Cron] Result for ${targetStr}:`, JSON.stringify(result));
            } else {
                console.log(`[Cron] ${targetStr} already clean or empty.`);
            }
        } catch (e) {
            console.error(`[Cron] Failed for ${targetStr}:`, e);
        }
    }

    // 3. SANITY INFO (GLOBAL)
    if (yesterdayResult && yesterdayResult.results) {
        const sanityPath = `raw_tns_compressed/sanity-info.json`;
        const totalCompressedFiles = yesterdayResult.results.reduce((acc, r) => acc + (r.processed || 0), 0);

        const sanityData = {
            last_update: Date.now(),
            service: "fut-housekeeping",
            target_date: yesterdayResult.date,
            status: "OK",
            details: {
                processed_files: totalCompressedFiles,
                mode: "atomic_batch_verify_delete"
            }
        };
        await env.DATA_LAKE.put(sanityPath, JSON.stringify(sanityData));
    }
}

async function checkDataExistsForDate(env, dateStr) {
    const [y, m, d] = dateStr.split('-');
    // Check ENQ as proxy
    const prefix = `raw_tns/ENQ/${y}/${m}/${d}/`;
    try {
        const listed = await env.DATA_LAKE.list({ prefix, limit: 1 });
        return listed.objects.length > 0;
    } catch (e) {
        console.warn("[CheckData] Failed to list R2:", e);
        return false;
    }
}

// ATOMIC BATCH: Compress -> Verify -> Delete (per chunk)
async function compressAndPruneRawTns(env, dateStr) {
    const [y, m, d] = dateStr.split('-');
    const SYMBOLS = ["ENQ", "MNQ", "MGC", "MIC", "MYM", "NES", "NQ", "ES", "YM"];
    const results = [];

    for (const symbol of SYMBOLS) {
        const prefix = `raw_tns/${symbol}/${y}/${m}/${d}/`;

        // 1. LIST ALL FILES (Pagination)
        let allFiles = [];
        let cursor;
        let truncated = true;

        while (truncated) {
            const listed = await env.DATA_LAKE.list({ prefix, cursor, limit: 1000 });
            allFiles.push(...listed.objects);
            cursor = listed.cursor;
            truncated = listed.truncated;
            if (allFiles.length > 20000) {
                console.warn(`[Compress] Huge file count for ${symbol} ${dateStr}, limit invoked.`);
                break;
            }
        }

        if (allFiles.length === 0) {
            continue;
        }

        // 2. PROCESS IN BATCHES
        const BATCH_SIZE = 250;
        const MAX_BATCHES_PER_RUN = 3; // Limit to avoid rate limiting
        let processedFilesCount = 0;
        let partSeq = 0;

        for (let i = 0; i < allFiles.length; i += BATCH_SIZE) {
            partSeq++;
            const batch = allFiles.slice(i, i + BATCH_SIZE);
            const outputKey = `raw_tns_compressed/${symbol}/${dateStr}_part${partSeq}.jsonl.gz`;

            // Skip if already exists?
            // If part exists, we assume it's done? 
            // We can check metadata `verified: true`.
            const head = await env.DATA_LAKE.head(outputKey);
            if (head && head.customMetadata && head.customMetadata.verified === "true") {
                // If verified, we can check if raw files still exist and delete them?
                // This handles "crash recovery".
                // But we need to know WHICH raw files belong to this part.
                // We don't track that mapping persistently.
                // So, standard behavior: we assume if verified part exists, raw files MIGHT be gone.
                // If they are not gone, we will re-process them? 
                // Wait, if we re-process, we'll create a NEW part (duplicates?).
                // Ideally we should delete raw files if verified part exists.
                // But we don't know if `batch` == `partSeq` content.
                // Simple logic: We proceed to compress. If content matches existing part, we are good?
                // Too complex.
                // ATOMIC LOGIC: We always compress. We overwrite part. We verify. We delete raw.
                // R2 overwrite is cheap. Safety is key.
            }

            // Step A: Read & Concat (Parallel with Concurrency Control)
            const allLines = [];
            let totalSize = 0;
            const batchKeys = [];

            const SUB_BATCH_SIZE = 25; // Control concurrency (25 requests at once)
            for (let j = 0; j < batch.length; j += SUB_BATCH_SIZE) {
                const subBatch = batch.slice(j, j + SUB_BATCH_SIZE);

                // Fetch in parallel
                const promises = subBatch.map(async (f) => {
                    try {
                        const obj = await env.DATA_LAKE.get(f.key);
                        if (!obj) return null;
                        const text = await obj.text();
                        return { text: text.trim(), size: f.size, key: f.key };
                    } catch (e) {
                        console.warn(`[Compress] Failed to read ${f.key}:`, e);
                        return null; // Skip failed file
                    }
                });

                const results = await Promise.all(promises);

                // Collect successful results
                for (const res of results) {
                    if (res && res.text) {
                        allLines.push(res.text);
                        totalSize += res.size;
                        batchKeys.push(res.key);
                    }
                }
            }

            if (allLines.length === 0) continue;
            const combined = allLines.join('\n');

            // Step B: Signature
            const rawLength = combined.length;
            const first100 = combined.slice(0, 100);
            const last100 = combined.slice(-100);

            // Step C: Compress
            const stream = new ReadableStream({
                start(controller) {
                    controller.enqueue(new TextEncoder().encode(combined));
                    controller.close();
                }
            });
            const compressedStream = stream.pipeThrough(new CompressionStream('gzip'));
            const reader = compressedStream.getReader();
            const chunks = [];
            while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                chunks.push(value);
            }
            const totalLength = chunks.reduce((acc, c) => acc + c.length, 0);
            const compressedData = new Uint8Array(totalLength);
            let offset = 0;
            for (const c of chunks) {
                compressedData.set(c, offset);
                offset += c.length;
            }

            // Step D: Upload & Verify
            let verified = false;
            try {
                // Upload (Temp)
                await env.DATA_LAKE.put(outputKey, compressedData, {
                    httpMetadata: { contentType: 'application/gzip' },
                    customMetadata: { verified: "false", batch_size: String(batch.length) }
                });

                // Read Back
                const uploadedObj = await env.DATA_LAKE.get(outputKey);
                const uploadedStream = uploadedObj.body.pipeThrough(new DecompressionStream('gzip'));
                const decompressedText = await new Response(uploadedStream).text();

                if (decompressedText.length === rawLength &&
                    decompressedText.slice(0, 100) === first100 &&
                    decompressedText.slice(-100) === last100) {

                    verified = true;
                    // Tag Verified
                    await env.DATA_LAKE.put(outputKey, compressedData, {
                        httpMetadata: { contentType: 'application/gzip' },
                        customMetadata: { verified: "true", raw_len: String(rawLength) }
                    });
                } else {
                    console.error(`[Integrity] Mismatch part${partSeq}`);
                    await env.DATA_LAKE.delete(outputKey); // Cleanup bad file
                }
            } catch (e) {
                console.error(`[Integrity] Err part${partSeq}:`, e);
            }

            // Step E: ATOMIC DELETE
            if (verified) {
                if (batchKeys.length > 0) {
                    await env.DATA_LAKE.delete(batchKeys);
                    processedFilesCount += batchKeys.length;
                    console.log(`[Compress] Batch ${partSeq} verified & deleted (${batchKeys.length} files).`);
                }
            }

            // LIMIT: Stop after MAX_BATCHES_PER_RUN to avoid rate limiting
            if (partSeq >= MAX_BATCHES_PER_RUN) {
                console.log(`[Compress] Reached batch limit (${MAX_BATCHES_PER_RUN}), stopping. Remaining files: ${allFiles.length - (i + BATCH_SIZE)}`);
                break;
            }
        }

        results.push({
            symbol,
            status: "done_atomic",
            totalFiles: allFiles.length,
            processed: processedFilesCount,
            parts: partSeq
        });
    }

    return { date: dateStr, results };
}


function processTrade(trade, currentBid, currentAsk, candles, symbol, barMs) {
    const ts = new Date(trade.timestamp).getTime();
    if (!Number.isFinite(ts)) return;

    const bucketTs = Math.floor(ts / barMs) * barMs;

    // Init Candle
    if (!candles[bucketTs]) {
        const t0 = new Date(bucketTs);
        const t1 = new Date(bucketTs + barMs);

        candles[bucketTs] = {
            v: 1,
            symbol: `F.US.${symbol}`,
            tick: 0.25,
            bar_ms: barMs,
            t0: t0.toISOString(),
            t1: t1.toISOString(),
            time: bucketTs,
            ohlc: { o: trade.price, h: trade.price, l: trade.price, c: trade.price },
            vol: 0,
            delta: 0,
            total_bid: 0,
            total_ask: 0,
            profile: {},
            _seen: new Set() // ⬅️ DEDUP PER BAR
        };
    }

    const c = candles[bucketTs];

    // ---- DEDUP (WAJIB) ----
    const dedupKey = `${trade.timestamp}|${trade.price}|${trade.volume}|${trade.type}`;
    if (c._seen.has(dedupKey)) return;
    c._seen.add(dedupKey);

    // ---- OHLC ----
    c.ohlc.h = Math.max(c.ohlc.h, trade.price);
    c.ohlc.l = Math.min(c.ohlc.l, trade.price);
    c.ohlc.c = trade.price;
    c.vol += trade.volume;

    // ---- AGGRESSOR LOGIC (ROBUST) ----
    const tick = c.tick;
    const eps = tick / 2;

    let isBuy;
    const haveL1 = Number.isFinite(currentBid) && Number.isFinite(currentAsk) && currentBid > 0 && currentAsk > 0;

    if (haveL1 && trade.price >= (currentAsk - eps)) {
        isBuy = true;              // lift ask
    } else if (haveL1 && trade.price <= (currentBid + eps)) {
        isBuy = false;             // hit bid
    } else {
        // 🔒 VERIFIED FOR TRADOVATE / TOPSTEPX
        // type = 0 → BUY, type = 1 → SELL
        isBuy = (trade.type === 0);
    }

    // ---- PRICE LEVEL (QUANTIZE KE TICK) ----
    const p = Math.round(trade.price / tick) * tick;
    const pStr = p.toFixed(2);

    if (!c.profile[pStr]) c.profile[pStr] = { bid: 0, ask: 0 };

    if (isBuy) {
        c.delta += trade.volume;
        c.total_ask += trade.volume;
        c.profile[pStr].ask += trade.volume;
    } else {
        c.delta -= trade.volume;
        c.total_bid += trade.volume;
        c.profile[pStr].bid += trade.volume;
    }
}

function finalizeCandle(c) {
    let maxVol = -1;
    let pocPrice = 0;
    const levels = [];

    const tick = c.tick || 0.25;

    // keys are strings like "25101.50"
    const priceKeys = Object.keys(c.profile)
        .map(k => Number(k))
        .filter(n => Number.isFinite(n))
        .sort((a, b) => a - b);

    // Build profile map for O(1) lookup
    const profileMap = new Map();
    for (const p of priceKeys) {
        const key = p.toFixed(2);
        const stats = c.profile[key];
        if (stats) {
            profileMap.set(p, stats);
            const total = stats.bid + stats.ask;
            if (total > maxVol) {
                maxVol = total;
                pocPrice = p;
            }
        }
    }

    // Generate continuous tick ladder from hi to lo (fill gaps with 0)
    if (priceKeys.length > 0) {
        const hi = priceKeys[priceKeys.length - 1];  // highest price
        const lo = priceKeys[0];  // lowest price

        for (let p = hi; p >= lo - 0.001; p -= tick) {
            const tickPrice = Math.round(p * 1000) / 1000; // avoid float errors
            const stats = profileMap.get(tickPrice);
            if (stats) {
                levels.push([tickPrice, stats.bid, stats.ask]);
            } else {
                // Fill gap with 0
                levels.push([tickPrice, 0, 0]);
            }
        }
    }

    c.poc = pocPrice;
    c.levels = levels;

    // Integrity Assertions
    const sumVol = c.total_ask + c.total_bid;
    const calcDelta = c.total_ask - c.total_bid;

    if (sumVol !== c.vol) {
        console.warn(`[Integrity Fail] Vol Mismatch! Vol:${c.vol} Sum:${sumVol} @ ${c.t0}`);
        c.integrity_error = "vol_mismatch";
    }

    if (calcDelta !== c.delta) {
        console.warn(`[Integrity Fail] Delta Mismatch! Delta:${c.delta} Calc:${calcDelta} @ ${c.t0}`);
        c.integrity_error = "delta_mismatch";
    }

    delete c.profile;
    delete c.time;
    delete c._seen;
}

// Patch existing footprint files to fill ladder gaps with 0 volume
async function patchLadderGaps(env, dateStr, hour = null) {
    const SYMBOL = "ENQ";
    const TF = "1m";
    const TICK = 0.25;

    const [y, m, d] = dateStr.split('-');

    // List hours to process
    const hours = hour ? [hour.padStart(2, '0')] :
        Array.from({ length: 24 }, (_, i) => String(i).padStart(2, '0'));

    const results = [];

    for (const h of hours) {
        const key = `footprint/${SYMBOL}/${TF}/${y}/${m}/${d}/${h}.jsonl`;

        try {
            const obj = await env.DATA_LAKE.get(key);
            if (!obj) {
                results.push({ hour: h, status: "not_found" });
                continue;
            }

            const text = await obj.text();
            const lines = text.split('\n').filter(l => l.trim());

            if (lines.length === 0) {
                results.push({ hour: h, status: "empty" });
                continue;
            }

            const patchedCandles = [];
            let patchedCount = 0;

            for (const line of lines) {
                try {
                    const candle = JSON.parse(line);
                    const patched = patchCandleLevels(candle, TICK);
                    patchedCandles.push(JSON.stringify(patched));
                    if (patched._patched) patchedCount++;
                } catch (e) {
                    // Keep original line if parse fails
                    patchedCandles.push(line);
                }
            }

            // Write back
            const newContent = patchedCandles.join('\n') + '\n';
            await env.DATA_LAKE.put(key, newContent, {
                httpMetadata: { contentType: 'application/x-ndjson' }
            });

            results.push({
                hour: h,
                status: "patched",
                candles: lines.length,
                patched: patchedCount
            });

        } catch (e) {
            results.push({ hour: h, status: "error", error: e.message });
        }
    }

    return { date: dateStr, symbol: SYMBOL, results };
}

// Patch a single candle's levels to fill gaps
function patchCandleLevels(candle, tick = 0.25) {
    if (!candle.levels || !Array.isArray(candle.levels) || candle.levels.length === 0) {
        return candle;
    }

    // Detect format: array [p, bd, av] or object {p, bv, av, ...}
    const isObjectFormat = typeof candle.levels[0] === 'object' && !Array.isArray(candle.levels[0]);

    // Extract prices
    const prices = candle.levels.map(l => isObjectFormat ? l.p : l[0]);
    const hi = Math.max(...prices);
    const lo = Math.min(...prices);

    // Build lookup map
    const levelMap = new Map();
    for (const level of candle.levels) {
        const price = isObjectFormat ? level.p : level[0];
        levelMap.set(Math.round(price * 1000) / 1000, level);
    }

    // Expected tick count
    const expectedTicks = Math.round((hi - lo) / tick) + 1;

    // If already continuous, no patch needed
    if (candle.levels.length >= expectedTicks) {
        return candle;
    }

    // Generate continuous ladder
    const newLevels = [];
    for (let p = hi; p >= lo - 0.001; p -= tick) {
        const tickPrice = Math.round(p * 1000) / 1000;
        const existing = levelMap.get(tickPrice);

        if (existing) {
            newLevels.push(existing);
        } else {
            // Fill gap with 0
            if (isObjectFormat) {
                newLevels.push({
                    p: tickPrice,
                    bv: 0,
                    av: 0,
                    bt: 0,
                    at: 0,
                    d: 0,
                    imb: 1,
                    abs: 0,
                    aas: 0
                });
            } else {
                newLevels.push([tickPrice, 0, 0]);
            }
        }
    }

    candle.levels = newLevels;
    candle._patched = true;

    return candle;
}

// Backup footprint data to gzip before patching
async function backupFootprintData(env, dateStr) {
    const SYMBOL = "ENQ";
    const TF = "1m";

    const [y, m, d] = dateStr.split('-');
    const hours = Array.from({ length: 24 }, (_, i) => String(i).padStart(2, '0'));

    let totalSize = 0;
    let hoursBacked = 0;
    const allContent = [];

    for (const h of hours) {
        const key = `footprint/${SYMBOL}/${TF}/${y}/${m}/${d}/${h}.jsonl`;

        try {
            const obj = await env.DATA_LAKE.get(key);
            if (!obj) continue;

            const text = await obj.text();
            allContent.push(`# Hour ${h}\n${text}`);
            totalSize += text.length;
            hoursBacked++;
        } catch (e) {
            console.warn(`Backup skip ${h}:`, e.message);
        }
    }

    if (allContent.length === 0) {
        return { date: dateStr, status: "no_data" };
    }

    // Combine all hours into one file
    const combined = allContent.join('\n');

    // Compress
    const stream = new ReadableStream({
        start(controller) {
            controller.enqueue(new TextEncoder().encode(combined));
            controller.close();
        }
    });
    const compressedStream = stream.pipeThrough(new CompressionStream('gzip'));
    const reader = compressedStream.getReader();
    const chunks = [];
    while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        chunks.push(value);
    }
    const totalLength = chunks.reduce((acc, c) => acc + c.length, 0);
    const compressedData = new Uint8Array(totalLength);
    let offset = 0;
    for (const c of chunks) {
        compressedData.set(c, offset);
        offset += c.length;
    }

    // Store backup
    const backupKey = `footprint_backup/${SYMBOL}/${TF}/${dateStr}.jsonl.gz`;
    await env.DATA_LAKE.put(backupKey, compressedData, {
        httpMetadata: { contentType: 'application/gzip' },
        customMetadata: {
            original_size: String(totalSize),
            hours: String(hoursBacked),
            backed_at: new Date().toISOString()
        }
    });

    return {
        date: dateStr,
        status: "backed_up",
        backup_key: backupKey,
        hours: hoursBacked,
        original_size: totalSize,
        compressed_size: totalLength
    };
}
