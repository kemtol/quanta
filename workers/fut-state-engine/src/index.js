import { DurableObject } from "cloudflare:workers";

export default {
    async fetch(request, env, ctx) {
        const id = env.TOPSTEPX_TAPER.idFromName("MAIN_TAPER");
        const stub = env.TOPSTEPX_TAPER.get(id);
        return stub.fetch(request);
    },
};

const TF_MAP = {
    '1m': 60000,
    '3m': 180000,
    '5m': 300000,
    '15m': 900000,
    '30m': 1800000,
    '1h': 3600000
};

// Large trade threshold (configurable per asset in future)
const LARGE_TRADE_THRESHOLD = 10;

export class TopstepXTaper extends DurableObject {
    constructor(ctx, env) {
        super(ctx, env);
        this.ctx = ctx;
        this.env = env;

        // Configuration
        this.SYMBOL = this.env.TAPER_SYMBOL || "ENQ";
        this.TIME_FRAME = this.env.TAPER_TIMEFRAME || "1m";
        this.TICK_SIZE = parseFloat(this.env.TAPER_TICK_SIZE || "0.25");

        // [Phase 2] Parse timeframe
        this.BAR_MS = TF_MAP[this.TIME_FRAME] || 60000;

        // [Phase 1] Tick decimals for output formatting only
        this.TICK_DECIMALS = (String(this.TICK_SIZE).split(".")[1] || "").length;

        // Settings
        this.DUAL_WRITE = true;
        this.FLUSH_RAW_INTERVAL = 5000;

        // State
        this.accessToken = "";
        this.ws = null;
        this.sessions = [];

        // Aggregation State
        this.currentCandle = null;
        this.lastPrice = 0;
        this.bestBid = 0;
        this.bestAsk = 0;

        // [Phase 1] Integer tick versions for accurate comparison
        this.bestBidPt = 0;
        this.bestAskPt = 0;

        // Raw Buffer
        this.rawBuffer = [];
        this.lastRawFlush = Date.now();

        // [Phase 3] Connectivity State Machine
        // States: DISCONNECTED, CONNECTING, HANDSHAKE_SENT, SUBSCRIBED
        this.wsState = 'DISCONNECTED';
        this.nextConnectAt = 0;
        this.reconnectDelayMs = 2000;
        this.invId = 0;

        // [Phase 3] Metrics
        this.metrics = {
            tradesProcessed: 0,
            parseErrors: 0,
            reconnects: 0,
            lateDropped: 0,
            candlesFlushed: 0,
            rawFlushed: 0,
            staleResets: 0
        };

        // [Phase 3.1] Subscribe confirmation tracking
        this.subs = { trade: false, quote: false };
        this.pendingInv = new Map();  // invocationId -> 'trade'|'quote'

        // [Phase 3.2] Liveness
        this.lastMsgAt = 0;
        this.LIVENESS_TIMEOUT_MS = 20000; // 20s

        // Restore token
        this.ctx.blockConcurrencyWhile(async () => {
            const token = await this.ctx.storage.get("access_token");
            if (token) this.accessToken = token;
        });

        this.ctx.storage.setAlarm(Date.now() + 1000);
    }

    // Helper: price to integer tick index (with float hardening)
    priceToTick(price) {
        return Math.round((price / this.TICK_SIZE) + 1e-9);
    }

    // Helper: bid price to tick (floor for conservative buy boundary)
    bidToPt(bid) {
        return Math.floor((bid / this.TICK_SIZE) + 1e-9);
    }

    // Helper: ask price to tick (ceil for conservative sell boundary)
    askToPt(ask) {
        return Math.ceil((ask / this.TICK_SIZE) - 1e-9);
    }

    // Helper: tick index to price (for output) - rounded to tick decimals
    tickToPrice(pt) {
        const x = pt * this.TICK_SIZE;
        return Number(x.toFixed(this.TICK_DECIMALS));
    }

    async fetch(request) {
        const url = new URL(request.url);

        if (url.pathname === "/token-status") return this.handleTokenStatus();
        if (url.pathname === "/update-token") return this.handleUpdateToken(url);
        if (url.pathname.startsWith("/data/")) return this.handleDataRequest(url);
        if (url.pathname === '/stream') return this.handleStreamConnection(request);

        if (url.pathname === "/debug") {
            return Response.json({
                symbol: this.SYMBOL,
                timeframe: this.TIME_FRAME,
                bar_ms: this.BAR_MS,
                ws_state: this.wsState,
                ws_ready: this.ws?.readyState,
                subs: this.subs,
                last_msg_age_ms: this.lastMsgAt > 0 ? Date.now() - this.lastMsgAt : null,
                buffer_len: this.rawBuffer.length,
                candle: this.currentCandle ? "ACTIVE" : "NONE",
                last_price: this.lastPrice,
                best_bid: this.bestBid,
                best_ask: this.bestAsk
            });
        }

        if (url.pathname === "/metrics") {
            return Response.json(this.metrics);
        }

        return Response.json({ status: "OK", service: "fut-state-engine-v4-refactored" });
    }

    // =================================================================
    //  CORE LOOP
    // =================================================================

    async alarm() {
        try {
            if (!this.accessToken) {
                this.ctx.storage.setAlarm(Date.now() + 5000);
                return;
            }

            // Auto-reconnect
            if (this.wsState === 'DISCONNECTED' && Date.now() >= this.nextConnectAt) {
                this.connectSignalR();
            }

            // Liveness watchdog: if socket is "connected" but no messages for too long, reset it
            if (this.wsState !== 'DISCONNECTED' && this.ws) {
                const now = Date.now();
                if (this.lastMsgAt > 0 && (now - this.lastMsgAt) > this.LIVENESS_TIMEOUT_MS) {
                    console.warn(`[SignalR] Stale connection: no messages for ${now - this.lastMsgAt}ms. Resetting...`);
                    this.metrics.staleResets++;

                    try { this.ws.close(); } catch (_) { }
                    this.ws = null;
                    this.wsState = 'DISCONNECTED';
                    this.nextConnectAt = Date.now() + 1000; // quick retry
                    this.reconnectDelayMs = 2000;
                    this.subs = { trade: false, quote: false };
                    this.pendingInv.clear();
                }
            }

            // Raw flush
            if (this.DUAL_WRITE && this.rawBuffer.length > 0) {
                if (Date.now() - this.lastRawFlush > this.FLUSH_RAW_INTERVAL || this.rawBuffer.length > 50) {
                    await this.flushRawBuffer();
                }
            }

            // Candle roll
            const now = Date.now();
            if (this.currentCandle && !this.currentCandle.flushed) {
                const candleEndTime = this.currentCandle.time + this.BAR_MS;
                if (now >= candleEndTime) {
                    await this.closeAndFlushCandle(this.currentCandle);
                    this.currentCandle = null;
                }
            }
        } catch (e) {
            console.error("Alarm Error:", e);
        }

        // [Phase 4] Adaptive alarm: faster when connected
        const interval = this.wsState === 'SUBSCRIBED' ? 1000 : 5000;
        this.ctx.storage.setAlarm(Date.now() + interval);
    }

    // =================================================================
    //  SIGNALR (Phase 3: State Machine)
    // =================================================================

    connectSignalR() {
        if (this.wsState !== 'DISCONNECTED') return;

        console.log("[SignalR] Connecting...");
        this.wsState = 'CONNECTING';
        this.metrics.reconnects++;

        const wsUrl = `wss://chartapi.topstepx.com/hubs/chart?access_token=${this.accessToken}`;
        this.ws = new WebSocket(wsUrl);

        this.ws.addEventListener("open", () => {
            console.log("[SignalR] Open. Sending handshake.");
            this.lastMsgAt = Date.now();
            this.wsState = 'HANDSHAKE_SENT';
            this.ws.send(JSON.stringify({ protocol: "json", version: 1 }) + "\u001e");
        });

        this.ws.addEventListener("message", async (event) => {
            this.lastMsgAt = Date.now();
            const data = String(event.data);

            if (this.DUAL_WRITE) {
                this.rawBuffer.push({ raw: data, ts: Date.now() });
                // Spike guard: flush immediately if buffer too large
                if (this.rawBuffer.length > 500) {
                    this.ctx.waitUntil(this.flushRawBuffer());
                }
            }

            const frames = data.split('\u001e').filter(Boolean);

            for (const f of frames) {
                // Handshake ACK
                if (f === "{}") {
                    if (this.wsState === 'HANDSHAKE_SENT') {
                        console.log("[SignalR] Handshake confirmed");
                        // handshake ok, now send subscribe requests
                        this.wsState = 'SUBSCRIBE_SENT';
                        this.subs = { trade: false, quote: false };
                        this.pendingInv.clear();
                        this.subscribeToMarketData();
                    }
                    continue;
                }

                try {
                    const msg = JSON.parse(f);

                    // Ping (type 6)
                    if (msg.type === 6) continue;

                    // Completion (type 3) -> confirms server accepted invocationId
                    if (msg.type === 3 && msg.invocationId) {
                        const which = this.pendingInv.get(String(msg.invocationId));
                        if (which) {
                            if (msg.error) {
                                console.error(`[SignalR] Subscribe ${which} failed:`, msg.error);
                                // Force reconnect so we retry cleanly
                                try { this.ws?.close(); } catch (_) { }
                                this.ws = null;
                                this.wsState = 'DISCONNECTED';
                                this.nextConnectAt = Date.now() + 1000;
                                this.subs = { trade: false, quote: false };
                                this.pendingInv.clear();
                                return;
                            } else {
                                this.subs[which] = true;
                                this.pendingInv.delete(String(msg.invocationId));
                                console.log(`[SignalR] Subscribe confirmed: ${which}`);

                                // only mark SUBSCRIBED when both are confirmed
                                if (this.subs.trade && this.subs.quote) {
                                    this.wsState = 'SUBSCRIBED';
                                    console.log("[SignalR] Fully subscribed.");
                                }
                            }
                        }
                    }

                    // [Phase 3] Symbol normalization
                    const targetSymbol = `F.US.${this.SYMBOL}`;

                    // Quote
                    if (msg.target === 'RealTimeSymbolQuote') {
                        const quote = msg.arguments?.[0];
                        if (quote && (quote.symbol === targetSymbol || quote.symbol === this.SYMBOL)) {
                            const bid = quote.BestBid || quote.bestBid;
                            const ask = quote.BestAsk || quote.bestAsk;
                            if (bid != null) {
                                this.bestBid = bid;
                                this.bestBidPt = this.bidToPt(bid); // floor for conservative boundary
                            }
                            if (ask != null) {
                                this.bestAsk = ask;
                                this.bestAskPt = this.askToPt(ask); // ceil for conservative boundary
                            }

                            // [V4.2] Quote stats tracking
                            if (this.currentCandle && bid && ask) {
                                const spread = ask - bid;
                                const mid = (bid + ask) / 2;
                                const qs = this.currentCandle.quote_stats;
                                qs.updates++;
                                qs.spread_sum += spread;
                                if (spread < qs.spread_min) qs.spread_min = spread;
                                if (spread > qs.spread_max) qs.spread_max = spread;
                                qs.mid_last = mid;
                                if (qs.mid_first === 0) qs.mid_first = mid;
                            }
                        }
                    }

                    // Trade
                    if (msg.target === 'RealTimeTradeLogWithSpeed') {
                        const trades = msg.arguments?.[1] || msg.arguments?.[0];
                        if (Array.isArray(trades)) {
                            for (const t of trades) this.processTrade(t);
                        } else if (trades?.Price) {
                            this.processTrade(trades);
                        }
                    }
                } catch (e) {
                    this.metrics.parseErrors++;
                }
            }
        });

        this.ws.addEventListener("close", () => {
            console.log("[SignalR] Closed.");
            this.ws = null;
            this.wsState = 'DISCONNECTED';
            this.nextConnectAt = Date.now() + this.reconnectDelayMs;
            this.reconnectDelayMs = Math.min(Math.floor(this.reconnectDelayMs * 1.5), 60000);
        });

        this.ws.addEventListener("error", (e) => {
            console.error("[SignalR] Error:", e);
        });
    }

    subscribeToMarketData() {
        console.log("[SignalR] Subscribing...");
        const args = [`F.US.${this.SYMBOL}`, 0];

        // Trade
        this.invId++;
        const invTrade = String(this.invId);
        this.pendingInv.set(invTrade, 'trade');
        this.ws.send(JSON.stringify({
            type: 1, target: "SubscribeTradeLogWithSpeed", arguments: args, invocationId: invTrade
        }) + "\u001e");

        // Quote
        this.invId++;
        const invQuote = String(this.invId);
        this.pendingInv.set(invQuote, 'quote');
        this.ws.send(JSON.stringify({
            type: 1, target: "SubscribeQuotesForSymbolWithSpeed", arguments: args, invocationId: invQuote
        }) + "\u001e");

        this.reconnectDelayMs = 2000;
    }

    // =================================================================
    //  AGGREGATION (Phase 1: Integer Tick Index)
    // =================================================================

    processTrade(trade) {
        const price = trade.Price || trade.price;
        const vol = trade.Volume || trade.volume;
        const type = trade.Type || trade.type || 0;
        const ts = new Date(trade.Timestamp || trade.timestamp || Date.now()).getTime();

        if (!price || !vol) return;

        this.metrics.tradesProcessed++;

        // Broadcast
        if (price !== this.lastPrice) {
            const dir = price > (this.lastPrice || price) ? 1 : -1;
            this.lastPrice = price;
            this.broadcastPrice(price, dir);
        }

        // [Phase 1] Convert to tick index
        const pt = this.priceToTick(price);

        // Candle bucket
        const bucketTs = Math.floor(ts / this.BAR_MS) * this.BAR_MS;

        // Handle candle sync
        if (this.currentCandle) {
            if (bucketTs > this.currentCandle.time) {
                // New candle - close old
                this.ctx.waitUntil(this.closeAndFlushCandle(this.currentCandle));
                this.initCandle(bucketTs, pt, price);
            } else if (bucketTs < this.currentCandle.time) {
                // [Phase 2] Late trade - drop and log (increment per-candle, not global)
                this.metrics.lateDropped++;
                if (this.currentCandle?.quality) this.currentCandle.quality.late_trades++;
                console.warn(`[Late] Dropping trade from ${new Date(bucketTs).toISOString()}`);
                return;
            }
        }

        if (!this.currentCandle) {
            this.initCandle(bucketTs, pt, price);
        }

        this.updateCandle(this.currentCandle, pt, price, vol, type, ts);
    }

    initCandle(ts, pt, price) {
        const t0 = new Date(ts);
        const t1 = new Date(ts + this.BAR_MS);
        this.currentCandle = {
            v: 3, // Schema version - V4.2 with event-driven stats
            symbol: `F.US.${this.SYMBOL}`,
            tick: this.TICK_SIZE,
            bar_ms: this.BAR_MS,
            timeframe: this.TIME_FRAME,
            t0: t0.toISOString(),
            t1: t1.toISOString(),
            time: ts,
            // OHLC - tick-aligned from pt (not raw vendor price)
            ohlc_pt: { o: pt, h: pt, l: pt, c: pt },
            ohlc: { o: this.tickToPrice(pt), h: this.tickToPrice(pt), l: this.tickToPrice(pt), c: this.tickToPrice(pt) },
            vol: 0,
            delta: 0,
            total_bid: 0,
            total_ask: 0,
            // Profile keyed by integer tick index
            profile: {},
            // Flush flag
            flushed: false,
            // Aggressor mode counts
            aggr_modes: { quote: 0, type: 0, fallback: 0 },

            // [V4.2] Event-driven trade stats
            trade_stats: {
                count: 0,
                buy_count: 0,
                sell_count: 0,
                size_sum: 0,
                size_max: 0,
                size_avg: 0,
                large_trades: 0,
                large_vol: 0
            },

            // [V4.2] Quote stats
            quote_stats: {
                updates: 0,
                spread_min: Infinity,
                spread_max: 0,
                spread_sum: 0,
                spread_avg: 0,
                mid_first: 0,
                mid_last: 0
            },

            // [V4.2] Speed/burst stats
            speed_stats: {
                first_trade_ts: 0,
                last_trade_ts: 0,
                min_gap_ms: Infinity,
                max_gap_ms: 0,
                trades_per_sec_avg: 0
            },

            // [V4.2] Quality audit
            quality: {
                aggr_conf: 0,  // quote_count / total_count
                late_trades: 0
            }
        };

        // Initialize first quote mid price
        if (this.bestBid > 0 && this.bestAsk > 0) {
            this.currentCandle.quote_stats.mid_first = (this.bestBid + this.bestAsk) / 2;
        }
    }

    updateCandle(c, pt, price, vol, type, tradeTs) {
        // Update OHLC (tick-aligned from pt, not raw vendor price)
        if (pt > c.ohlc_pt.h) { c.ohlc_pt.h = pt; c.ohlc.h = this.tickToPrice(pt); }
        if (pt < c.ohlc_pt.l) { c.ohlc_pt.l = pt; c.ohlc.l = this.tickToPrice(pt); }
        c.ohlc_pt.c = pt;
        c.ohlc.c = this.tickToPrice(pt);

        c.vol += vol;

        // [V4.2] Trade stats
        c.trade_stats.count++;
        c.trade_stats.size_sum += vol;
        if (vol > c.trade_stats.size_max) c.trade_stats.size_max = vol;
        if (vol >= LARGE_TRADE_THRESHOLD) {
            c.trade_stats.large_trades++;
            c.trade_stats.large_vol += vol;
        }

        // [V4.2] Speed stats - inter-trade timing
        const now = tradeTs || Date.now();
        if (c.speed_stats.first_trade_ts === 0) {
            c.speed_stats.first_trade_ts = now;
        } else {
            const gap = now - c.speed_stats.last_trade_ts;
            if (gap > 0) {
                if (gap < c.speed_stats.min_gap_ms) c.speed_stats.min_gap_ms = gap;
                if (gap > c.speed_stats.max_gap_ms) c.speed_stats.max_gap_ms = gap;
            }
        }
        c.speed_stats.last_trade_ts = now;

        // Aggressor classification with clear hierarchy
        let isBuy;
        let mode;

        if (this.bestAskPt > 0 && this.bestBidPt > 0) {
            if (pt >= this.bestAskPt) {
                isBuy = true;
                mode = 'quote';
            } else if (pt <= this.bestBidPt) {
                isBuy = false;
                mode = 'quote';
            } else {
                isBuy = (type === 0);
                mode = 'type';
            }
        } else {
            isBuy = (type === 0);
            mode = 'fallback';
        }

        c.aggr_modes[mode]++;

        // [V4.2] Track buy/sell counts
        if (isBuy) {
            c.delta += vol;
            c.total_ask += vol;
            c.trade_stats.buy_count++;
        } else {
            c.delta -= vol;
            c.total_bid += vol;
            c.trade_stats.sell_count++;
        }

        // Profile keyed by integer tick index
        if (!c.profile[pt]) c.profile[pt] = { bid: 0, ask: 0 };
        if (isBuy) c.profile[pt].ask += vol;
        else c.profile[pt].bid += vol;
    }

    async closeAndFlushCandle(c) {
        // [Phase 2] Idempotent flush
        if (c.flushed) return;
        c.flushed = true;

        console.log(`[Footprint] Closing Candle ${c.t0}, Vol:${c.vol}`);

        // Finalize levels
        let maxVol = -1;
        let pocPt = 0;
        const levels = [];

        // [Phase 1] Work with integer tick indices
        const tickKeys = Object.keys(c.profile).map(Number).sort((a, b) => a - b);

        if (tickKeys.length > 0) {
            const loPt = tickKeys[0];
            const hiPt = tickKeys[tickKeys.length - 1];

            // Build continuous ladder (high to low)
            for (let pt = hiPt; pt >= loPt; pt--) {
                const stats = c.profile[pt] || { bid: 0, ask: 0 };
                const total = stats.bid + stats.ask;
                if (total > maxVol) {
                    maxVol = total;
                    pocPt = pt;
                }
                // [V4 Canonical] Include pt (integer tick) for float-free storage
                levels.push({
                    pt: pt,
                    p: this.tickToPrice(pt),
                    bv: stats.bid,
                    av: stats.ask
                });
            }
        }

        c.poc_pt = pocPt;
        c.poc = this.tickToPrice(pocPt);
        c.levels = levels;

        // [V4] Compute derived fields (optional, for analytics)
        let vwapNum = 0, vwapDenom = 0;
        let vaLevels = [...levels].sort((a, b) => (b.bv + b.av) - (a.bv + a.av));
        let vaVol = 0;
        const vaTarget = c.vol * 0.7; // 70% value area
        let vahPt = pocPt, valPt = pocPt;

        for (const lv of levels) {
            const lvVol = lv.bv + lv.av;
            vwapNum += lv.p * lvVol;
            vwapDenom += lvVol;
        }

        for (const lv of vaLevels) {
            if (vaVol >= vaTarget) break;
            vaVol += lv.bv + lv.av;
            if (lv.pt > vahPt) vahPt = lv.pt;
            if (lv.pt < valPt) valPt = lv.pt;
        }

        c.derived = {
            gen: "v4.2",
            va_method: "top_levels_70pct", // Document VA calculation method
            vwap: vwapDenom > 0 ? vwapNum / vwapDenom : 0,
            vah_pt: vahPt,
            val_pt: valPt,
            vah: this.tickToPrice(vahPt),
            val: this.tickToPrice(valPt),
            va_vol: vaVol
        };

        // [V4.2] Finalize event-driven stats - compute averages
        if (c.trade_stats.count > 0) {
            c.trade_stats.size_avg = c.trade_stats.size_sum / c.trade_stats.count;
        }

        if (c.quote_stats.updates > 0) {
            c.quote_stats.spread_avg = c.quote_stats.spread_sum / c.quote_stats.updates;
        }
        // Clean up Infinity values for JSON serialization
        if (c.quote_stats.spread_min === Infinity) c.quote_stats.spread_min = 0;

        if (c.speed_stats.first_trade_ts > 0 && c.speed_stats.last_trade_ts > c.speed_stats.first_trade_ts) {
            const durationSec = (c.speed_stats.last_trade_ts - c.speed_stats.first_trade_ts) / 1000;
            c.speed_stats.trades_per_sec_avg = durationSec > 0 ? c.trade_stats.count / durationSec : 0;
        }
        // Clean up Infinity values
        if (c.speed_stats.min_gap_ms === Infinity) c.speed_stats.min_gap_ms = 0;

        // Compute aggressor confidence (quote-based / total)
        const totalClassified = c.aggr_modes.quote + c.aggr_modes.type + c.aggr_modes.fallback;
        c.quality.aggr_conf = totalClassified > 0 ? c.aggr_modes.quote / totalClassified : 0;
        // Note: late_trades is now tracked per-candle in processTrade, no need to set from global

        delete c.profile;
        delete c.ohlc_pt; // Remove internal tick data

        // [Phase 1] Invariant validation
        const sumBid = levels.reduce((s, l) => s + l.bv, 0);
        const sumAsk = levels.reduce((s, l) => s + l.av, 0);
        if (c.vol !== sumBid + sumAsk) {
            console.warn(`[Invariant] vol mismatch: ${c.vol} vs ${sumBid + sumAsk}`);
        }
        if (c.delta !== sumAsk - sumBid) {
            console.warn(`[Invariant] delta mismatch: ${c.delta} vs ${sumAsk - sumBid}`);
        }

        // R2 write (per-candle immutable)
        const date = new Date(c.time);
        const y = date.getUTCFullYear();
        const m = String(date.getUTCMonth() + 1).padStart(2, '0');
        const d = String(date.getUTCDate()).padStart(2, '0');
        const h = String(date.getUTCHours()).padStart(2, '0');

        const key = `footprint/${this.SYMBOL}/${this.TIME_FRAME}/${y}/${m}/${d}/${h}/${c.time}.json`;

        try {
            await this.env.DATA_LAKE.put(key, JSON.stringify(c), {
                httpMetadata: { contentType: "application/json" }
            });
            this.metrics.candlesFlushed++;
            console.log(`[Footprint] Saved to ${key}`);
        } catch (e) {
            console.error("Footprint Flush Error:", e);
        }
    }

    // =================================================================
    //  RAW DUMP
    // =================================================================

    async flushRawBuffer() {
        if (this.rawBuffer.length === 0) return;

        const dataToWrite = [...this.rawBuffer];
        this.rawBuffer = [];
        this.lastRawFlush = Date.now();

        const now = new Date();
        const y = now.getUTCFullYear();
        const m = String(now.getUTCMonth() + 1).padStart(2, '0');
        const d = String(now.getUTCDate()).padStart(2, '0');
        const h = String(now.getUTCHours()).padStart(2, '0');

        const key = `raw_tns/${this.SYMBOL}/${y}/${m}/${d}/${h}/${Date.now()}.json`;
        const fileContent = dataToWrite.map(JSON.stringify).join('\n');

        try {
            await this.env.DATA_LAKE.put(key, fileContent);
            this.metrics.rawFlushed += dataToWrite.length;
            console.log(`[Raw] Flushed ${dataToWrite.length} msgs`);
        } catch (e) {
            console.error("[Raw] Flush Error:", e);
        }
    }

    // =================================================================
    //  HANDLERS
    // =================================================================

    broadcastPrice(price, dir) {
        const msg = JSON.stringify({ type: 'price', price, direction: dir });
        // Limit sessions to prevent memory issues
        if (this.sessions.length > 100) {
            this.sessions = this.sessions.slice(-50);
        }
        this.sessions.forEach(s => { try { s.send(msg) } catch (e) { } });
    }

    handleStreamConnection(r) {
        const up = r.headers.get('Upgrade');
        if (!up || up !== 'websocket') return new Response("Websocket?", { status: 426 });
        const { 0: cl, 1: sv } = new WebSocketPair();
        sv.accept();
        this.sessions.push(sv);
        sv.addEventListener('close', () => this.sessions = this.sessions.filter(s => s !== sv));
        sv.addEventListener('error', () => this.sessions = this.sessions.filter(s => s !== sv));
        sv.send(JSON.stringify({ type: 'init', price: this.lastPrice }));
        return new Response(null, { status: 101, webSocket: cl });
    }

    handleTokenStatus() {
        return Response.json({
            valid: !!this.accessToken,
            ws_state: this.wsState
        });
    }

    async handleUpdateToken(u) {
        const t = u.searchParams.get("token");
        if (t) {
            this.accessToken = t;
            await this.ctx.storage.put("access_token", t);
            if (this.ws) { this.ws.close(); this.ws = null; }
            this.wsState = 'DISCONNECTED';
            this.nextConnectAt = 0;
            this.reconnectDelayMs = 2000;
            this.invId = 0;
            this.connectSignalR();
            return Response.json({ ok: true });
        }
        return Response.json({ error: "No token" }, { status: 400 });
    }

    async handleDataRequest(u) {
        const p = u.pathname.split('/').filter(Boolean);
        if (p.length < 5) return new Response("400", { status: 400 });
        const [_, y, m, d, h] = p;

        // Legacy single file
        const legacyKey = `footprint/${this.SYMBOL}/${this.TIME_FRAME}/${y}/${m}/${d}/${h}.jsonl`;
        const legacyObj = await this.env.DATA_LAKE.get(legacyKey);
        if (legacyObj) {
            return new Response(legacyObj.body, {
                headers: { "Access-Control-Allow-Origin": "*", "Content-Type": "application/x-ndjson" }
            });
        }

        // Per-candle objects
        const prefix = `footprint/${this.SYMBOL}/${this.TIME_FRAME}/${y}/${m}/${d}/${h}/`;
        let allObjects = [];
        let cursor = undefined;

        // [Phase 4] Handle R2.list pagination
        do {
            const list = await this.env.DATA_LAKE.list({ prefix, cursor });
            if (list.objects) allObjects = allObjects.concat(list.objects);
            cursor = list.truncated ? list.cursor : undefined;
        } while (cursor);

        if (allObjects.length === 0) {
            return new Response("404", { status: 404 });
        }

        // Sort by timestamp
        const sorted = allObjects.sort((a, b) => {
            const tsA = parseInt(a.key.split('/').pop().replace('.json', ''));
            const tsB = parseInt(b.key.split('/').pop().replace('.json', ''));
            return tsA - tsB;
        });

        // Fetch and concatenate
        const lines = [];
        for (const obj of sorted) {
            const data = await this.env.DATA_LAKE.get(obj.key);
            if (data) lines.push(await data.text());
        }

        return new Response(lines.join('\n') + '\n', {
            headers: { "Access-Control-Allow-Origin": "*", "Content-Type": "application/x-ndjson" }
        });
    }
}
