// --- Advanced Analysis Logic ---
function processData() {
    const IMB_RATIO_TH = 3.0;
    const MIN_ABS_VOL = 6;
    const STACK_LEN = 3;
    const DIV_LOOKBACK = 3;
    const DELTA_RATIO_TH = 0.25;
    const MIN_CANDLE_VOL = 100; // Adjustable

    data.forEach((c, idx) => {
        // 1. Cluster Levels (Integer Aggregation)
        const levelMap = new Map();
        c.levels.forEach(l => {
            const price = Math.round(l.p);
            if (!levelMap.has(price)) levelMap.set(price, { p: price, bv: 0, av: 0 });
            const agg = levelMap.get(price);
            agg.bv += l.bv;
            agg.av += l.av;
        });

        // Convert to Sorted Array (High to Low)
        c.rows = Array.from(levelMap.values()).sort((a, b) => b.p - a.p);

        // 2. Calculate Diagonal Imbalance
        for (let i = 0; i < c.rows.length; i++) {
            const row = c.rows[i];
            row.imbalance = null;

            // Buy Imbalance
            if (i < c.rows.length - 1) {
                const currBuy = row.av;
                const belowSell = c.rows[i + 1].bv;
                const ratio = currBuy / Math.max(1, belowSell);
                if (ratio >= IMB_RATIO_TH && currBuy >= MIN_ABS_VOL && belowSell >= 1) {
                    row.imbalance = 'BUY';
                    row.imbalanceRatio = ratio;
                }
            }

            // Sell Imbalance
            if (i > 0) {
                const currSell = row.bv;
                const aboveBuy = c.rows[i - 1].av;
                const ratio = currSell / Math.max(1, aboveBuy);
                if (ratio >= IMB_RATIO_TH && currSell >= MIN_ABS_VOL && aboveBuy >= 1) {
                    if (!row.imbalance) {
                        row.imbalance = 'SELL';
                        row.imbalanceRatio = ratio;
                    }
                }
            }
        }

        // 3. Stacked Imbalance
        c.stacked = { buy: [], sell: [] };
        let currentStack = { type: null, count: 0, startIdx: -1 };

        const finishStack = () => {
            if (currentStack.count >= STACK_LEN && currentStack.type) {
                const highP = c.rows[currentStack.startIdx].p;
                const lowP = c.rows[currentStack.startIdx + currentStack.count - 1].p;
                c.stacked[currentStack.type.toLowerCase()].push({ high: highP, low: lowP, count: currentStack.count });
            }
            currentStack = { type: null, count: 0, startIdx: -1 };
        };

        for (let i = 0; i < c.rows.length; i++) {
            const row = c.rows[i];
            if (row.imbalance) {
                if (row.imbalance === currentStack.type) {
                    currentStack.count++;
                } else {
                    finishStack();
                    currentStack = { type: row.imbalance, count: 1, startIdx: i };
                }
            } else {
                finishStack();
            }
        }
        finishStack();

        // 4. Candle Metrics & Delta
        let totalVol = 0;
        let delta = 0;
        c.rows.forEach(r => {
            totalVol += r.bv + r.av;
            delta += r.av - r.bv;
        });
        c.totalVol = totalVol;
        c.delta = delta;

        // Delta Signal
        c.deltaSignal = null;
        const deltaRatio = Math.abs(delta) / Math.max(1, totalVol);
        if (totalVol >= MIN_CANDLE_VOL && deltaRatio >= DELTA_RATIO_TH) {
            if (delta > 0) c.deltaSignal = 'BUY';
            else if (delta < 0) c.deltaSignal = 'SELL';
        }

        // Delta Divergence
        c.deltaDiv = null;
        if (idx >= DIV_LOOKBACK) {
            const prevCandles = data.slice(idx - DIV_LOOKBACK, idx);
            const lookbackHigh = Math.max(...prevCandles.map(x => x.ohlc.h));
            const lookbackLow = Math.min(...prevCandles.map(x => x.ohlc.l));

            if (c.ohlc.h > lookbackHigh && c.delta <= 0) c.deltaDiv = 'BEAR';
            else if (c.ohlc.l < lookbackLow && c.delta >= 0) c.deltaDiv = 'BULL';
        }

        // 5. Composite Score
        let score = 0;
        let tags = [];

        const maxBuyStack = c.stacked.buy.reduce((m, s) => Math.max(m, s.count), 0);
        const maxSellStack = c.stacked.sell.reduce((m, s) => Math.max(m, s.count), 0);

        if (maxBuyStack >= STACK_LEN) { score += 2; tags.push('STACK_BUY'); }
        if (maxSellStack >= STACK_LEN) { score -= 2; tags.push('STACK_SELL'); }

        if (c.deltaDiv === 'BEAR') { score -= 2; tags.push('DIV_BEAR'); }
        if (c.deltaDiv === 'BULL') { score += 2; tags.push('DIV_BULL'); }

        // Alignment Bonus (Delta Signal aligns with Stack)
        if (c.deltaSignal === 'BUY' && maxBuyStack >= STACK_LEN) score += 1;
        if (c.deltaSignal === 'SELL' && maxSellStack >= STACK_LEN) score -= 1;

        c.signal = { score, tags };
    });
}
