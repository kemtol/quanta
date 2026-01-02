import puppeteer from "@cloudflare/puppeteer";

export default {
    async fetch(request, env) {
        const url = new URL(request.url);

        // RPA Endpoints
        if (url.pathname === "/rpa/refresh-token") {
            // Get target from query param (default: all)
            const target = url.searchParams.get("target") || "all";
            return await this.doLoginAndRefresh(env, target);
        }

        if (url.pathname === "/rpa/status") {
            return Response.json({
                ready: true,
                targets: ["enq", "gc", "all"]
            });
        }

        return new Response("RPA Worker Ready. Use /rpa/refresh-token?target=enq|gc|all", { status: 200 });
    },

    async doLoginAndRefresh(env, target) {
        let browser;
        try {
            console.log(`[RPA] Starting token refresh for target: ${target}`);
            console.log("[RPA] Launching browser...");
            browser = await puppeteer.launch(env.MYBROWSER);
            const page = await browser.newPage();

            // 1. Go to Login
            console.log("[RPA] Navigating to login page...");
            await page.goto("https://topstepx.com/login", { waitUntil: 'networkidle2', timeout: 30000 });

            // 2. Input Credentials
            console.log("[RPA] Inputting credentials...");

            try {
                await page.waitForSelector('input', { timeout: 15000 });
            } catch (timeoutErr) {
                const html = await page.content();
                const title = await page.title();
                const pageUrl = page.url();
                throw new Error(`Login page load failed. Title: ${title}, URL: ${pageUrl}. HTML: ${html.substring(0, 1000)}`);
            }

            const inputs = await page.$$('input');
            console.log(`[RPA] Found ${inputs.length} inputs`);

            if (inputs.length >= 2) {
                await inputs[0].type(env.TOPSTEP_USER);
                const passInput = await page.$('input[type="password"]');
                if (passInput) {
                    await passInput.type(env.TOPSTEP_PASS);
                } else {
                    await inputs[1].type(env.TOPSTEP_PASS);
                }
            } else {
                throw new Error(`Found ${inputs.length} inputs, expected at least 2`);
            }

            // 3. Submit
            console.log("[RPA] Clicking login...");
            const submitBtn = await page.$('button[type="submit"]');
            if (submitBtn) {
                await Promise.all([
                    page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 30000 }),
                    submitBtn.click()
                ]);
            } else {
                await Promise.all([
                    page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 30000 }),
                    page.keyboard.press('Enter')
                ]);
            }

            console.log("[RPA] Login submitted, checking for token...");
            await new Promise(r => setTimeout(r, 3000));

            // 4. Extract Token
            const token = await page.evaluate(() => {
                return localStorage.getItem('access_token') ||
                    localStorage.getItem('token') ||
                    localStorage.getItem('jwt') ||
                    sessionStorage.getItem('access_token');
            });

            if (!token) {
                const bodyText = await page.evaluate(() => document.body.innerText.substring(0, 200));
                throw new Error(`Token not found after login. Page text: ${bodyText}`);
            }

            console.log(`[RPA] Token found! Length: ${token.length}`);

            // 6. Push to Target Worker(s)
            const results = {};

            if (target === "enq" || target === "all") {
                try {
                    const res = await env.STATE_ENQ.fetch(`http://internal/update-token?token=${token}`);
                    results.enq = { success: res.ok, status: res.status };
                    console.log(`[RPA] ENQ update: ${res.ok ? 'OK' : 'FAILED'}`);
                } catch (e) {
                    results.enq = { success: false, error: e.message };
                }
            }

            if (target === "gc" || target === "all") {
                try {
                    const res = await env.STATE_GC.fetch(`http://internal/update-token?token=${token}`);
                    results.gc = { success: res.ok, status: res.status };
                    console.log(`[RPA] GC update: ${res.ok ? 'OK' : 'FAILED'}`);
                } catch (e) {
                    results.gc = { success: false, error: e.message };
                }
            }

            return Response.json({
                success: true,
                message: "RPA Login Successful & Token Updated",
                target,
                results
            });

        } catch (e) {
            console.error("[RPA] Error:", e);
            return Response.json({
                success: false,
                error: e.message,
                stack: e.stack
            }, { status: 500 });
        } finally {
            if (browser) {
                await browser.close();
            }
        }
    },

    async scheduled(event, env, ctx) {
        console.log("[Cron] Scheduled event triggered");

        // 1. Random Jitter (1 to 5 minutes)
        // Range: 60,000ms to 300,000ms
        const jitterMs = Math.floor(Math.random() * (300000 - 60000 + 1) + 60000);
        console.log(`[Cron] Applying jitter: ${Math.round(jitterMs / 1000)}s`);

        // Note: Using setTimeout in a promise to block execution without blocking the thread
        await new Promise(resolve => setTimeout(resolve, jitterMs));

        // 2. Check Status (Smart Rotation)
        console.log("[Cron] Checking token status...");
        try {
            // Using STATE_ENQ as the primary canary
            const statusRes = await env.STATE_ENQ.fetch("http://internal/token-status");
            if (statusRes.ok) {
                const status = await statusRes.json();
                console.log(`[Cron] Status: valid=${status.valid}, ws_state=${status.ws_state}`);

                // If token is valid AND websocket is subscribed, skip login
                if (status.valid && status.ws_state === 'SUBSCRIBED') {
                    console.log("[Cron] Token is healthy and connected. Skipping rotation.");
                    return;
                }
            }
        } catch (e) {
            console.warn("[Cron] Status check failed, forcing rotation:", e);
        }

        // 3. Execute Rotation if needed
        console.log("[Cron] Token unhealthy or expired. Executing rotation.");
        await this.doLoginAndRefresh(env, "all");
    }
};