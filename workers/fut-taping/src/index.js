import puppeteer from "@cloudflare/puppeteer";

export default {
    async fetch(request, env) {
        const url = new URL(request.url);

        // Simple proxy for other requests (existing functionality)
        if (!url.pathname.startsWith("/rpa")) {
            return env.STATE_ENGINE.fetch(request);
        }

        // RPA Endpoint: /rpa/refresh-token
        if (url.pathname === "/rpa/refresh-token") {
            return await this.doLoginAndRefresh(env);
        }

        return new Response("RPA Worker Ready", { status: 200 });
    },

    async doLoginAndRefresh(env) {
        let browser;
        try {
            console.log("[RPA] Launching browser...");
            browser = await puppeteer.launch(env.MYBROWSER);
            const page = await browser.newPage();

            // 1. Go to Login
            console.log("[RPA] Navigating to login page...");
            await page.goto("https://topstepx.com/login", { waitUntil: 'networkidle2', timeout: 30000 });

            // 2. Input Credentials
            console.log("[RPA] Inputting credentials...");

            // Wait with debug dump
            try {
                // Try wait for ANY input first to see if page loaded
                await page.waitForSelector('input', { timeout: 15000 });
            } catch (timeoutErr) {
                const html = await page.content();
                const title = await page.title();
                const url = page.url();
                // Dump first 1000 chars of HTML to see what's wrong (Auth0? Captcha? Cloudflare challenge?)
                throw new Error(`Login page load failed (no inputs). Title: ${title}, URL: ${url}. HTML: ${html.substring(0, 1000)}`);
            }

            // Type Username (Try generic inputs by order if specific names fail)
            const inputs = await page.$$('input');
            console.log(`[RPA] Found ${inputs.length} inputs`);

            if (inputs.length >= 2) {
                // Assume first text/email input is username, first password input is password
                // Or just blindly type into first two visible inputs
                await inputs[0].type(env.TOPSTEP_USER);

                // Find password input specifically if possible
                const passInput = await page.$('input[type="password"]');
                if (passInput) {
                    await passInput.type(env.TOPSTEP_PASS);
                } else {
                    // Fallback to second input
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
                // Try Enter key if button not found
                await Promise.all([
                    page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 30000 }),
                    page.keyboard.press('Enter')
                ]);
            }

            console.log("[RPA] Login submitted, checking for token...");

            // 4. Extract Token (Try multiple sources)
            // Wait a bit for JS to populate storage
            await new Promise(r => setTimeout(r, 3000));

            const token = await page.evaluate(() => {
                // TopStepX likely uses local storage or session storage key 'access_token', 'token', or similar
                // Or inside a persistence object
                return localStorage.getItem('access_token') ||
                    localStorage.getItem('token') ||
                    localStorage.getItem('jwt') ||
                    sessionStorage.getItem('access_token');
            });

            if (!token) {
                // Debug: Take screenshot logic here if needed (omitted for now)
                // Dump body text to logs for debugging failed login
                const bodyText = await page.evaluate(() => document.body.innerText.substring(0, 200));
                throw new Error(`Token not found after login. Page text: ${bodyText}`);
            }

            console.log(`[RPA] Token found! Length: ${token.length}`);

            // 5. Push to Engine
            const updateUrl = `http://internal/update-token?token=${token}`;
            const updateRes = await env.STATE_ENGINE.fetch(updateUrl);
            const updateText = await updateRes.text();

            return Response.json({
                success: true,
                message: "RPA Login Successful & Token Updated",
                engineResponse: updateText
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
    }
};