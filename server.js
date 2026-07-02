const express = require("express");
const { chromium } = require("playwright");

const app = express();
const PORT = process.env.PORT || 3000;
const BROWSER_TOKEN = process.env.BROWSER_TOKEN || "dev-token";

const LIVE_STOP_CACHE_TTL_MS = Number(process.env.LIVE_STOP_CACHE_TTL_MS || 8000);
const LIVE_STOP_CACHE_MAX = Number(process.env.LIVE_STOP_CACHE_MAX || 450);
const PAGE_POOL_SIZE = Math.max(1, Math.min(Number(process.env.PAGE_POOL_SIZE || 2), 5));
const NAV_TIMEOUT_MS = Math.max(1200, Math.min(Number(process.env.NAV_TIMEOUT_MS || 6500), 20000));
const ROW_TIMEOUT_MS = Math.max(500, Math.min(Number(process.env.ROW_TIMEOUT_MS || 3200), 12000));

let browser;
let browserContext;
let activePageCount = 0;
const idlePages = [];
const pageWaiters = [];

const liveStopCache = new Map();
const liveStopInFlight = new Map();

function nowMs() {
  return Date.now();
}

function authOk(req) {
  return (req.headers.authorization || "") === `Bearer ${BROWSER_TOKEN}`;
}

function json(res, status, body, extraHeaders = {}) {
  for (const [key, value] of Object.entries(extraHeaders)) res.setHeader(key, value);
  res.status(status).json(body);
}

function browserOptions() {
  return {
    viewport: { width: 390, height: 844 },
    userAgent:
      "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148 Safari/604.1",
    locale: "en-AU",
    timezoneId: "Australia/Perth"
  };
}

function stopUrl(stopId) {
  return `https://136213.mobi/RealTime/RealTimeStopResults.aspx?SN=${encodeURIComponent(stopId)}`;
}

function normaliseStopId(raw) {
  return String(raw || "").trim().replace(/[^\d]/g, "");
}

function normaliseLimit(raw) {
  return Math.max(1, Math.min(Number(raw || 5), 20));
}

function cacheKey(stopId, limit) {
  return `${stopId}|${limit}`;
}

function rememberCache(key, payload) {
  liveStopCache.set(key, {
    expiresAt: nowMs() + LIVE_STOP_CACHE_TTL_MS,
    payload
  });

  while (liveStopCache.size > LIVE_STOP_CACHE_MAX) {
    const firstKey = liveStopCache.keys().next().value;
    if (!firstKey) break;
    liveStopCache.delete(firstKey);
  }
}

async function ensureBrowserReady() {
  if (!browser) {
    browser = await chromium.launch({
      headless: true,
      args: [
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--no-sandbox"
      ]
    });
  }

  if (!browserContext) {
    browserContext = await browser.newContext(browserOptions());

    // The stop rows are in the document. Images, fonts, media and CSS are not
    // needed for parsing and can delay networkidle / page activity substantially.
    await browserContext.route("**/*", async route => {
      const request = route.request();
      const type = request.resourceType();
      if (type === "image" || type === "media" || type === "font" || type === "stylesheet") {
        return route.abort().catch(() => {});
      }
      return route.continue().catch(() => {});
    });
  }
}

async function acquirePage() {
  const started = nowMs();
  await ensureBrowserReady();

  if (idlePages.length) {
    const page = idlePages.pop();
    return { page, waitMs: nowMs() - started };
  }

  if (activePageCount < PAGE_POOL_SIZE) {
    activePageCount += 1;
    try {
      const page = await browserContext.newPage();
      page.setDefaultNavigationTimeout(NAV_TIMEOUT_MS);
      page.setDefaultTimeout(ROW_TIMEOUT_MS);
      return { page, waitMs: nowMs() - started };
    } catch (error) {
      activePageCount = Math.max(0, activePageCount - 1);
      throw error;
    }
  }

  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      const index = pageWaiters.findIndex(waiter => waiter.resolve === resolve);
      if (index >= 0) pageWaiters.splice(index, 1);
      reject(new Error("Timed out waiting for browser page"));
    }, Math.max(2000, NAV_TIMEOUT_MS + ROW_TIMEOUT_MS));

    pageWaiters.push({
      resolve: page => {
        clearTimeout(timeout);
        resolve({ page, waitMs: nowMs() - started });
      },
      reject
    });
  });
}

async function releasePage(page, discard = false) {
  if (!page) return;

  if (discard || page.isClosed()) {
    try { if (!page.isClosed()) await page.close(); } catch {}
    activePageCount = Math.max(0, activePageCount - 1);
  } else if (pageWaiters.length) {
    const waiter = pageWaiters.shift();
    waiter.resolve(page);
    return;
  } else {
    idlePages.push(page);
  }

  while (pageWaiters.length && activePageCount < PAGE_POOL_SIZE) {
    const waiter = pageWaiters.shift();
    activePageCount += 1;
    try {
      const newPage = await browserContext.newPage();
      newPage.setDefaultNavigationTimeout(NAV_TIMEOUT_MS);
      newPage.setDefaultTimeout(ROW_TIMEOUT_MS);
      waiter.resolve(newPage);
    } catch (error) {
      activePageCount = Math.max(0, activePageCount - 1);
      waiter.reject(error);
    }
  }
}

function servicesFromPageScript(limit) {
  const rows = Array.from(document.querySelectorAll(".tpm_row_timetable"));

  return rows
    .map(row => {
      const text = (row.textContent || "")
        .replace(/\s+/g, " ")
        .trim();

      if (!text) return null;

      const route = text.match(/\b([A-Z]?\d{1,4}[A-Z]?|[A-Z]+ CAT)\b/i)?.[1] || null;

      const destination =
        row.querySelector(".route-display-name strong")?.textContent?.trim() ||
        text.match(/To\s+.+?(?=\s+Depart from stop|\s+\d+\s*MIN|\s+\(sched\)|\s+NOW|\s+ARRIVING)/i)?.[0]?.trim() ||
        null;

      const stopText =
        Array.from(row.querySelectorAll(".route-display-name"))
          .map(el => (el.textContent || "").replace(/\s+/g, " ").trim())
          .find(t => t.toLowerCase().includes("depart from stop")) ||
        "Depart from stop";

      const tripId = row.getAttribute("data-tripid") || null;
      const fleet = row.getAttribute("data-fleet") || null;

      const isLive =
        row.classList.contains("fleet-running") ||
        Boolean(fleet) ||
        /\bLIVE\b/i.test(text);

      const due =
        text.match(/\b\d+\s*MIN\b/i)?.[0]?.replace(/\s+/g, " ").toUpperCase() ||
        (/\barriving\b/i.test(text) ? "Arriving" : /\bNOW\b/i.test(text) ? "NOW" : null);

      const scheduled = /\(sched\.\)/i.test(text);

      const time =
        text.match(/\b\d{1,2}:\d{2}\s*(am|pm)\b/i)?.[0]?.replace(/\s+/g, "") ||
        null;

      return {
        route,
        destination,
        stopText,
        due,
        time,
        statusText: isLive ? "Live" : "Scheduled",
        scheduled,
        live: isLive,
        fleetNumber: fleet || null,
        fleet: fleet || null,
        tripId,
        detailURL: fleet
          ? `https://136213.mobi/RealTime/RealTimeFleetTrip.aspx?nq=true&fleet=${encodeURIComponent(fleet)}`
          : null,
        rawText: text
      };
    })
    .filter(service => service && service.route && service.destination)
    .slice(0, limit);
}

async function fetchLiveStopFresh(stopId, limit, debug = false) {
  const overallStartedAt = nowMs();
  const timings = {};
  const { page, waitMs } = await acquirePage();
  timings.acquirePageMs = waitMs;
  let discardPage = false;

  try {
    const navStartedAt = nowMs();
    await page.goto(stopUrl(stopId), {
      waitUntil: "domcontentloaded",
      timeout: NAV_TIMEOUT_MS
    });
    timings.gotoDomContentLoadedMs = nowMs() - navStartedAt;

    const rowsStartedAt = nowMs();
    const rowHandle = await page.waitForSelector(".tpm_row_timetable", {
      state: "attached",
      timeout: ROW_TIMEOUT_MS
    }).catch(() => null);
    timings.waitFirstRowMs = nowMs() - rowsStartedAt;
    timings.firstRowSeen = Boolean(rowHandle);
    try { await rowHandle?.dispose?.(); } catch {}

    const evalStartedAt = nowMs();
    const services = await page.evaluate(servicesFromPageScript, limit);
    timings.evaluateMs = nowMs() - evalStartedAt;

    const payload = {
      ok: true,
      stopId,
      stopName: `Stop ${stopId}`,
      source: "136213-browser-fast-v2",
      count: services.length,
      services,
      fetchedAt: new Date().toISOString(),
      ...(debug ? {
        debug: {
          url: stopUrl(stopId),
          pagePoolSize: PAGE_POOL_SIZE,
          navTimeoutMs: NAV_TIMEOUT_MS,
          rowTimeoutMs: ROW_TIMEOUT_MS,
          timings: {
            ...timings,
            totalElapsedMs: nowMs() - overallStartedAt
          }
        }
      } : {})
    };

    return payload;
  } catch (error) {
    discardPage = true;
    return {
      ok: false,
      stopId,
      source: "136213-browser-fast-v2",
      error: String(error.message || error),
      ...(debug ? {
        debug: {
          url: stopUrl(stopId),
          timings: {
            ...timings,
            totalElapsedMs: nowMs() - overallStartedAt
          }
        }
      } : {})
    };
  } finally {
    await releasePage(page, discardPage);
  }
}

async function fetchLiveStop(stopId, limit, options = {}) {
  const key = cacheKey(stopId, limit);
  const bypassCache = Boolean(options.cold);
  const debug = Boolean(options.debug);

  if (!bypassCache) {
    const cached = liveStopCache.get(key);
    if (cached && cached.expiresAt > nowMs()) {
      const payload = {
        ...cached.payload,
        source: `${cached.payload.source || "136213-browser-fast-v2"}-memory-cache`,
        cache: "hit",
        fetchedAt: cached.payload.fetchedAt
      };
      if (debug) {
        payload.debug = {
          ...(payload.debug || {}),
          cache: "hit",
          cacheTtlMs: Math.max(0, cached.expiresAt - nowMs())
        };
      }
      return payload;
    }

    if (liveStopInFlight.has(key)) {
      const payload = await liveStopInFlight.get(key);
      return {
        ...payload,
        cache: "joined-inflight",
        source: payload.source || "136213-browser-fast-v2"
      };
    }
  }

  const promise = fetchLiveStopFresh(stopId, limit, debug).then(payload => {
    if (payload?.ok) rememberCache(key, payload);
    return payload;
  }).finally(() => {
    liveStopInFlight.delete(key);
  });

  liveStopInFlight.set(key, promise);
  const payload = await promise;
  return {
    ...payload,
    cache: bypassCache ? "bypass" : "miss"
  };
}

app.get("/", (req, res) => {
  res.json({
    ok: true,
    service: "transperth-browser",
    source: "136213-browser-fast-v2",
    endpoints: [
      "/live-stop/26768?limit=5",
      "/debug/live-stop-timing/26768?limit=5&runs=3&cold=1"
    ],
    cacheTtlMs: LIVE_STOP_CACHE_TTL_MS,
    pagePoolSize: PAGE_POOL_SIZE
  });
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    service: "transperth-browser",
    browserReady: Boolean(browser),
    contextReady: Boolean(browserContext),
    activePageCount,
    idlePageCount: idlePages.length,
    cacheSize: liveStopCache.size,
    inFlight: liveStopInFlight.size
  });
});

app.get("/live-stop/:stopId", async (req, res) => {
  if (!authOk(req)) {
    return res.status(401).json({ ok: false, error: "Unauthorized" });
  }

  const stopId = normaliseStopId(req.params.stopId);
  if (!stopId) return res.status(400).json({ ok: false, error: "Invalid stopId" });

  const limit = normaliseLimit(req.query.limit);
  const debug = req.query.debug === "1" || req.query.debug === "true";
  const cold = req.query.cold === "1" || req.query.cold === "true";

  const payload = await fetchLiveStop(stopId, limit, { debug, cold });

  const cacheControl = cold
    ? "no-store"
    : `public, max-age=${Math.max(1, Math.floor(LIVE_STOP_CACHE_TTL_MS / 1000))}, stale-while-revalidate=30`;

  return json(res, payload.ok ? 200 : 504, payload, {
    "Cache-Control": cacheControl
  });
});

app.get("/debug/live-stop-timing/:stopId", async (req, res) => {
  if (!authOk(req)) {
    return res.status(401).json({ ok: false, error: "Unauthorized" });
  }

  const stopId = normaliseStopId(req.params.stopId);
  if (!stopId) return res.status(400).json({ ok: false, error: "Invalid stopId" });

  const limit = normaliseLimit(req.query.limit);
  const runs = Math.max(1, Math.min(Number(req.query.runs || 3), 8));
  const cold = req.query.cold !== "0" && req.query.cold !== "false";
  const includeRows = req.query.rows === "1" || req.query.rows === "true";

  const results = [];
  for (let i = 0; i < runs; i += 1) {
    const startedAt = nowMs();
    const payload = await fetchLiveStop(stopId, limit, { debug: true, cold });
    const elapsedMs = nowMs() - startedAt;
    results.push({
      run: i + 1,
      ok: Boolean(payload.ok),
      status: payload.ok ? 200 : 504,
      elapsedMs,
      count: payload.count || 0,
      cache: payload.cache || null,
      timings: payload.debug?.timings || payload.debug || null,
      ...(includeRows ? { services: payload.services || [] } : {
        preview: (payload.services || []).slice(0, 5).map(s => ({
          fleetNumber: s.fleetNumber || null,
          route: s.route || null,
          time: s.time || null,
          due: s.due || null,
          destination: s.destination || null
        }))
      }),
      error: payload.error || null
    });
  }

  const elapsedValues = results.filter(r => r.ok).map(r => r.elapsedMs);
  const avgMs = elapsedValues.length
    ? Math.round(elapsedValues.reduce((a, b) => a + b, 0) / elapsedValues.length)
    : null;

  return res.json({
    ok: results.some(r => r.ok),
    source: "browser-live-stop-fast-v2-timing",
    stopId,
    limit,
    runs,
    cold,
    summary: {
      avgMs,
      minMs: elapsedValues.length ? Math.min(...elapsedValues) : null,
      maxMs: elapsedValues.length ? Math.max(...elapsedValues) : null
    },
    results,
    fetchedAt: new Date().toISOString()
  });
});

async function shutdown() {
  try {
    for (const page of idlePages.splice(0)) {
      try { await page.close(); } catch {}
    }
    if (browserContext) await browserContext.close();
    if (browser) await browser.close();
  } finally {
    process.exit(0);
  }
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

app.listen(PORT, async () => {
  await ensureBrowserReady();
  console.log(`Transperth browser service running on http://localhost:${PORT}`);
  console.log(`Token: ${BROWSER_TOKEN}`);
  console.log(`Fast live-stop v2: domcontentloaded + first-row wait, resource blocking, memory cache, singleflight`);
});
