"use strict";

const crypto = require("crypto");
const express = require("express");
const { chromium } = require("playwright");

const app = express();
app.disable("x-powered-by");

const PORT = positiveInt(process.env.PORT, 3000, 1, 65535);
const BROWSER_TOKEN = String(process.env.BROWSER_TOKEN || "").trim();
const POOL_SIZE = positiveInt(process.env.BROWSER_POOL_SIZE, 2, 1, 4);
const NAVIGATION_TIMEOUT_MS = positiveInt(process.env.NAVIGATION_TIMEOUT_MS, 12000, 3000, 30000);
const ROW_WAIT_TIMEOUT_MS = positiveInt(process.env.ROW_WAIT_TIMEOUT_MS, 6500, 1000, 15000);
const QUEUE_TIMEOUT_MS = positiveInt(process.env.QUEUE_TIMEOUT_MS, 7000, 500, 20000);
const FRESH_CACHE_MS = positiveInt(process.env.FRESH_CACHE_MS, 8000, 0, 60000);
const STALE_CACHE_MS = positiveInt(process.env.STALE_CACHE_MS, 45000, 0, 300000);
const MAX_CACHE_ENTRIES = positiveInt(process.env.MAX_CACHE_ENTRIES, 250, 10, 2000);

let browser = null;
let context = null;
let shuttingDown = false;
let startPromise = null;

const availablePages = [];
const waiters = [];
const cache = new Map();
const inFlight = new Map();

const stats = {
  startedAt: new Date().toISOString(),
  requests: 0,
  cacheHits: 0,
  staleRescues: 0,
  coalesced: 0,
  browserFetches: 0,
  browserErrors: 0,
  queueTimeouts: 0,
  browserRestarts: 0
};

function positiveInt(value, fallback, min, max) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, Math.trunc(parsed)));
}

function authOk(req) {
  if (!BROWSER_TOKEN) return false;
  const supplied = String(req.headers.authorization || "").replace(/^Bearer\s+/i, "");
  const expectedBuffer = Buffer.from(BROWSER_TOKEN);
  const suppliedBuffer = Buffer.from(supplied);
  return expectedBuffer.length === suppliedBuffer.length &&
    crypto.timingSafeEqual(expectedBuffer, suppliedBuffer);
}

function browserOptions() {
  return {
    viewport: { width: 390, height: 844 },
    userAgent:
      "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) " +
      "AppleWebKit/605.1.15 Mobile/15E148 Safari/604.1",
    locale: "en-AU",
    timezoneId: "Australia/Perth"
  };
}

function stopUrl(stopId) {
  return `https://136213.mobi/RealTime/RealTimeStopResults.aspx?SN=${encodeURIComponent(stopId)}`;
}

function cacheKey(stopId, limit) {
  return `${stopId}|${limit}`;
}

function pruneCache(now = Date.now()) {
  for (const [key, entry] of cache.entries()) {
    if (!entry || entry.staleUntil <= now) cache.delete(key);
  }
  while (cache.size > MAX_CACHE_ENTRIES) {
    const oldest = cache.keys().next().value;
    if (oldest == null) break;
    cache.delete(oldest);
  }
}

function getCache(key, allowStale = false) {
  const now = Date.now();
  const entry = cache.get(key);
  if (!entry) return null;
  const valid = allowStale ? entry.staleUntil > now : entry.expiresAt > now;
  if (!valid) {
    if (entry.staleUntil <= now) cache.delete(key);
    return null;
  }
  cache.delete(key);
  cache.set(key, entry);
  return {
    payload: structuredCloneSafe(entry.payload),
    ageMs: Math.max(0, now - entry.createdAt),
    stale: entry.expiresAt <= now
  };
}

function setCache(key, payload) {
  const now = Date.now();
  cache.set(key, {
    payload: structuredCloneSafe(payload),
    createdAt: now,
    expiresAt: now + FRESH_CACHE_MS,
    staleUntil: now + Math.max(FRESH_CACHE_MS, STALE_CACHE_MS)
  });
  pruneCache(now);
}

function structuredCloneSafe(value) {
  try {
    if (typeof structuredClone === "function") return structuredClone(value);
  } catch (_) {}
  return JSON.parse(JSON.stringify(value));
}

async function configurePage(page) {
  page.setDefaultNavigationTimeout(NAVIGATION_TIMEOUT_MS);
  page.setDefaultTimeout(ROW_WAIT_TIMEOUT_MS);
  await page.route("**/*", async route => {
    const type = route.request().resourceType();
    if (["image", "font", "media"].includes(type)) {
      await route.abort().catch(() => null);
      return;
    }
    await route.continue().catch(() => null);
  });
  page.on("crash", () => {
    void replaceDeadPage(page);
  });
  return page;
}

async function ensureBrowser() {
  if (browser && context && browser.isConnected()) return;
  if (startPromise) return startPromise;

  startPromise = (async () => {
    await closeBrowser();
    browser = await chromium.launch({
      headless: true,
      args: [
        "--disable-dev-shm-usage",
        "--disable-background-networking",
        "--disable-default-apps",
        "--disable-extensions",
        "--disable-sync",
        "--metrics-recording-only",
        "--no-first-run"
      ]
    });
    browser.on("disconnected", () => {
      browser = null;
      context = null;
      availablePages.splice(0, availablePages.length);
      rejectAllWaiters(new Error("Browser disconnected"));
    });
    context = await browser.newContext(browserOptions());
    availablePages.splice(0, availablePages.length);
    for (let index = 0; index < POOL_SIZE; index += 1) {
      availablePages.push(await configurePage(await context.newPage()));
    }
    stats.browserRestarts += 1;
  })();

  try {
    await startPromise;
  } finally {
    startPromise = null;
  }
}

function rejectAllWaiters(error) {
  while (waiters.length) {
    const waiter = waiters.shift();
    clearTimeout(waiter.timer);
    waiter.reject(error);
  }
}

async function closeBrowser() {
  availablePages.splice(0, availablePages.length);
  rejectAllWaiters(new Error("Browser closing"));
  if (context) await context.close().catch(() => null);
  if (browser) await browser.close().catch(() => null);
  context = null;
  browser = null;
}

async function replaceDeadPage(deadPage) {
  const index = availablePages.indexOf(deadPage);
  if (index >= 0) availablePages.splice(index, 1);
  await deadPage.close().catch(() => null);
  if (shuttingDown) return;
  try {
    await ensureBrowser();
    const replacement = await configurePage(await context.newPage());
    releasePage(replacement);
  } catch (error) {
    console.error("Failed to replace browser page:", error.message);
  }
}

async function acquirePage() {
  await ensureBrowser();
  const page = availablePages.pop();
  if (page) return page;

  return new Promise((resolve, reject) => {
    const waiter = { resolve, reject, timer: null };
    waiter.timer = setTimeout(() => {
      const index = waiters.indexOf(waiter);
      if (index >= 0) waiters.splice(index, 1);
      stats.queueTimeouts += 1;
      reject(new Error("Browser queue timeout"));
    }, QUEUE_TIMEOUT_MS);
    waiters.push(waiter);
  });
}

function releasePage(page) {
  if (!page || page.isClosed() || shuttingDown) return;
  const waiter = waiters.shift();
  if (waiter) {
    clearTimeout(waiter.timer);
    waiter.resolve(page);
    return;
  }
  availablePages.push(page);
}

async function scrapeStop(stopId, limit) {
  const page = await acquirePage();
  const startedAt = Date.now();
  stats.browserFetches += 1;

  try {
    await page.goto(stopUrl(stopId), {
      waitUntil: "domcontentloaded",
      timeout: NAVIGATION_TIMEOUT_MS
    });

    await page.waitForSelector(".tpm_row_timetable", {
      state: "attached",
      timeout: ROW_WAIT_TIMEOUT_MS
    }).catch(() => null);

    const parsed = await page.evaluate(limitValue => {
      const rows = Array.from(document.querySelectorAll(".tpm_row_timetable"));
      const headingCandidates = [
        document.querySelector("h1"),
        document.querySelector("h2"),
        document.querySelector(".stop-name"),
        document.querySelector(".page-title")
      ];
      const stopName = headingCandidates
        .map(node => node?.textContent?.replace(/\s+/g, " ").trim())
        .find(Boolean) || null;

      const services = rows
        .map(row => {
          const text = (row.innerText || row.textContent || "")
            .replace(/\s+/g, " ")
            .trim();
          if (!text) return null;

          const route = text.match(/\b([A-Z]?\d{1,4}[A-Z]?|[A-Z]+ CAT)\b/)?.[1] || null;
          const destination =
            row.querySelector(".route-display-name strong")?.innerText?.trim() ||
            text.match(/To\s+.+?(?=\s+Depart from stop|\s+\d+\s*MIN|\s+\(sched)/i)?.[0]?.trim() ||
            null;
          const stopText =
            Array.from(row.querySelectorAll(".route-display-name"))
              .map(el => el.innerText.trim())
              .find(value => value.toLowerCase().includes("depart from stop")) ||
            "Depart from stop";
          const tripId = row.getAttribute("data-tripid") || null;
          const fleet = row.getAttribute("data-fleet") || null;
          const isLive =
            row.classList.contains("fleet-running") ||
            Boolean(fleet) ||
            /\bLIVE\b/i.test(text);
          const due =
            text.match(/\b\d+\s*MIN\b/i)?.[0]?.replace(/\s+/g, " ").toUpperCase() ||
            (/\barriving\b/i.test(text) ? "Arriving" : null);
          const scheduled = /\(sched\.\)/i.test(text);
          const time = text.match(/\b\d{1,2}:\d{2}\s*(am|pm)\b/i)?.[0]?.replace(/\s+/g, "") || null;

          return {
            route,
            destination,
            stopText,
            due,
            time,
            statusText: isLive ? "Live" : "Scheduled",
            scheduled,
            live: isLive,
            fleetNumber: fleet,
            fleet,
            tripId,
            detailURL: fleet
              ? `https://136213.mobi/RealTime/RealTimeFleetTrip.aspx?nq=true&fleet=${encodeURIComponent(fleet)}`
              : null,
            rawText: text
          };
        })
        .filter(service => service && service.route && service.destination)
        .slice(0, limitValue);

      return { stopName, services };
    }, limit);

    return {
      ok: true,
      stopId,
      stopName: parsed.stopName || `Stop ${stopId}`,
      source: "136213-browser-v2",
      count: parsed.services.length,
      services: parsed.services,
      fetchedAt: new Date().toISOString(),
      timings: {
        browserMs: Date.now() - startedAt
      }
    };
  } finally {
    try {
      await page.evaluate(() => {
        window.stop?.();
        localStorage.clear();
        sessionStorage.clear();
      });
    } catch (_) {}
    releasePage(page);
  }
}

async function fetchStopShared(stopId, limit) {
  const key = cacheKey(stopId, limit);
  const fresh = getCache(key, false);
  if (fresh) {
    stats.cacheHits += 1;
    return {
      ...fresh.payload,
      cache: { hit: true, stale: false, ageMs: fresh.ageMs }
    };
  }

  const existing = inFlight.get(key);
  if (existing) {
    stats.coalesced += 1;
    const payload = await existing;
    return { ...payload, cache: { hit: false, coalesced: true } };
  }

  const promise = (async () => {
    try {
      const payload = await scrapeStop(stopId, limit);
      setCache(key, payload);
      return payload;
    } catch (error) {
      stats.browserErrors += 1;
      const stale = getCache(key, true);
      if (stale) {
        stats.staleRescues += 1;
        return {
          ...stale.payload,
          degraded: true,
          cache: { hit: true, stale: true, ageMs: stale.ageMs },
          warning: String(error.message || error)
        };
      }
      throw error;
    }
  })();

  inFlight.set(key, promise);
  try {
    return await promise;
  } finally {
    if (inFlight.get(key) === promise) inFlight.delete(key);
  }
}

app.get("/", (req, res) => {
  res.json({
    ok: true,
    service: "transperth-browser-v2",
    region: process.env.RENDER_REGION || null,
    poolSize: POOL_SIZE,
    availablePages: availablePages.length,
    queuedRequests: waiters.length,
    cacheEntries: cache.size,
    inFlight: inFlight.size,
    stats,
    endpoints: ["/health", "/live-stop/26768?limit=5"]
  });
});

app.get("/health", async (req, res) => {
  try {
    await ensureBrowser();
    res.status(200).json({
      ok: true,
      browserConnected: Boolean(browser?.isConnected()),
      poolSize: POOL_SIZE,
      availablePages: availablePages.length,
      queuedRequests: waiters.length
    });
  } catch (error) {
    res.status(503).json({ ok: false, error: String(error.message || error) });
  }
});

app.get("/live-stop/:stopId", async (req, res) => {
  stats.requests += 1;
  if (!authOk(req)) {
    return res.status(401).json({ ok: false, error: "Unauthorized" });
  }

  const stopId = String(req.params.stopId || "").trim();
  if (!/^\d{1,8}$/.test(stopId)) {
    return res.status(400).json({ ok: false, error: "Invalid stop number" });
  }
  const limit = positiveInt(req.query.limit, 5, 1, 24);
  const startedAt = Date.now();

  try {
    const payload = await fetchStopShared(stopId, limit);
    res.set("Cache-Control", "no-store");
    return res.json({
      ...payload,
      timings: {
        ...(payload.timings || {}),
        totalMs: Date.now() - startedAt
      }
    });
  } catch (error) {
    const message = String(error.message || error);
    const status = /queue timeout/i.test(message) ? 503 : 504;
    return res.status(status).json({
      ok: false,
      stopId,
      error: message,
      fetchedAt: new Date().toISOString(),
      timings: { totalMs: Date.now() - startedAt }
    });
  }
});

app.use((error, req, res, next) => {
  console.error("Unhandled request error:", error);
  if (res.headersSent) return next(error);
  res.status(500).json({ ok: false, error: "Internal server error" });
});

async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`Received ${signal}; closing browser service.`);
  await closeBrowser();
  process.exit(0);
}

process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("unhandledRejection", error => {
  console.error("Unhandled rejection:", error);
});
process.on("uncaughtException", error => {
  console.error("Uncaught exception:", error);
});

app.listen(PORT, async () => {
  try {
    await ensureBrowser();
    console.log(`Transperth browser v2 listening on port ${PORT}; pool=${POOL_SIZE}`);
  } catch (error) {
    console.error("Browser startup failed:", error);
    process.exit(1);
  }
});
