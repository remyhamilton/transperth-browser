"use strict";

// Keep the Playwright browser inside the deployed project rather than Render's
// temporary build cache. This must be set before importing Playwright.
if (!process.env.PLAYWRIGHT_BROWSERS_PATH) {
  process.env.PLAYWRIGHT_BROWSERS_PATH = "0";
}

const fs = require("fs");
const crypto = require("crypto");
const express = require("express");
const { chromium } = require("playwright");

const app = express();
app.disable("x-powered-by");
app.use(express.json({ limit: "64kb" }));

const PORT = positiveInt(process.env.PORT, 3000, 1, 65535);
const BROWSER_TOKEN = String(process.env.BROWSER_TOKEN || "").trim();
const POOL_SIZE = positiveInt(process.env.BROWSER_POOL_SIZE, 2, 1, 4);
const NAVIGATION_TIMEOUT_MS = positiveInt(process.env.NAVIGATION_TIMEOUT_MS, 12000, 3000, 30000);
const ROW_WAIT_TIMEOUT_MS = positiveInt(process.env.ROW_WAIT_TIMEOUT_MS, 6500, 1000, 15000);
const QUEUE_TIMEOUT_MS = positiveInt(process.env.QUEUE_TIMEOUT_MS, 7000, 500, 20000);
const FRESH_CACHE_MS = positiveInt(process.env.FRESH_CACHE_MS, 8000, 0, 60000);
const STALE_CACHE_MS = positiveInt(process.env.STALE_CACHE_MS, 45000, 0, 300000);
const MAX_CACHE_ENTRIES = positiveInt(process.env.MAX_CACHE_ENTRIES, 250, 10, 2000);
const BATCH_MAX_STOPS = positiveInt(process.env.BATCH_MAX_STOPS, 60, 2, 120);
const BATCH_ROW_WAIT_TIMEOUT_MS = positiveInt(process.env.BATCH_ROW_WAIT_TIMEOUT_MS, 900, 250, 4000);
const BATCH_FRESH_CACHE_MS = positiveInt(process.env.BATCH_FRESH_CACHE_MS, 30000, 1000, 120000);
const BATCH_STALE_CACHE_MS = positiveInt(process.env.BATCH_STALE_CACHE_MS, 1800000, 5000, 3600000);
const BATCH_CACHE_MAX_ENTRIES = positiveInt(process.env.BATCH_CACHE_MAX_ENTRIES, 80, 4, 250);
const BATCH_KEEP_WARM_MS = positiveInt(process.env.BATCH_KEEP_WARM_MS, 900000, 60000, 3600000);
const BATCH_REFRESH_INTERVAL_MS = positiveInt(process.env.BATCH_REFRESH_INTERVAL_MS, 45000, 10000, 300000);
const WORKER_BASE_URL = String(
  process.env.WORKER_BASE_URL || "https://twilight-wildflower-4e89.remy-hamilton.workers.dev"
).trim().replace(/\/$/, "");
const SERVICE_WARM_MAX_FLEETS = positiveInt(process.env.SERVICE_WARM_MAX_FLEETS, 12, 1, 24);
const SERVICE_WARM_CONCURRENCY = positiveInt(process.env.SERVICE_WARM_CONCURRENCY, 2, 1, 4);
// v2.5: a complete selected-service packet can legitimately take longer than
// nine seconds when the Worker must resolve both the stop sequence and map line.
// This work is background-only, so reliability is more important than a short
// orchestration timeout.
const SERVICE_WARM_TIMEOUT_MS = positiveInt(process.env.SERVICE_WARM_TIMEOUT_MS, 30000, 5000, 45000);
const SERVICE_WARM_RECENT_MS = positiveInt(process.env.SERVICE_WARM_RECENT_MS, 45000, 5000, 300000);
const SERVICE_WARM_FAILED_RECENT_MS = positiveInt(process.env.SERVICE_WARM_FAILED_RECENT_MS, 2000, 500, 10000);
const SERVICE_WARM_QUEUE_MAX = positiveInt(process.env.SERVICE_WARM_QUEUE_MAX, 80, 8, 300);

let browser = null;
let context = null;
let shuttingDown = false;
let startPromise = null;

const availablePages = [];
const waiters = [];
const cache = new Map();
const inFlight = new Map();
const batchCache = new Map();
const batchInFlight = new Map();
const serviceWarmPending = [];
const serviceWarmInFlight = new Map();
const serviceWarmRecent = new Map();
let serviceWarmActive = 0;

const stats = {
  startedAt: new Date().toISOString(),
  requests: 0,
  cacheHits: 0,
  staleRescues: 0,
  coalesced: 0,
  browserFetches: 0,
  browserErrors: 0,
  queueTimeouts: 0,
  browserRestarts: 0,
  batchRequests: 0,
  batchCacheHits: 0,
  batchStaleHits: 0,
  batchRefreshes: 0,
  batchErrors: 0,
  prewarmRuns: 0,
  serviceWarmRequests: 0,
  serviceWarmQueued: 0,
  serviceWarmSkippedRecent: 0,
  serviceWarmCoalesced: 0,
  serviceWarmCompleted: 0,
  serviceWarmFailed: 0
};

function positiveInt(value, fallback, min, max) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, Math.trunc(parsed)));
}

function cleanFleet(value) {
  const digits = String(value || "").replace(/\D+/g, "");
  return /^\d{3,5}$/.test(digits) ? digits : "";
}

function cleanServiceWarmRow(raw) {
  if (typeof raw === "string" || typeof raw === "number") {
    const fleet = cleanFleet(raw);
    return fleet ? { fleet } : null;
  }
  if (!raw || typeof raw !== "object") return null;
  const fleet = cleanFleet(raw.fleet || raw.fleetNumber || raw.vehicle);
  if (!fleet) return null;
  const tripId = String(raw.tripId || raw.tripID || raw.matchedTripId || "").trim().slice(0, 100);
  const stopId = String(raw.stopId || raw.stopID || raw.selectedStopId || "").trim().replace(/[^0-9A-Za-z_.:-]+/g, "").slice(0, 80);
  return {
    fleet,
    ...(tripId ? { tripId } : {}),
    ...(stopId ? { stopId } : {})
  };
}

function serviceWarmRowsFromRequest(req) {
  const bodyRows = Array.isArray(req.body?.rows)
    ? req.body.rows
    : (Array.isArray(req.body?.fleets) ? req.body.fleets : []);
  const queryRows = String(req.query.fleets || req.query.fleet || "")
    .split(/[,\s;|]+/)
    .filter(Boolean);
  const rows = [...bodyRows, ...queryRows]
    .map(cleanServiceWarmRow)
    .filter(Boolean);
  const seen = new Set();
  const out = [];
  for (const row of rows) {
    const key = `${row.fleet}|${row.tripId || "*"}|${row.stopId || "*"}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(row);
    if (out.length >= SERVICE_WARM_MAX_FLEETS) break;
  }
  return out;
}

function serviceWarmKey(row) {
  return `${row.fleet}|${row.tripId || "*"}|${row.stopId || "*"}`;
}

function pruneServiceWarmRecent(now = Date.now()) {
  for (const [key, entry] of serviceWarmRecent.entries()) {
    if (!entry || Number(entry.expiresAt || 0) <= now) serviceWarmRecent.delete(key);
  }
}

async function callWorkerServiceWarm(row, reason) {
  const target = new URL(`${WORKER_BASE_URL}/internal/serviceOpen/prewarmOne`);
  target.searchParams.set("fleet", row.fleet);
  if (row.tripId) target.searchParams.set("preferredTripId", row.tripId);
  if (row.stopId) target.searchParams.set("selectedStopId", row.stopId);
  target.searchParams.set("reason", String(reason || "render-service-warm-v2.5").slice(0, 100));
  target.searchParams.set("source", "render-v2.4");

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort("service-warm-timeout"), SERVICE_WARM_TIMEOUT_MS);
  const startedAt = Date.now();
  try {
    const response = await fetch(target, {
      signal: controller.signal,
      headers: {
        "Authorization": `Bearer ${BROWSER_TOKEN}`,
        "Accept": "application/json",
        "User-Agent": "Hubway-Render-ServiceWarm/2.5"
      }
    });
    const text = await response.text();
    let payload = null;
    try { payload = JSON.parse(text); } catch {}
    if (!response.ok || payload?.ok === false) {
      const details = payload && typeof payload === "object"
        ? [
            payload.error,
            payload.packetSource ? `packetSource=${payload.packetSource}` : null,
            Number.isFinite(Number(payload.stopCount)) ? `stops=${payload.stopCount}` : null,
            Number.isFinite(Number(payload.routePointCount)) ? `points=${payload.routePointCount}` : null,
            payload.storage?.persistentStored === false ? "persistentStore=false" : null
          ].filter(Boolean).join("; ")
        : "";
      throw new Error(details || `Worker service warm HTTP ${response.status}`);
    }
    return {
      ok: true,
      fleet: row.fleet,
      tripId: row.tripId || null,
      stopId: row.stopId || null,
      stored: payload?.stored === true,
      source: payload?.source || "worker-service-warm",
      elapsedMs: Date.now() - startedAt
    };
  } finally {
    clearTimeout(timer);
  }
}

function pumpServiceWarmQueue() {
  while (serviceWarmActive < SERVICE_WARM_CONCURRENCY && serviceWarmPending.length) {
    const job = serviceWarmPending.shift();
    if (!job) break;
    serviceWarmActive += 1;
    const key = serviceWarmKey(job.row);
    const promise = callWorkerServiceWarm(job.row, job.reason)
      .then(result => {
        stats.serviceWarmCompleted += 1;
        serviceWarmRecent.set(key, {
          result,
          expiresAt: Date.now() + SERVICE_WARM_RECENT_MS
        });
        job.resolve(result);
      })
      .catch(error => {
        stats.serviceWarmFailed += 1;
        const result = {
          ok: false,
          fleet: job.row.fleet,
          error: String(error.message || error),
          elapsedMs: Date.now() - job.startedAt
        };
        serviceWarmRecent.set(key, {
          result,
          // v2.5: do not suppress an immediate retry for ten seconds after a
          // transient Worker/R2 timeout.
          expiresAt: Date.now() + SERVICE_WARM_FAILED_RECENT_MS
        });
        job.resolve(result);
      })
      .finally(() => {
        serviceWarmActive = Math.max(0, serviceWarmActive - 1);
        if (serviceWarmInFlight.get(key) === promise) serviceWarmInFlight.delete(key);
        pumpServiceWarmQueue();
      });
    serviceWarmInFlight.set(key, promise);
  }
}

function enqueueServiceWarm(row, reason) {
  pruneServiceWarmRecent();
  const key = serviceWarmKey(row);
  const recent = serviceWarmRecent.get(key);
  if (recent) {
    stats.serviceWarmSkippedRecent += 1;
    return Promise.resolve({
      ...(recent.result || { ok: true, fleet: row.fleet }),
      skipped: true,
      reason: "recently-warmed"
    });
  }
  const existing = serviceWarmInFlight.get(key);
  if (existing) {
    stats.serviceWarmCoalesced += 1;
    return existing;
  }
  if (serviceWarmPending.length >= SERVICE_WARM_QUEUE_MAX) {
    stats.serviceWarmFailed += 1;
    return Promise.resolve({ ok: false, fleet: row.fleet, error: "service-warm-queue-full" });
  }

  let resolveJob;
  const promise = new Promise(resolve => { resolveJob = resolve; });
  serviceWarmInFlight.set(key, promise);
  serviceWarmPending.push({
    row,
    reason,
    resolve: resolveJob,
    startedAt: Date.now()
  });
  stats.serviceWarmQueued += 1;
  pumpServiceWarmQueue();
  return promise;
}

function uniqueStopIds(raw, maxStops = BATCH_MAX_STOPS) {
  return Array.from(new Set(
    String(raw || "")
      .split(/[,\s;|]+/)
      .map(value => value.trim())
      .filter(value => /^\d{1,8}$/.test(value))
  )).slice(0, maxStops);
}

async function mapLimit(items, concurrency, operation) {
  const list = Array.isArray(items) ? items : [];
  if (!list.length) return [];
  const out = new Array(list.length);
  let nextIndex = 0;

  async function worker() {
    while (nextIndex < list.length) {
      const index = nextIndex++;
      out[index] = await operation(list[index], index);
    }
  }

  await Promise.all(
    Array.from(
      { length: Math.max(1, Math.min(concurrency, list.length)) },
      () => worker()
    )
  );
  return out;
}

function batchCacheKey(stopIds, perStop) {
  return `${stopIds.join(",")}|${perStop}`;
}

function pruneBatchCache(now = Date.now()) {
  for (const [key, entry] of batchCache.entries()) {
    if (!entry || entry.staleUntil <= now) batchCache.delete(key);
  }
  while (batchCache.size > BATCH_CACHE_MAX_ENTRIES) {
    const oldest = batchCache.keys().next().value;
    if (oldest == null) break;
    batchCache.delete(oldest);
  }
}

function getBatchCache(key, allowStale = false) {
  const now = Date.now();
  const entry = batchCache.get(key);
  if (!entry) return null;
  const valid = allowStale ? entry.staleUntil > now : entry.expiresAt > now;
  if (!valid) {
    if (entry.staleUntil <= now) batchCache.delete(key);
    return null;
  }
  entry.lastRequestedAt = now;
  batchCache.delete(key);
  batchCache.set(key, entry);
  return {
    payload: structuredCloneSafe(entry.payload),
    ageMs: Math.max(0, now - entry.createdAt),
    stale: entry.expiresAt <= now
  };
}

function setBatchCache(key, payload) {
  const now = Date.now();
  batchCache.set(key, {
    payload: structuredCloneSafe(payload),
    stopIds: Array.isArray(payload?.stopIds) ? [...payload.stopIds] : [],
    perStop: Number(payload?.perStop) || 10,
    createdAt: now,
    lastRequestedAt: now,
    expiresAt: now + BATCH_FRESH_CACHE_MS,
    staleUntil: now + Math.max(BATCH_FRESH_CACHE_MS, BATCH_STALE_CACHE_MS)
  });
  pruneBatchCache(now);
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

    const executablePath = chromium.executablePath();
    if (!fs.existsSync(executablePath)) {
      throw new Error(
        `Playwright Chromium is missing at ${executablePath}. ` +
        `Run the Render build command with PLAYWRIGHT_BROWSERS_PATH=0.`
      );
    }

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

async function scrapeStop(stopId, limit, options = {}) {
  const page = await acquirePage();
  const startedAt = Date.now();
  stats.browserFetches += 1;

  try {
    await page.goto(stopUrl(stopId), {
      waitUntil: "domcontentloaded",
      timeout: NAVIGATION_TIMEOUT_MS
    });

    const rowWaitMs = positiveInt(
      options.rowWaitMs,
      ROW_WAIT_TIMEOUT_MS,
      250,
      ROW_WAIT_TIMEOUT_MS
    );
    const immediateRowCount = await page.locator(".tpm_row_timetable").count().catch(() => 0);
    if (immediateRowCount === 0) {
      await page.waitForSelector(".tpm_row_timetable", {
        state: "attached",
        timeout: rowWaitMs
      }).catch(() => null);
    }

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
      source: "136213-browser-v2.4",
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

async function fetchStopShared(stopId, limit, options = {}) {
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
      const payload = await scrapeStop(stopId, limit, options);
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


async function buildStopsBatch(stopIds, perStop) {
  const startedAt = Date.now();
  stats.batchRefreshes += 1;

  const rowsByStop = await mapLimit(
    stopIds,
    POOL_SIZE,
    async stopId => {
      const stopStartedAt = Date.now();
      try {
        const payload = await fetchStopShared(stopId, perStop, {
          rowWaitMs: BATCH_ROW_WAIT_TIMEOUT_MS
        });
        const services = (Array.isArray(payload?.services) ? payload.services : [])
          .slice(0, perStop)
          .map(service => ({ ...service, stopId }));

        return {
          stopId,
          ok: payload?.ok !== false,
          stopName: payload?.stopName || `Stop ${stopId}`,
          source: payload?.source || "136213-browser-v2.6-batch",
          count: services.length,
          services,
          cache: payload?.cache || null,
          ms: Date.now() - stopStartedAt
        };
      } catch (error) {
        return {
          stopId,
          ok: false,
          stopName: `Stop ${stopId}`,
          source: "136213-browser-v2.6-batch-error",
          count: 0,
          services: [],
          error: String(error.message || error),
          ms: Date.now() - stopStartedAt
        };
      }
    }
  );

  const services = rowsByStop.flatMap(row => row.services || []);
  return {
    ok: true,
    source: "136213-browser-v2.6-batched-stops",
    grouped: true,
    stopIds,
    perStop,
    count: services.length,
    componentCount: stopIds.length,
    services,
    rowsByStop,
    fetchedAt: new Date().toISOString(),
    timings: {
      totalMs: Date.now() - startedAt
    }
  };
}

function beginBatchRefresh(key, stopIds, perStop) {
  const existing = batchInFlight.get(key);
  if (existing) return existing;

  const promise = buildStopsBatch(stopIds, perStop)
    .then(payload => {
      setBatchCache(key, payload);
      return payload;
    })
    .finally(() => {
      if (batchInFlight.get(key) === promise) batchInFlight.delete(key);
    });

  batchInFlight.set(key, promise);
  return promise;
}

async function fetchStopsBatchShared(stopIds, perStop, { forceRefresh = false } = {}) {
  const key = batchCacheKey(stopIds, perStop);

  if (!forceRefresh) {
    const fresh = getBatchCache(key, false);
    if (fresh) {
      stats.batchCacheHits += 1;
      return {
        ...fresh.payload,
        cache: { hit: true, stale: false, ageMs: fresh.ageMs }
      };
    }

    const stale = getBatchCache(key, true);
    if (stale) {
      stats.batchStaleHits += 1;
      void beginBatchRefresh(key, stopIds, perStop).catch(error => {
        stats.batchErrors += 1;
        console.error("Background batch refresh failed:", error.message);
      });
      return {
        ...stale.payload,
        cache: {
          hit: true,
          stale: true,
          ageMs: stale.ageMs,
          refreshQueued: true
        }
      };
    }
  }

  const existing = batchInFlight.get(key);
  if (existing) return existing;

  try {
    return await beginBatchRefresh(key, stopIds, perStop);
  } catch (error) {
    stats.batchErrors += 1;
    const stale = getBatchCache(key, true);
    if (stale) {
      return {
        ...stale.payload,
        degraded: true,
        warning: String(error.message || error),
        cache: { hit: true, stale: true, ageMs: stale.ageMs }
      };
    }
    throw error;
  }
}


async function refreshRecentlyRequestedGroupedStops() {
  const now = Date.now();
  const candidates = [];

  for (const [key, entry] of batchCache.entries()) {
    if (!entry || !Array.isArray(entry.stopIds) || !entry.stopIds.length) continue;
    const lastRequestedAt = Number(entry.lastRequestedAt || entry.createdAt || 0);
    if (!lastRequestedAt || now - lastRequestedAt > BATCH_KEEP_WARM_MS) continue;
    if (batchInFlight.has(key)) continue;

    // Refresh before the fresh window expires so an active grouped stop continues
    // returning from cache instead of becoming a cold 20-30 second batch.
    if (Number(entry.expiresAt || 0) - now > BATCH_REFRESH_INTERVAL_MS) continue;
    candidates.push({
      key,
      stopIds: entry.stopIds,
      perStop: entry.perStop
    });
  }

  await mapLimit(candidates, 1, async candidate => {
    try {
      await beginBatchRefresh(
        candidate.key,
        candidate.stopIds,
        candidate.perStop
      );
    } catch (error) {
      stats.batchErrors += 1;
      console.error("Active grouped-stop refresh failed:", error.message);
    }
  });
}

async function prewarmKnownGroupedStops() {
  const perthBusport = ["27172", "27180", "27184"];
  const elizabethQuay = [
    "12195", "12196", "12197", "12198", "12199", "12200",
    "12205", "12206", "12210", "12211", "12212", "12213",
    "12214", "12215", "12216", "12217", "12218", "12219",
    "12220", "12221", "12222", "12223", "12224", "12225",
    "12226", "12227", "12228", "12229", "28068"
  ];

  stats.prewarmRuns += 1;
  for (const group of [perthBusport, elizabethQuay]) {
    const key = batchCacheKey(group, 10);
    await beginBatchRefresh(key, group, 10).catch(error => {
      stats.batchErrors += 1;
      console.error("Grouped-stop prewarm failed:", error.message);
    });
  }
}


app.all("/warm-service-packets", async (req, res) => {
  stats.requests += 1;
  stats.serviceWarmRequests += 1;
  if (!authOk(req)) {
    return res.status(401).json({ ok: false, error: "Unauthorized" });
  }

  const rows = serviceWarmRowsFromRequest(req);
  if (!rows.length) {
    return res.status(400).json({
      ok: false,
      error: "Provide fleets or rows with 3-5 digit fleet numbers"
    });
  }

  const reason = String(req.body?.reason || req.query.reason || "worker-request-v2.5").slice(0, 100);
  const wait = String(req.query.wait || req.body?.wait || "0") === "1";
  const jobs = rows.map(row => enqueueServiceWarm(row, reason));
  res.set("Cache-Control", "no-store");

  if (!wait) {
    return res.status(202).json({
      ok: true,
      source: "transperth-browser-v2.6-service-packet-prewarm",
      queued: true,
      requested: rows.length,
      fleets: rows.map(row => row.fleet),
      active: serviceWarmActive,
      pending: serviceWarmPending.length,
      fetchedAt: new Date().toISOString()
    });
  }

  const results = await Promise.all(jobs);
  return res.json({
    ok: results.some(result => result?.ok),
    source: "transperth-browser-v2.6-service-packet-prewarm",
    queued: false,
    requested: rows.length,
    completed: results.filter(result => result?.ok).length,
    failed: results.filter(result => !result?.ok).length,
    results,
    fetchedAt: new Date().toISOString()
  });
});

app.get("/warm-service-packets/status", (req, res) => {
  if (!authOk(req)) {
    return res.status(401).json({ ok: false, error: "Unauthorized" });
  }
  pruneServiceWarmRecent();
  return res.json({
    ok: true,
    source: "transperth-browser-v2.6-service-packet-prewarm-status",
    active: serviceWarmActive,
    pending: serviceWarmPending.length,
    inFlight: serviceWarmInFlight.size,
    recent: serviceWarmRecent.size,
    stats,
    fetchedAt: new Date().toISOString()
  });
});

app.get("/", (req, res) => {
  res.json({
    ok: true,
    service: "transperth-browser-v2.5",
    region: process.env.RENDER_REGION || null,
    poolSize: POOL_SIZE,
    availablePages: availablePages.length,
    queuedRequests: waiters.length,
    cacheEntries: cache.size,
    inFlight: inFlight.size,
    batchCacheEntries: batchCache.size,
    batchInFlight: batchInFlight.size,
    serviceWarmActive,
    serviceWarmPending: serviceWarmPending.length,
    serviceWarmInFlight: serviceWarmInFlight.size,
    serviceWarmRecent: serviceWarmRecent.size,
    stats,
    endpoints: [
      "/health",
      "/live-stop/26768?limit=5",
      "/live-stops?stops=27172,27180,27184&perStop=10",
      "/warm-service-packets",
      "/warm-service-packets/status"
    ]
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


app.get("/live-stops", async (req, res) => {
  stats.requests += 1;
  stats.batchRequests += 1;

  if (!authOk(req)) {
    return res.status(401).json({ ok: false, error: "Unauthorized" });
  }

  const stopIds = uniqueStopIds(req.query.stops || req.query.stopIds || req.query.ids);
  if (!stopIds.length) {
    return res.status(400).json({
      ok: false,
      error: "Missing stop IDs. Use stops=123,456"
    });
  }

  const perStop = positiveInt(req.query.perStop || req.query.limitPerStop, 10, 1, 24);
  const forceRefresh = String(req.query.refresh || "") === "1";
  const startedAt = Date.now();

  try {
    const payload = await fetchStopsBatchShared(stopIds, perStop, { forceRefresh });
    res.set("Cache-Control", "no-store");
    return res.json({
      ...payload,
      timings: {
        ...(payload.timings || {}),
        requestTotalMs: Date.now() - startedAt
      }
    });
  } catch (error) {
    return res.status(504).json({
      ok: false,
      source: "136213-browser-v2.6-batched-stops",
      stopIds,
      error: String(error.message || error),
      fetchedAt: new Date().toISOString(),
      timings: { requestTotalMs: Date.now() - startedAt }
    });
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
    console.log(`Transperth browser v2.6 listening on port ${PORT}; pool=${POOL_SIZE}`);
    console.log(`Playwright Chromium: ${chromium.executablePath()}`);
    void prewarmKnownGroupedStops().then(() => {
      console.log("Known grouped-stop caches prewarmed.");
    });

    const groupedRefreshTimer = setInterval(() => {
      void refreshRecentlyRequestedGroupedStops();
    }, BATCH_REFRESH_INTERVAL_MS);
    groupedRefreshTimer.unref?.();
  } catch (error) {
    console.error("Browser startup failed:", error);
    process.exit(1);
  }
});
