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
const LIVE_ROW_WAIT_TIMEOUT_MS = positiveInt(process.env.LIVE_ROW_WAIT_TIMEOUT_MS, 1800, 250, 6000);
const LIVE_SETTLE_MS = positiveInt(process.env.LIVE_SETTLE_MS, 80, 0, 500);
const BLOCK_STYLESHEETS = String(process.env.BLOCK_STYLESHEETS || "1") !== "0";
const QUEUE_TIMEOUT_MS = positiveInt(process.env.QUEUE_TIMEOUT_MS, 7000, 500, 20000);
const FRESH_CACHE_MS = positiveInt(process.env.FRESH_CACHE_MS, 8000, 0, 60000);
const STALE_CACHE_MS = positiveInt(process.env.STALE_CACHE_MS, 45000, 0, 300000);
const MAX_CACHE_ENTRIES = positiveInt(process.env.MAX_CACHE_ENTRIES, 250, 10, 2000);
const BATCH_MAX_STOPS = positiveInt(process.env.BATCH_MAX_STOPS, 60, 2, 120);
const BATCH_ROW_WAIT_TIMEOUT_MS = positiveInt(process.env.BATCH_ROW_WAIT_TIMEOUT_MS, 900, 250, 4000);
const BATCH_FRESH_CACHE_MS = positiveInt(process.env.BATCH_FRESH_CACHE_MS, 30000, 1000, 120000);
const BATCH_STALE_CACHE_MS = positiveInt(process.env.BATCH_STALE_CACHE_MS, 1800000, 5000, 3600000);
const BATCH_CACHE_MAX_ENTRIES = positiveInt(process.env.BATCH_CACHE_MAX_ENTRIES, 80, 4, 250);
const BATCH_KEEP_WARM_MS = positiveInt(process.env.BATCH_KEEP_WARM_MS, 300000, 60000, 3600000);
const BATCH_REFRESH_INTERVAL_MS = positiveInt(process.env.BATCH_REFRESH_INTERVAL_MS, 45000, 10000, 300000);
const BATCH_REFRESH_LEAD_MS = positiveInt(process.env.BATCH_REFRESH_LEAD_MS, 15000, 1000, 120000);
const BATCH_REFRESH_MAX_PER_TICK = positiveInt(process.env.BATCH_REFRESH_MAX_PER_TICK, 2, 1, 12);
const PAGE_MAX_USES = positiveInt(process.env.PAGE_MAX_USES, 80, 10, 500);
const MEMORY_CHECK_INTERVAL_MS = positiveInt(process.env.MEMORY_CHECK_INTERVAL_MS, 60000, 15000, 300000);
const HEAP_SOFT_LIMIT_MB = positiveInt(process.env.HEAP_SOFT_LIMIT_MB, 650, 128, 4096);
const SERVICE_WARM_RECENT_MAX_ENTRIES = positiveInt(process.env.SERVICE_WARM_RECENT_MAX_ENTRIES, 256, 16, 2000);
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
let groupedRefreshPromise = null;
let browserRecyclePromise = null;
let browserRecycleRequested = false;
let activeBrowserJobs = 0;

const availablePages = [];
const waiters = [];
const cache = new Map();
const inFlight = new Map();
const batchCache = new Map();
const batchInFlight = new Map();
// v3.1: service rows are never reused for strict-fresh requests. This map stores
// only which component stop IDs recently produced live rows, so a large grouped
// stop can scan the most promising stands first without returning stale times.
const groupedStopHotness = new Map();
const groupedSnapshotGeneration = new Map();
const serviceWarmPending = [];
const serviceWarmInFlight = new Map();
const serviceWarmRecent = new Map();
const managedPages = new Set();
const pageUseCount = new WeakMap();
const replacingPages = new WeakSet();
let serviceWarmActive = 0;
let lastMemorySnapshot = null;

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
  serviceWarmFailed: 0,
  groupedRefreshSkippedOverlap: 0,
  groupedRefreshCandidates: 0,
  pageRecycles: 0,
  memoryWarnings: 0,
  memoryRecycles: 0,
  strictFreshRequests: 0,
  strictFreshBatchRequests: 0,
  liveOnlyRowsDropped: 0,
  hotFirstBatchReturns: 0,
  groupedBackgroundCompletions: 0,
  completeSnapshotRequests: 0,
  completeSnapshotCacheHits: 0,
  completeSnapshotPublishes: 0,
  completeSnapshotColdBuilds: 0,
  componentBatchRequests: 0,
  componentBatchCompleted: 0,
  componentBatchFailedStops: 0
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
  while (serviceWarmRecent.size > SERVICE_WARM_RECENT_MAX_ENTRIES) {
    const oldest = serviceWarmRecent.keys().next().value;
    if (oldest == null) break;
    serviceWarmRecent.delete(oldest);
  }
}

async function callWorkerServiceWarm(row, reason) {
  const target = new URL(`${WORKER_BASE_URL}/internal/serviceOpen/prewarmOne`);
  target.searchParams.set("fleet", row.fleet);
  if (row.tripId) target.searchParams.set("preferredTripId", row.tripId);
  if (row.stopId) target.searchParams.set("selectedStopId", row.stopId);
  target.searchParams.set("reason", String(reason || "render-service-warm-v2.7").slice(0, 100));
  target.searchParams.set("source", "render-v2.7");

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort("service-warm-timeout"), SERVICE_WARM_TIMEOUT_MS);
  const startedAt = Date.now();
  try {
    const response = await fetch(target, {
      signal: controller.signal,
      headers: {
        "Authorization": `Bearer ${BROWSER_TOKEN}`,
        "Accept": "application/json",
        "User-Agent": "Hubway-Render-ServiceWarm/2.7"
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

function batchCacheKey(stopIds, perStop, completeOnly = false) {
  const stableIds = [...stopIds].sort((a, b) => Number(a) - Number(b) || String(a).localeCompare(String(b)));
  return `${completeOnly ? "complete" : "hot"}|${stableIds.join(",")}|${perStop}`;
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

function setBatchCache(key, payload, { markRequested = false } = {}) {
  const now = Date.now();
  const previous = batchCache.get(key);
  const previousRequestedAt = Number(previous?.lastRequestedAt || 0);
  const completeOnly = payload?.groupedCompleteSnapshot === true || payload?.completeOnly === true;

  if (completeOnly && payload?.groupedScanComplete === true) {
    const identity = `${(payload.stopIds || []).join(",")}|${Number(payload.perStop) || 10}`;
    const nextGeneration = Number(groupedSnapshotGeneration.get(identity) || 0) + 1;
    groupedSnapshotGeneration.set(identity, nextGeneration);
    payload.groupedGeneration = nextGeneration;
    payload.groupedSnapshotComplete = true;
    payload.groupedCompleteSnapshot = true;
    stats.completeSnapshotPublishes += 1;
  }

  batchCache.set(key, {
    payload: structuredCloneSafe(payload),
    stopIds: Array.isArray(payload?.stopIds) ? [...payload.stopIds] : [],
    perStop: Number(payload?.perStop) || 10,
    completeOnly,
    createdAt: now,
    lastRequestedAt: markRequested ? now : previousRequestedAt,
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

function cacheKey(stopId, limit, liveOnly = false) {
  return `${stopId}|${limit}|${liveOnly ? "live" : "mixed"}`;
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
  managedPages.add(page);
  pageUseCount.set(page, 0);
  page.once("close", () => {
    managedPages.delete(page);
  });
  page.setDefaultNavigationTimeout(NAVIGATION_TIMEOUT_MS);
  page.setDefaultTimeout(ROW_WAIT_TIMEOUT_MS);
  await page.route("**/*", async route => {
    const type = route.request().resourceType();
    if (["image", "font", "media"].includes(type) || (BLOCK_STYLESHEETS && type === "stylesheet")) {
      await route.abort().catch(() => null);
      return;
    }
    await route.continue().catch(() => null);
  });
  page.on("crash", () => {
    void replaceDeadPage(page, "page-crash");
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
      managedPages.clear();
      rejectAllWaiters(new Error("Browser disconnected"));
    });
    context = await browser.newContext(browserOptions());
    availablePages.splice(0, availablePages.length);
    managedPages.clear();
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
  managedPages.clear();
  context = null;
  browser = null;
}

async function replaceDeadPage(deadPage, reason = "dead") {
  if (!deadPage || replacingPages.has(deadPage)) return;
  replacingPages.add(deadPage);
  const index = availablePages.indexOf(deadPage);
  if (index >= 0) availablePages.splice(index, 1);
  managedPages.delete(deadPage);
  await deadPage.close().catch(() => null);
  if (shuttingDown) {
    replacingPages.delete(deadPage);
    return;
  }
  try {
    await ensureBrowser();
    // A browser disconnect can rebuild the whole pool while this replacement is
    // awaiting. Only add a page if the pool is still below its target size.
    if (context && browser?.isConnected() && managedPages.size < POOL_SIZE) {
      const replacement = await configurePage(await context.newPage());
      releasePage(replacement);
      stats.pageRecycles += 1;
    }
  } catch (error) {
    console.error(`Failed to replace browser page (${reason}):`, error.message);
  } finally {
    replacingPages.delete(deadPage);
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
  if (!page || page.isClosed() || shuttingDown || !managedPages.has(page)) return;
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
  activeBrowserJobs += 1;
  stats.browserFetches += 1;

  const liveOnly = options.liveOnly === true;

  try {
    await page.goto(stopUrl(stopId), {
      waitUntil: "domcontentloaded",
      timeout: NAVIGATION_TIMEOUT_MS
    });

    const rowWaitMs = positiveInt(
      options.rowWaitMs,
      liveOnly ? LIVE_ROW_WAIT_TIMEOUT_MS : ROW_WAIT_TIMEOUT_MS,
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
    if (LIVE_SETTLE_MS > 0) {
      await page.waitForTimeout(LIVE_SETTLE_MS).catch(() => null);
    }

    const parsed = await page.evaluate(({ limitValue, liveOnlyValue, stopIdValue }) => {
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

      const clean = value => String(value || "").replace(/\s+/g, " ").trim();
      const absolute = href => {
        if (!href) return null;
        try { return new URL(href, location.href).toString(); } catch { return null; }
      };
      const queryValue = (href, names) => {
        if (!href) return null;
        try {
          const u = new URL(href, location.href);
          for (const name of names) {
            const value = u.searchParams.get(name);
            if (value) return clean(value);
          }
        } catch {}
        return null;
      };

      const allServices = rows
        .map(row => {
          const text = clean(row.innerText || row.textContent || "");
          if (!text) return null;

          const links = Array.from(row.querySelectorAll("a[href]"));
          const detailHref = links
            .map(link => link.getAttribute("href"))
            .find(href => /RealTimeFleetTrip|fleet=/i.test(href || "")) || null;
          const detailURL = absolute(detailHref);
          const route =
            clean(row.getAttribute("data-route")) ||
            text.match(/\b([A-Z]?\d{1,4}[A-Z]?|[A-Z]+ CAT|Airport Line|Armadale Line|Ellenbrook Line|Fremantle Line|Mandurah Line|Midland Line|Thornlie-Cockburn Line|Yanchep Line)\b/i)?.[1] ||
            null;
          const destination =
            clean(row.querySelector(".route-display-name strong")?.innerText) ||
            clean(row.getAttribute("data-destination")) ||
            text.match(/To\s+.+?(?=\s+Depart from stop|\s+\d+\s*MIN|\s+\(sched|$)/i)?.[0]?.trim() ||
            null;
          const stopText =
            Array.from(row.querySelectorAll(".route-display-name"))
              .map(el => clean(el.innerText))
              .find(value => value.toLowerCase().includes("depart from stop")) ||
            "Depart from stop";

          const fleet =
            clean(row.getAttribute("data-fleet")) ||
            clean(row.dataset?.fleet) ||
            queryValue(detailHref, ["fleet", "fleetNumber", "vehicle"]) ||
            text.match(/\bFleet\s*#?\s*(\d{3,5})\b/i)?.[1] ||
            null;
          const tripId =
            clean(row.getAttribute("data-tripid")) ||
            clean(row.dataset?.tripid) ||
            clean(row.dataset?.tripId) ||
            queryValue(detailHref, ["tripId", "tripID", "trip", "t"]) ||
            null;
          const runNumber =
            clean(row.getAttribute("data-run")) ||
            clean(row.dataset?.run) ||
            text.match(/\b(?:Run|Service)\s*#?\s*([A-Z0-9-]{2,12})\b/i)?.[1] ||
            null;
          const platform =
            clean(row.getAttribute("data-platform")) ||
            text.match(/\bPlatform\s*([0-9]+[A-Z]?)\b/i)?.[1] ||
            null;

          const scheduled = /\(sched\.?\)|\bscheduled\b/i.test(text);
          const isLive = !scheduled && (
            row.classList.contains("fleet-running") ||
            row.classList.contains("live") ||
            Boolean(fleet) ||
            /\bLIVE\b|\barriving\b|\bdeparting\b/i.test(text)
          );
          const due =
            text.match(/\b\d+\s*MIN\b/i)?.[0]?.replace(/\s+/g, " ").toUpperCase() ||
            (/\barriving\b/i.test(text) ? "Arriving" : null);
          const dueMinutesMatch = due?.match(/\d+/);
          const minutesUntilDeparture = dueMinutesMatch
            ? Number(dueMinutesMatch[0])
            : (due === "Arriving" ? 0 : null);
          const time = text.match(/\b\d{1,2}[:.]\d{2}\s*(?:am|pm)\b/i)?.[0]?.replace(/\s+/g, "") || null;

          return {
            route: route ? clean(route) : null,
            destination: destination ? clean(destination) : null,
            stopText,
            stopId: String(stopIdValue),
            due,
            dueText: due,
            minutesUntilDeparture,
            time,
            departureTime: time,
            liveTime: isLive ? time : null,
            statusText: isLive ? "Live" : "Scheduled",
            scheduled,
            live: isLive,
            workerLive: isLive,
            workerMobiBoardRow: true,
            workerMobiLiveRow: isLive,
            workerMobiScheduledRow: !isLive,
            fleetNumber: fleet,
            fleet,
            tripId,
            runNumber,
            platform,
            detailURL: detailURL || (fleet
              ? `https://136213.mobi/RealTime/RealTimeFleetTrip.aspx?nq=true&fleet=${encodeURIComponent(fleet)}`
              : null),
            rawText: text.slice(0, 420)
          };
        })
        .filter(service => service && service.route && service.destination);

      const services = (liveOnlyValue
        ? allServices.filter(service => service.live === true)
        : allServices
      ).slice(0, limitValue);

      return {
        stopName,
        services,
        rawRowCount: allServices.length,
        liveRowCount: allServices.filter(service => service.live === true).length
      };
    }, { limitValue: limit, liveOnlyValue: liveOnly, stopIdValue: stopId });

    stats.liveOnlyRowsDropped += liveOnly
      ? Math.max(0, Number(parsed.rawRowCount || 0) - Number(parsed.liveRowCount || 0))
      : 0;

    const fetchedAt = new Date().toISOString();
    return {
      ok: true,
      stopId,
      stopName: parsed.stopName || `Stop ${stopId}`,
      source: "136213-browser-v3.1-fresh-live-board",
      freshLive: options.forceRefresh === true,
      liveOnly,
      rawRowCount: parsed.rawRowCount,
      liveRowCount: parsed.liveRowCount,
      count: parsed.services.length,
      services: parsed.services.map(service => ({
        ...service,
        observedAt: fetchedAt,
        liveFetchedAt: fetchedAt
      })),
      fetchedAt,
      timings: {
        browserMs: Date.now() - startedAt,
        rowWaitMs
      }
    };
  } finally {
    try {
      // Preserve cookies/session storage between requests. That is the warm-session
      // speed advantage of this service. Only halt stray network activity.
      await page.evaluate(() => window.stop?.());
    } catch (_) {}

    activeBrowserJobs = Math.max(0, activeBrowserJobs - 1);
    const uses = Number(pageUseCount.get(page) || 0) + 1;
    pageUseCount.set(page, uses);
    if (page.isClosed()) {
      void replaceDeadPage(page, "closed-after-scrape");
    } else if (uses >= PAGE_MAX_USES) {
      void replaceDeadPage(page, "scheduled-page-recycle");
    } else {
      releasePage(page);
    }

    if (browserRecycleRequested && activeBrowserJobs === 0 && waiters.length === 0) {
      void recycleBrowserForMemory("deferred-after-request");
    }
  }
}

async function fetchStopShared(stopId, limit, options = {}) {
  const liveOnly = options.liveOnly === true;
  const forceRefresh = options.forceRefresh === true;
  const allowStale = options.allowStale !== false && !forceRefresh;
  const cacheResult = options.cacheResult !== false && !forceRefresh;
  const key = cacheKey(stopId, limit, liveOnly);
  const inFlightKey = `${forceRefresh ? "fresh" : "normal"}|${key}`;

  if (!forceRefresh) {
    const fresh = getCache(key, false);
    if (fresh) {
      stats.cacheHits += 1;
      return {
        ...fresh.payload,
        cache: { hit: true, stale: false, ageMs: fresh.ageMs }
      };
    }
  } else {
    stats.strictFreshRequests += 1;
  }

  const existing = inFlight.get(inFlightKey);
  if (existing) {
    stats.coalesced += 1;
    const payload = await existing;
    return { ...payload, cache: { hit: false, coalesced: true, strictFresh: forceRefresh } };
  }

  const promise = (async () => {
    try {
      const payload = await scrapeStop(stopId, limit, {
        ...options,
        liveOnly,
        forceRefresh
      });
      if (cacheResult) setCache(key, payload);
      return payload;
    } catch (error) {
      stats.browserErrors += 1;
      if (allowStale) {
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
      }
      throw error;
    }
  })();

  inFlight.set(inFlightKey, promise);
  try {
    return await promise;
  } finally {
    if (inFlight.get(inFlightKey) === promise) inFlight.delete(inFlightKey);
  }
}


function updateGroupedStopHotnessV29(stopId, liveCount) {
  const key = String(stopId || "").trim();
  if (!key) return;
  const now = Date.now();
  const previous = groupedStopHotness.get(key);
  groupedStopHotness.set(key, {
    liveCount: Math.max(0, Number(liveCount) || 0),
    lastLiveAt: Number(liveCount) > 0 ? now : Number(previous?.lastLiveAt || 0),
    lastScannedAt: now
  });
  while (groupedStopHotness.size > 512) {
    const oldest = groupedStopHotness.keys().next().value;
    if (oldest == null) break;
    groupedStopHotness.delete(oldest);
  }
}

function hotFirstGroupedStopIdsV29(stopIds) {
  return stopIds
    .map((stopId, index) => ({ stopId, index, hot: groupedStopHotness.get(stopId) || null }))
    .sort((a, b) => {
      const liveDiff = Number(b.hot?.liveCount || 0) - Number(a.hot?.liveCount || 0);
      if (liveDiff) return liveDiff;
      const seenDiff = Number(b.hot?.lastLiveAt || 0) - Number(a.hot?.lastLiveAt || 0);
      if (seenDiff) return seenDiff;
      return a.index - b.index;
    })
    .map(row => row.stopId);
}

async function scanGroupedStopV29(stopId, perStop, options = {}) {
  const stopStartedAt = Date.now();
  try {
    const payload = await fetchStopShared(stopId, perStop, {
      rowWaitMs: options.liveOnly
        ? Math.min(LIVE_ROW_WAIT_TIMEOUT_MS, BATCH_ROW_WAIT_TIMEOUT_MS)
        : BATCH_ROW_WAIT_TIMEOUT_MS,
      forceRefresh: options.forceRefresh === true,
      allowStale: options.allowStale !== false,
      cacheResult: options.cacheResult !== false,
      liveOnly: options.liveOnly === true
    });
    const services = (Array.isArray(payload?.services) ? payload.services : [])
      .slice(0, perStop)
      .map(service => ({ ...service, stopId }));
    updateGroupedStopHotnessV29(stopId, services.filter(row => row?.live === true).length);
    return {
      stopId,
      ok: payload?.ok !== false,
      stopName: payload?.stopName || `Stop ${stopId}`,
      source: payload?.source || "136213-browser-v3.1-batch",
      count: services.length,
      services,
      cache: payload?.cache || null,
      ms: Date.now() - stopStartedAt
    };
  } catch (error) {
    updateGroupedStopHotnessV29(stopId, 0);
    return {
      stopId,
      ok: false,
      stopName: `Stop ${stopId}`,
      source: "136213-browser-v3.1-batch-error",
      count: 0,
      services: [],
      error: String(error.message || error),
      ms: Date.now() - stopStartedAt
    };
  }
}

async function buildStopsBatch(stopIds, perStop, options = {}) {
  const startedAt = Date.now();
  stats.batchRefreshes += 1;

  const orderedStopIds = options.hotFirst === false
    ? [...stopIds]
    : hotFirstGroupedStopIdsV29(stopIds);
  const completeOnly = options.completeOnly === true;

  if (completeOnly) {
    stats.completeSnapshotColdBuilds += 1;
    const rowsByStop = await mapLimit(
      orderedStopIds,
      Math.max(1, POOL_SIZE),
      stopId => scanGroupedStopV29(stopId, perStop, {
        ...options,
        backgroundComplete: false
      })
    );
    const services = rowsByStop.flatMap(row => row.services || []);
    const failedStopIds = rowsByStop
      .filter(row => row?.ok === false)
      .map(row => row.stopId);
    const complete = rowsByStop.length === orderedStopIds.length && failedStopIds.length === 0;

    return {
      ok: complete,
      source: complete
        ? "136213-browser-v3.1-complete-grouped-snapshot"
        : "136213-browser-v3.1-complete-grouped-snapshot-failed",
      grouped: true,
      completeOnly: true,
      groupedCompleteSnapshot: complete,
      groupedSnapshotComplete: complete,
      stopIds,
      scannedStopIds: rowsByStop.map(row => row.stopId),
      failedStopIds,
      pendingStopIds: [],
      groupedScanComplete: complete,
      livePending: !complete,
      authoritativeEmptyBoard: complete && services.length === 0,
      retryAfterMs: complete ? 0 : 1200,
      perStop,
      count: services.length,
      componentCount: stopIds.length,
      scannedComponentCount: rowsByStop.length,
      services,
      rowsByStop,
      fetchedAt: new Date().toISOString(),
      timings: { totalMs: Date.now() - startedAt }
    };
  }

  const targetRows = positiveInt(
    options.targetRows,
    Math.min(Math.max(perStop, 5), 18),
    1,
    100
  );
  const fastReturnMs = positiveInt(options.fastReturnMs, 3800, 800, 15000);
  const rowsByStop = [];
  const pending = [...orderedStopIds];

  while (pending.length) {
    const wave = pending.splice(0, Math.max(1, POOL_SIZE));
    const waveRows = await Promise.all(
      wave.map(stopId => scanGroupedStopV29(stopId, perStop, options))
    );
    rowsByStop.push(...waveRows);

    const liveRowsFound = rowsByStop.reduce(
      (sum, row) => sum + (Array.isArray(row?.services) ? row.services.length : 0),
      0
    );
    const elapsed = Date.now() - startedAt;
    const canFastReturn = pending.length > 0 && liveRowsFound > 0 && (
      liveRowsFound >= targetRows || elapsed >= fastReturnMs
    );

    if (canFastReturn) {
      stats.hotFirstBatchReturns += 1;
      if (options.backgroundComplete !== false) {
        const remaining = [...pending];
        void mapLimit(remaining, POOL_SIZE, stopId =>
          scanGroupedStopV29(stopId, perStop, { ...options, cacheResult: false })
        ).then(() => {
          stats.groupedBackgroundCompletions += 1;
        }).catch(error => {
          stats.batchErrors += 1;
          console.error("Grouped hot-first background completion failed:", error.message);
        });
      }
      break;
    }
  }

  const services = rowsByStop.flatMap(row => row.services || []);
  const scannedStopIds = rowsByStop.map(row => row.stopId);
  const pendingStopIds = orderedStopIds.filter(stopId => !scannedStopIds.includes(stopId));
  const complete = pendingStopIds.length === 0;

  return {
    ok: true,
    source: "136213-browser-v3.1-hot-first-batched-stops",
    grouped: true,
    completeOnly: false,
    groupedCompleteSnapshot: false,
    stopIds,
    scannedStopIds,
    pendingStopIds,
    groupedScanComplete: complete,
    livePending: !complete,
    authoritativeEmptyBoard: complete && services.length === 0,
    retryAfterMs: complete ? 0 : 900,
    perStop,
    targetRows,
    count: services.length,
    componentCount: stopIds.length,
    scannedComponentCount: rowsByStop.length,
    services,
    rowsByStop,
    fetchedAt: new Date().toISOString(),
    timings: { totalMs: Date.now() - startedAt }
  };
}

function beginBatchRefresh(key, stopIds, perStop, { markRequested = false, buildOptions = {} } = {}) {
  const existing = batchInFlight.get(key);
  if (existing) return existing;

  const promise = buildStopsBatch(stopIds, perStop, buildOptions)
    .then(payload => {
      const isCompleteSnapshot = buildOptions.completeOnly === true;
      if (!isCompleteSnapshot || payload?.groupedScanComplete === true) {
        setBatchCache(key, payload, { markRequested });
      }
      return payload;
    })
    .finally(() => {
      if (batchInFlight.get(key) === promise) batchInFlight.delete(key);
    });

  batchInFlight.set(key, promise);
  return promise;
}

async function fetchStopsBatchShared(stopIds, perStop, {
  forceRefresh = false,
  liveOnly = false,
  allowStale = true,
  hotFirst = true,
  targetRows = null,
  fastReturnMs = null,
  backgroundComplete = true,
  completeOnly = false
} = {}) {
  const key = batchCacheKey(stopIds, perStop, completeOnly);
  if (completeOnly) stats.completeSnapshotRequests += 1;

  if (forceRefresh) {
    stats.strictFreshBatchRequests += 1;
    const strictKey = `fresh|${key}|${liveOnly ? "live" : "mixed"}`;
    const existingStrict = batchInFlight.get(strictKey);
    if (existingStrict) return existingStrict;
    const strictPromise = buildStopsBatch(stopIds, perStop, {
      forceRefresh: true,
      liveOnly,
      allowStale: false,
      cacheResult: false,
      hotFirst,
      targetRows,
      fastReturnMs,
      backgroundComplete: completeOnly ? false : backgroundComplete,
      completeOnly
    }).then(payload => {
      if (payload?.groupedScanComplete === true) {
        setBatchCache(key, payload, { markRequested: true });
      }
      return payload;
    }).finally(() => {
      if (batchInFlight.get(strictKey) === strictPromise) batchInFlight.delete(strictKey);
    });
    batchInFlight.set(strictKey, strictPromise);
    return strictPromise;
  }

  const fresh = getBatchCache(key, false);
  if (fresh) {
    stats.batchCacheHits += 1;
    if (completeOnly) stats.completeSnapshotCacheHits += 1;
    return {
      ...fresh.payload,
      cache: { hit: true, stale: false, ageMs: fresh.ageMs }
    };
  }

  const stale = allowStale ? getBatchCache(key, true) : null;
  if (stale) {
    stats.batchStaleHits += 1;
    if (completeOnly) stats.completeSnapshotCacheHits += 1;
    void beginBatchRefresh(key, stopIds, perStop, {
      buildOptions: {
        liveOnly,
        allowStale: false,
        cacheResult: false,
        hotFirst,
        completeOnly,
        backgroundComplete: completeOnly ? false : backgroundComplete
      }
    }).catch(error => {
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

  const existing = batchInFlight.get(key);
  if (existing) return existing;

  try {
    return await beginBatchRefresh(key, stopIds, perStop, {
      markRequested: true,
      buildOptions: {
        liveOnly,
        allowStale,
        hotFirst,
        targetRows,
        fastReturnMs,
        backgroundComplete: completeOnly ? false : backgroundComplete,
        completeOnly
      }
    });
  } catch (error) {
    stats.batchErrors += 1;
    const rescue = getBatchCache(key, true);
    if (rescue) {
      return {
        ...rescue.payload,
        degraded: true,
        warning: String(error.message || error),
        cache: { hit: true, stale: true, ageMs: rescue.ageMs }
      };
    }
    throw error;
  }
}


async function refreshRecentlyRequestedGroupedStops() {
  if (groupedRefreshPromise) {
    stats.groupedRefreshSkippedOverlap += 1;
    return groupedRefreshPromise;
  }

  groupedRefreshPromise = (async () => {
    const now = Date.now();
    const candidates = [];

    for (const [key, entry] of batchCache.entries()) {
      if (!entry || !Array.isArray(entry.stopIds) || !entry.stopIds.length) continue;
      const lastRequestedAt = Number(entry.lastRequestedAt || 0);
      if (!lastRequestedAt || now - lastRequestedAt > BATCH_KEEP_WARM_MS) continue;
      if (batchInFlight.has(key)) continue;

      // Only refresh shortly before expiry. v2.6 refreshed every active group on
      // every timer tick because the fresh TTL was shorter than the timer.
      if (Number(entry.expiresAt || 0) - now > BATCH_REFRESH_LEAD_MS) continue;
      candidates.push({
        key,
        stopIds: entry.stopIds,
        perStop: entry.perStop,
        completeOnly: entry.completeOnly === true,
        expiresAt: Number(entry.expiresAt || 0)
      });
    }

    candidates.sort((a, b) => a.expiresAt - b.expiresAt);
    const selected = candidates.slice(0, BATCH_REFRESH_MAX_PER_TICK);
    stats.groupedRefreshCandidates += selected.length;

    await mapLimit(selected, 1, async candidate => {
      try {
        await beginBatchRefresh(candidate.key, candidate.stopIds, candidate.perStop, {
          markRequested: false,
          buildOptions: {
            completeOnly: candidate.completeOnly,
            backgroundComplete: candidate.completeOnly ? false : true,
            cacheResult: false,
            allowStale: false
          }
        });
      } catch (error) {
        stats.batchErrors += 1;
        console.error("Active grouped-stop refresh failed:", error.message);
      }
    });
  })();

  try {
    return await groupedRefreshPromise;
  } finally {
    groupedRefreshPromise = null;
  }
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
    const key = batchCacheKey(group, 10, true);
    await beginBatchRefresh(key, group, 10, { buildOptions: { completeOnly: true, backgroundComplete: false } }).catch(error => {
      stats.batchErrors += 1;
      console.error("Grouped-stop prewarm failed:", error.message);
    });
  }
}


function memorySnapshot() {
  const usage = process.memoryUsage();
  const toMB = value => Math.round((Number(value || 0) / 1024 / 1024) * 10) / 10;
  return {
    rssMB: toMB(usage.rss),
    heapUsedMB: toMB(usage.heapUsed),
    heapTotalMB: toMB(usage.heapTotal),
    externalMB: toMB(usage.external),
    arrayBuffersMB: toMB(usage.arrayBuffers),
    cacheEntries: cache.size,
    batchCacheEntries: batchCache.size,
    groupedStopHotnessEntries: groupedStopHotness.size,
    groupedSnapshotGenerationEntries: groupedSnapshotGeneration.size,
    serviceWarmRecent: serviceWarmRecent.size,
    managedPages: managedPages.size,
    activeBrowserJobs,
    at: new Date().toISOString()
  };
}

async function recycleBrowserForMemory(reason = "heap-soft-limit") {
  if (browserRecyclePromise || shuttingDown) return browserRecyclePromise;
  if (activeBrowserJobs > 0 || waiters.length > 0) {
    browserRecycleRequested = true;
    return null;
  }

  browserRecycleRequested = false;
  browserRecyclePromise = (async () => {
    stats.memoryRecycles += 1;
    console.warn(`Memory recycle started (${reason})`, memorySnapshot());

    // These caches are performance hints only; dropping them is safer than letting
    // a long-lived Playwright process reach V8's fatal heap limit.
    cache.clear();
    batchCache.clear();
    serviceWarmRecent.clear();
    pruneServiceWarmRecent();

    await closeBrowser();
    if (!shuttingDown) await ensureBrowser();
    if (typeof global.gc === "function") {
      try { global.gc(); } catch (_) {}
    }
    lastMemorySnapshot = memorySnapshot();
    console.warn(`Memory recycle completed (${reason})`, lastMemorySnapshot);
  })().catch(error => {
    console.error("Memory recycle failed:", error);
  }).finally(() => {
    browserRecyclePromise = null;
  });

  return browserRecyclePromise;
}

function monitorMemory() {
  pruneCache();
  pruneBatchCache();
  pruneServiceWarmRecent();
  lastMemorySnapshot = memorySnapshot();
  if (lastMemorySnapshot.heapUsedMB < HEAP_SOFT_LIMIT_MB) return;

  stats.memoryWarnings += 1;
  console.warn(
    `Heap soft limit reached: ${lastMemorySnapshot.heapUsedMB} MB >= ${HEAP_SOFT_LIMIT_MB} MB`,
    lastMemorySnapshot
  );
  void recycleBrowserForMemory("heap-soft-limit");
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

  const reason = String(req.body?.reason || req.query.reason || "worker-request-v2.7").slice(0, 100);
  const wait = String(req.query.wait || req.body?.wait || "0") === "1";
  const jobs = rows.map(row => enqueueServiceWarm(row, reason));
  res.set("Cache-Control", "no-store");

  if (!wait) {
    return res.status(202).json({
      ok: true,
      source: "transperth-browser-v2.7-service-packet-prewarm",
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
    source: "transperth-browser-v2.7-service-packet-prewarm",
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
    source: "transperth-browser-v2.7-service-packet-prewarm-status",
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
    service: "transperth-browser-v3.1",
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
    memory: lastMemorySnapshot || memorySnapshot(),
    stats,
    endpoints: [
      "/health",
      "/live-stop/26768?limit=5",
      "/live-stops?stops=27172,27180,27184&perStop=10&completeOnly=1",
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
      queuedRequests: waiters.length,
      memory: lastMemorySnapshot || memorySnapshot()
    });
  } catch (error) {
    res.status(503).json({ ok: false, error: String(error.message || error) });
  }
});



// v3.1: bounded per-component refresh endpoint. The Worker uses this only for
// missing/stale component stop snapshots, so a 29-stop group no longer needs a
// fresh 29-stop browser rebuild on every open. fetchStopShared already coalesces
// overlapping requests for the same stop ID and the page pool remains capped.
app.get("/live-stop-components", async (req, res) => {
  stats.requests += 1;
  stats.componentBatchRequests += 1;

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
  const forceRefresh = String(req.query.fresh || req.query.refresh || "") === "1";
  const liveOnly = String(req.query.liveOnly || req.query.live || "1") !== "0";
  const startedAt = Date.now();

  try {
    const rowsByStop = await mapLimit(
      stopIds,
      Math.max(1, POOL_SIZE),
      stopId => scanGroupedStopV29(stopId, perStop, {
        forceRefresh,
        liveOnly,
        allowStale: !forceRefresh,
        // A strict refresh should still replace the Browser's individual-stop
        // cache so subsequent overlapping groups can reuse the completed stop.
        cacheResult: true,
        hotFirst: false,
        backgroundComplete: false
      })
    );

    const failedStopIds = rowsByStop
      .filter(row => row?.ok === false)
      .map(row => row.stopId);
    const complete = rowsByStop.length === stopIds.length && failedStopIds.length === 0;
    const services = rowsByStop.flatMap(row => row?.services || []);

    stats.componentBatchCompleted += complete ? 1 : 0;
    stats.componentBatchFailedStops += failedStopIds.length;

    res.set("Cache-Control", "no-store");
    return res.status(complete ? 200 : 207).json({
      ok: complete,
      source: complete
        ? "136213-browser-v3.1-component-stop-snapshots"
        : "136213-browser-v3.1-component-stop-snapshots-partial",
      componentSnapshots: true,
      grouped: stopIds.length > 1,
      stopIds,
      perStop,
      componentCount: stopIds.length,
      scannedComponentCount: rowsByStop.length,
      failedStopIds,
      groupedScanComplete: complete,
      authoritativeEmptyBoard: complete && services.length === 0,
      rowsByStop,
      services,
      count: services.length,
      fetchedAt: new Date().toISOString(),
      timings: { requestTotalMs: Date.now() - startedAt }
    });
  } catch (error) {
    stats.batchErrors += 1;
    return res.status(504).json({
      ok: false,
      source: "136213-browser-v3.1-component-stop-snapshots-error",
      componentSnapshots: true,
      stopIds,
      perStop,
      error: String(error.message || error),
      fetchedAt: new Date().toISOString(),
      timings: { requestTotalMs: Date.now() - startedAt }
    });
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
  const forceRefresh = String(req.query.fresh || req.query.refresh || "") === "1";
  const liveOnly = String(req.query.liveOnly || req.query.live || "") === "1";
  const hotFirst = String(req.query.hotFirst || "1") !== "0";
  const targetRows = positiveInt(req.query.targetRows, Math.min(Math.max(perStop, 5), 18), 1, 100);
  const fastReturnMs = positiveInt(req.query.fastReturnMs, 3800, 800, 15000);
  const backgroundComplete = String(req.query.backgroundComplete || "1") !== "0";
  const completeOnly = String(req.query.completeOnly || req.query.completeSnapshot || req.query.snapshot || "") === "1";
  const startedAt = Date.now();

  try {
    const payload = await fetchStopsBatchShared(stopIds, perStop, {
      forceRefresh,
      liveOnly,
      allowStale: !forceRefresh,
      hotFirst,
      targetRows,
      fastReturnMs,
      backgroundComplete,
      completeOnly
    });
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
      source: "136213-browser-v3.1-batched-stops",
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
  const forceRefresh = String(req.query.fresh || req.query.refresh || "") === "1";
  const liveOnly = String(req.query.liveOnly || req.query.live || "") === "1";
  const allowStale = !forceRefresh && String(req.query.allowStale || "1") !== "0";
  const startedAt = Date.now();

  try {
    const payload = await fetchStopShared(stopId, limit, {
      forceRefresh,
      liveOnly,
      allowStale,
      cacheResult: !forceRefresh,
      rowWaitMs: liveOnly ? LIVE_ROW_WAIT_TIMEOUT_MS : undefined
    });
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
    console.log(`Transperth browser v3.1 listening on port ${PORT}; pool=${POOL_SIZE}`);
    console.log(`Playwright Chromium: ${chromium.executablePath()}`);
    void prewarmKnownGroupedStops().then(() => {
      console.log("Known grouped-stop caches prewarmed.");
    });

    const groupedRefreshTimer = setInterval(() => {
      void refreshRecentlyRequestedGroupedStops();
    }, BATCH_REFRESH_INTERVAL_MS);
    groupedRefreshTimer.unref?.();

    const memoryTimer = setInterval(() => {
      monitorMemory();
    }, MEMORY_CHECK_INTERVAL_MS);
    memoryTimer.unref?.();
    monitorMemory();
  } catch (error) {
    console.error("Browser startup failed:", error);
    process.exit(1);
  }
});
