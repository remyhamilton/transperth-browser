// v2253 SJP capture batch-read endpoint for single/group Hubway bus boards.
"use strict";

// Keep the Playwright browser inside the deployed project rather than Render's
// temporary build cache. This must be set before importing Playwright.
if (!process.env.PLAYWRIGHT_BROWSERS_PATH) {
  process.env.PLAYWRIGHT_BROWSERS_PATH = "0";
}

const fs = require("fs");
const crypto = require("crypto");
const https = require("https");
const express = require("express");
const { chromium } = require("playwright");

const app = express();
app.disable("x-powered-by");
app.use(express.json({ limit: "512kb" })); // v2252: SJP capture responses can exceed 64 KB

const PORT = positiveInt(process.env.PORT, 3000, 1, 65535);
const BROWSER_TOKEN = String(process.env.BROWSER_TOKEN || "").trim();
const POOL_SIZE = positiveInt(process.env.BROWSER_POOL_SIZE, 4, 1, 8);
const NAVIGATION_TIMEOUT_MS = positiveInt(process.env.NAVIGATION_TIMEOUT_MS, 12000, 3000, 30000);
const ROW_WAIT_TIMEOUT_MS = positiveInt(process.env.ROW_WAIT_TIMEOUT_MS, 6500, 1000, 15000);
const LIVE_ROW_WAIT_TIMEOUT_MS = positiveInt(process.env.LIVE_ROW_WAIT_TIMEOUT_MS, 1800, 250, 6000);
const LIVE_SETTLE_MS = positiveInt(process.env.LIVE_SETTLE_MS, 80, 0, 500);
const BLOCK_STYLESHEETS = String(process.env.BLOCK_STYLESHEETS || "1") !== "0";
const QUEUE_TIMEOUT_MS = positiveInt(process.env.QUEUE_TIMEOUT_MS, 7000, 500, 20000);
const FRESH_CACHE_MS = positiveInt(process.env.FRESH_CACHE_MS, 120000, 0, 300000);
const STALE_CACHE_MS = positiveInt(process.env.STALE_CACHE_MS, 900000, 0, 3600000);
const MAX_CACHE_ENTRIES = positiveInt(process.env.MAX_CACHE_ENTRIES, 250, 10, 2000);
const BATCH_MAX_STOPS = positiveInt(process.env.BATCH_MAX_STOPS, 60, 2, 120);
const BATCH_ROW_WAIT_TIMEOUT_MS = positiveInt(process.env.BATCH_ROW_WAIT_TIMEOUT_MS, 900, 250, 4000);
const ATOMIC_COMPONENT_HTTP_CONCURRENCY = positiveInt(process.env.ATOMIC_COMPONENT_HTTP_CONCURRENCY, 3, 1, 12);
const ATOMIC_COMPONENT_HTTP_TIMEOUT_MS = positiveInt(process.env.ATOMIC_COMPONENT_HTTP_TIMEOUT_MS, 6000, 1000, 12000);
const ATOMIC_COMPONENT_DIRECT_RETRY_ROUNDS = positiveInt(process.env.ATOMIC_COMPONENT_DIRECT_RETRY_ROUNDS, 0, 0, 2);
const ATOMIC_COMPONENT_DIRECT_RETRY_DELAY_MS = positiveInt(process.env.ATOMIC_COMPONENT_DIRECT_RETRY_DELAY_MS, 60, 0, 1000);
const ATOMIC_COMPONENT_PAGE_FALLBACK_CONCURRENCY = positiveInt(process.env.ATOMIC_COMPONENT_PAGE_FALLBACK_CONCURRENCY, 1, 1, 4);
const ATOMIC_COMPONENT_PARSE_CHUNK = positiveInt(process.env.ATOMIC_COMPONENT_PARSE_CHUNK, 60, 2, 120);
const ATOMIC_COMPONENT_MAX_HTML_BYTES = positiveInt(process.env.ATOMIC_COMPONENT_MAX_HTML_BYTES, 1500000, 100000, 4000000);
const ATOMIC_NATIVE_HTTP_TIMEOUT_MS_V35 = positiveInt(process.env.ATOMIC_NATIVE_HTTP_TIMEOUT_MS_V35, 4200, 1200, 8000);
const ATOMIC_CONTEXT_RETRY_TIMEOUT_MS_V35 = positiveInt(process.env.ATOMIC_CONTEXT_RETRY_TIMEOUT_MS_V35, 2600, 800, 6000);
const ATOMIC_ACCEPT_VERIFIED_EMPTY_V35 = String(process.env.ATOMIC_ACCEPT_VERIFIED_EMPTY_V35 || "1") !== "0";
const ATOMIC_FOREGROUND_PAGE_FALLBACK_V35 = String(process.env.ATOMIC_FOREGROUND_PAGE_FALLBACK_V35 || "0") === "1";
const ATOMIC_NATIVE_AGENT_V35 = new https.Agent({
  keepAlive: true,
  maxSockets: 3,
  maxFreeSockets: 3,
  scheduling: "lifo",
  timeout: 10000
 });
const BATCH_BACKGROUND_REFRESH_ENABLED = String(process.env.BATCH_BACKGROUND_REFRESH_ENABLED || "0") === "1";
const PREWARM_KNOWN_GROUPS = String(process.env.PREWARM_KNOWN_GROUPS || "0") === "1";
const BATCH_FRESH_CACHE_MS = positiveInt(process.env.BATCH_FRESH_CACHE_MS, 120000, 1000, 300000);
const BATCH_STALE_CACHE_MS = positiveInt(process.env.BATCH_STALE_CACHE_MS, 1800000, 5000, 3600000);
const BATCH_CACHE_MAX_ENTRIES = positiveInt(process.env.BATCH_CACHE_MAX_ENTRIES, 80, 4, 250);
const BATCH_KEEP_WARM_MS = positiveInt(process.env.BATCH_KEEP_WARM_MS, 300000, 60000, 3600000);
const BATCH_REFRESH_INTERVAL_MS = positiveInt(process.env.BATCH_REFRESH_INTERVAL_MS, 120000, 10000, 600000);
const BATCH_REFRESH_LEAD_MS = positiveInt(process.env.BATCH_REFRESH_LEAD_MS, 15000, 1000, 120000);
const BATCH_REFRESH_MAX_PER_TICK = positiveInt(process.env.BATCH_REFRESH_MAX_PER_TICK, 1, 1, 4);
const PAGE_MAX_USES = positiveInt(process.env.PAGE_MAX_USES, 60, 10, 500);
const MEMORY_CHECK_INTERVAL_MS = positiveInt(process.env.MEMORY_CHECK_INTERVAL_MS, 60000, 15000, 300000);
const HEAP_SOFT_LIMIT_MB = positiveInt(process.env.HEAP_SOFT_LIMIT_MB, 650, 128, 4096);
const SERVICE_WARM_RECENT_MAX_ENTRIES = positiveInt(process.env.SERVICE_WARM_RECENT_MAX_ENTRIES, 256, 16, 2000);
const WORKER_BASE_URL = String(
  process.env.WORKER_BASE_URL || "https://twilight-wildflower-4e89.remy-hamilton.workers.dev"
).trim().replace(/\/$/, "");
const SERVICE_WARM_MAX_FLEETS = positiveInt(process.env.SERVICE_WARM_MAX_FLEETS, 12, 1, 24);
const SERVICE_WARM_CONCURRENCY = positiveInt(process.env.SERVICE_WARM_CONCURRENCY, 2, 1, 4);
const SERVICE_WARM_TIMEOUT_MS = positiveInt(process.env.SERVICE_WARM_TIMEOUT_MS, 30000, 5000, 45000);
const SERVICE_WARM_RECENT_MS = positiveInt(process.env.SERVICE_WARM_RECENT_MS, 45000, 5000, 300000);
const SERVICE_WARM_FAILED_RECENT_MS = positiveInt(process.env.SERVICE_WARM_FAILED_RECENT_MS, 2000, 500, 10000);
const SERVICE_WARM_QUEUE_MAX = positiveInt(process.env.SERVICE_WARM_QUEUE_MAX, 80, 8, 300);

// Transperth SJP JSON test lane. Keep the upstream authorization material in
// Render environment variables; never commit it to this file. The captured
// PhoneApp Authorization value may be short-lived, so this lane intentionally
// fails closed when it is not configured.
const SJP_BASE_URL = String(process.env.SJP_BASE_URL || "https://realtime.transperth.info").trim().replace(/\/$/, "");
const SJP_AUTHORIZATION = String(process.env.SJP_AUTHORIZATION || "").trim();
const SJP_AV = String(process.env.SJP_AV || "2.12.3").trim();
const SJP_RV = String(process.env.SJP_RV || "Pi2123.1x").trim();
const SJP_USER_AGENT = String(process.env.SJP_USER_AGENT || "Transperth/132426 CFNetwork/3860.700.1 Darwin/25.6.0").trim();
const SJP_TIMEOUT_MS = positiveInt(process.env.SJP_TIMEOUT_MS, 4500, 1000, 15000);
const SJP_GROUP_CONCURRENCY = positiveInt(process.env.SJP_GROUP_CONCURRENCY, 3, 1, 8);
const SJP_CACHE_MS = positiveInt(process.env.SJP_CACHE_MS, 15000, 0, 120000);
// v2252 capture bridge: mitmproxy forwards only successful SJP response JSON.
// It never forwards the PhoneApp Authorization header, nonce, or token.
const SJP_CAPTURE_TOKEN = String(process.env.SJP_CAPTURE_TOKEN || "").trim();
const SJP_CAPTURE_TTL_MS_V2252 = positiveInt(process.env.SJP_CAPTURE_TTL_MS, 120000, 15000, 600000);
const SJP_CAPTURE_MAX_STOPS_V2252 = positiveInt(process.env.SJP_CAPTURE_MAX_STOPS, 160, 8, 1000);
const SJP_CAPTURE_MAX_TRIPS_V2252 = positiveInt(process.env.SJP_CAPTURE_MAX_TRIPS, 320, 16, 2000);

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
const atomicComponentBatchInFlightV32 = new Map();
const groupedStopHotness = new Map();
const groupedSnapshotGeneration = new Map();
const serviceWarmPending = [];
const serviceWarmInFlight = new Map();
const serviceWarmRecent = new Map();
const sjpStopCache = new Map();
const sjpStopInFlight = new Map();
const sjpCapturedStopsV2252 = new Map();
const sjpCapturedTripsV2252 = new Map();
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
  componentBatchFailedStops: 0,
  atomicComponentBatchRequestsV32: 0,
  atomicComponentBatchCoalescedV32: 0,
  atomicComponentHTTPFetchesV32: 0,
  atomicComponentHTTPSuccessesV32: 0,
  atomicComponentHTTPFallbacksV32: 0,
  atomicComponentHTTPRetryFetchesV33: 0,
  atomicComponentHTTPRetryRecoveriesV33: 0,
  atomicComponentHTTPErrorsV32: 0,
  atomicComponentBatchCompletedV32: 0,
  atomicComponentAggregateCacheHitsV34: 0,
  atomicComponentFreshCacheHitsV34: 0,
  atomicComponentStaleRescuesV34: 0,
  atomicComponentOneWaveBuildsV34: 0,
  atomicNativeHTTPFetchesV35: 0,
  atomicNativeHTTPSuccessesV35: 0,
  atomicNativeHTTPErrorsV35: 0,
  atomicContextRetryFetchesV35: 0,
  atomicFastParserDocumentsV35: 0,
  atomicVerifiedEmptyComponentsV35: 0,
  atomicPageFallbacksSkippedV35: 0,
  sjpStopRequestsV1: 0,
  sjpGroupRequestsV1: 0,
  sjpUpstreamRequestsV1: 0,
  sjpUpstreamSuccessesV1: 0,
  sjpUpstreamErrorsV1: 0,
  sjpCacheHitsV1: 0,
  sjpCoalescedV1: 0,
  sjpCaptureIngestRequestsV2252: 0,
  sjpCaptureStopPublishesV2252: 0,
  sjpCaptureTripPublishesV2252: 0,
  sjpCaptureReadHitsV2252: 0,
  sjpCaptureExpiredV2252: 0,
  sjpCaptureRejectedV2252: 0
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

function sjpAuthConfiguredV1() {
  return /^Custom\s+/i.test(SJP_AUTHORIZATION);
}

function sjpCaptureAuthOkV2252(req) {
  if (!SJP_CAPTURE_TOKEN) return false;
  const supplied = String(req.headers.authorization || "").replace(/^Bearer\s+/i, "");
  const expectedBuffer = Buffer.from(SJP_CAPTURE_TOKEN);
  const suppliedBuffer = Buffer.from(supplied);
  return expectedBuffer.length === suppliedBuffer.length &&
    crypto.timingSafeEqual(expectedBuffer, suppliedBuffer);
}

function cleanSJPRestrictedIdV2252(value) {
  return String(value || "").replace(/^PerthRestricted:/i, "").trim();
}

function pruneSJPCaptureV2252(now = Date.now()) {
  for (const [key, entry] of sjpCapturedStopsV2252) {
    if (!entry || Number(entry.expiresAt || 0) <= now) {
      sjpCapturedStopsV2252.delete(key);
      stats.sjpCaptureExpiredV2252 += 1;
    }
  }
  for (const [key, entry] of sjpCapturedTripsV2252) {
    if (!entry || Number(entry.expiresAt || 0) <= now) {
      sjpCapturedTripsV2252.delete(key);
      stats.sjpCaptureExpiredV2252 += 1;
    }
  }
  while (sjpCapturedStopsV2252.size > SJP_CAPTURE_MAX_STOPS_V2252) {
    const key = sjpCapturedStopsV2252.keys().next().value;
    if (key == null) break;
    sjpCapturedStopsV2252.delete(key);
  }
  while (sjpCapturedTripsV2252.size > SJP_CAPTURE_MAX_TRIPS_V2252) {
    const key = sjpCapturedTripsV2252.keys().next().value;
    if (key == null) break;
    sjpCapturedTripsV2252.delete(key);
  }
}

function perthMinuteStampV1(date = new Date()) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Australia/Perth",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23"
  }).formatToParts(date);
  const value = type => parts.find(part => part.type === type)?.value || "";
  return `${value("year")}-${value("month")}-${value("day")}T${value("hour")}:${value("minute")}`;
}

function validSJPRequestTimeV1(value) {
  const clean = String(value || "").trim();
  return /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(clean) ? clean : null;
}

function parseSJPPositionV1(value) {
  const clean = String(value || "").trim();
  if (!clean) return { latitude: null, longitude: null };
  const pieces = clean.split(/[\s,]+/).map(Number).filter(Number.isFinite);
  if (pieces.length < 2) return { latitude: null, longitude: null };
  const [latitude, longitude] = pieces;
  if (latitude < -90 || latitude > 90 || longitude < -180 || longitude > 180) {
    return { latitude: null, longitude: null };
  }
  return { latitude, longitude };
}

function sjpClockTextV1(value) {
  const clean = String(value || "").trim();
  if (!clean) return null;
  const iso = clean.match(/T(\d{2}:\d{2})(?::(\d{2}))?/);
  if (iso) return iso[2] ? `${iso[1]}:${iso[2]}` : iso[1];
  const duration = clean.match(/(?:^|\.)(\d{2}):(\d{2})(?::(\d{2}))?$/);
  if (duration) return duration[3]
    ? `${duration[1]}:${duration[2]}:${duration[3]}`
    : `${duration[1]}:${duration[2]}`;
  return clean;
}

function sjpServiceSortMsV1(service) {
  const candidates = [service?.scheduledDepartureISO, service?.scheduledArrivalISO];
  for (const candidate of candidates) {
    const ms = Date.parse(String(candidate || ""));
    if (Number.isFinite(ms)) return ms;
  }
  return Number.MAX_SAFE_INTEGER;
}

function normalizeSJPTripV1(trip, requestedStopId) {
  const summary = trip?.Summary || {};
  const summaryRealtime = summary?.RealTimeInfo || null;
  const stopRealtime = trip?.RealTimeInfo || null;
  const position = parseSJPPositionV1(summaryRealtime?.CurrentPosition);
  const tripId = String(summary?.TripSourceId || summary?.TripUid || "")
    .replace(/^PerthRestricted:/i, "")
    .trim() || null;
  const tripUid = String(summary?.TripUid || (tripId ? `PerthRestricted:${tripId}` : "")).trim() || null;
  const fleetNumber = String(summaryRealtime?.FleetNumber || "").trim() || null;
  const vehicleId = String(summaryRealtime?.VehicleId || "").trim() || null;
  const scheduledDepartureISO = String(trip?.DepartTime || "").trim() || null;
  const scheduledArrivalISO = String(trip?.ArriveTime || "").trim() || null;
  const estimatedDepartureRaw = String(stopRealtime?.EstimatedDepartureTime || "").trim() || null;
  const estimatedArrivalRaw = String(stopRealtime?.EstimatedArrivalTime || "").trim() || null;
  const actualDepartureRaw = String(stopRealtime?.ActualDepartureTime || "").trim() || null;
  const actualArrivalRaw = String(stopRealtime?.ActualArrivalTime || "").trim() || null;
  const hasLiveVehicle = Boolean(summaryRealtime && (fleetNumber || vehicleId || position.latitude != null));
  const hasRealtimeStop = Boolean(stopRealtime && Number(stopRealtime?.RealTimeTripStatus || 0) > 0);
  const live = hasLiveVehicle || hasRealtimeStop;
  const estimatedDepartureTime = sjpClockTextV1(estimatedDepartureRaw);
  const estimatedArrivalTime = sjpClockTextV1(estimatedArrivalRaw);
  const actualDepartureTime = sjpClockTextV1(actualDepartureRaw);
  const actualArrivalTime = sjpClockTextV1(actualArrivalRaw);
  const scheduledTime = sjpClockTextV1(scheduledDepartureISO || scheduledArrivalISO);
  const liveTime = actualDepartureTime || estimatedDepartureTime || actualArrivalTime || estimatedArrivalTime || null;

  return {
    route: String(summary?.RouteCode || summary?.RouteName || "").trim() || null,
    routeName: String(summary?.RouteName || "").trim() || null,
    destination: String(summary?.Headsign || trip?.Destination?.Name || "").trim() || null,
    headsign: String(summary?.Headsign || "").trim() || null,
    mode: String(summary?.Mode || "").trim() || null,
    tripId,
    tripUid,
    routeUid: String(summary?.RouteUid || "").trim() || null,
    routeSourceId: String(summary?.RouteSourceId || "").trim() || null,
    tripStartTime: String(summary?.TripStartTime || "").trim() || null,
    direction: String(summary?.Direction || "").trim() || null,
    stopId: String(requestedStopId || "").trim() || null,
    sequenceNumber: String(trip?.SequenceNumber || "").trim() || null,
    scheduledArrivalISO,
    scheduledDepartureISO,
    scheduledTime,
    estimatedArrivalTime,
    estimatedDepartureTime,
    actualArrivalTime,
    actualDepartureTime,
    liveTime,
    time: liveTime || scheduledTime,
    statusText: live ? "Live" : "Scheduled",
    status: live ? "Live" : "Scheduled",
    live,
    scheduled: !live,
    realTimeTripStatus: Number.isFinite(Number(stopRealtime?.RealTimeTripStatus))
      ? Number(stopRealtime.RealTimeTripStatus)
      : null,
    fleetNumber,
    fleet: fleetNumber,
    vehicleId,
    latitude: position.latitude,
    longitude: position.longitude,
    heading: Number.isFinite(Number(summaryRealtime?.CurrentBearing))
      ? Number(summaryRealtime.CurrentBearing)
      : null,
    liveUpdatedAt: String(summaryRealtime?.LastUpdated || "").trim() || null,
    originStopId: String(trip?.Origin?.Code || "").trim() || null,
    originName: String(trip?.Origin?.Name || "").trim() || null,
    destinationStopId: String(trip?.Destination?.Code || "").trim() || null,
    destinationName: String(trip?.Destination?.Name || "").trim() || null,
    provider: "realtime.transperth.info",
    source: "transperth-sjp-stop-v1"
  };
}


function buildSJPCapturedStopPayloadV2252(upstream, receivedAtMs = Date.now()) {
  if (!upstream || typeof upstream !== "object") return null;
  const requestedStop = upstream?.RequestedStop || null;
  const request = upstream?.Request || null;
  const stopId = cleanSJPRestrictedIdV2252(
    requestedStop?.Code ||
    requestedStop?.StopUid ||
    request?.StopUid
  );
  if (!/^\d{1,8}$/.test(stopId)) return null;
  const trips = Array.isArray(upstream?.Trips) ? upstream.Trips : [];
  const services = trips
    .map(trip => normalizeSJPTripV1(trip, stopId))
    .filter(service => service.route && service.destination && service.tripId)
    .sort((a, b) => sjpServiceSortMsV1(a) - sjpServiceSortMsV1(b));
  const liveCount = services.filter(service => service.live === true).length;
  return {
    ok: true,
    source: "transperth-sjp-mitm-capture-v2252",
    captureBridgeV2252: true,
    stopId,
    stopUid: `PerthRestricted:${stopId}`,
    requestedTime: String(request?.Time || "").trim() || null,
    upstreamTimeBandMinutes: Number(request?.TimeBand || 0) || 120,
    requestedStop,
    count: services.length,
    liveCount,
    scheduledCount: Math.max(0, services.length - liveCount),
    services,
    notes: Array.isArray(upstream?.Notes) ? upstream.Notes : [],
    capturedAt: new Date(receivedAtMs).toISOString(),
    capturedAtMs: receivedAtMs,
    fetchedAt: new Date(receivedAtMs).toISOString()
  };
}

function buildSJPCapturedTripPayloadV2252(upstream, receivedAtMs = Date.now()) {
  if (!upstream || typeof upstream !== "object") return null;
  const summary = upstream?.Summary || {};
  const realtime = summary?.RealTimeInfo || null;
  const tripId = cleanSJPRestrictedIdV2252(
    summary?.TripSourceId ||
    summary?.TripUid ||
    upstream?.Request?.TripUid
  );
  if (!/^\d{1,12}$/.test(tripId)) return null;
  const fleetNumber = String(
    realtime?.FleetNumber ||
    upstream?.Request?.FleetNumber ||
    ""
  ).trim() || null;
  const vehicleId = String(realtime?.VehicleId || "").trim() || null;
  const position = parseSJPPositionV1(realtime?.CurrentPosition);
  const tripStops = (Array.isArray(upstream?.TripStops) ? upstream.TripStops : []).map(stop => {
    const stopRealtime = stop?.RealTimeInfo || null;
    const stopId = cleanSJPRestrictedIdV2252(stop?.TransitStop?.Code || stop?.TransitStop?.StopUid);
    const estimatedDepartureTime = sjpClockTextV1(stopRealtime?.EstimatedDepartureTime);
    const estimatedArrivalTime = sjpClockTextV1(stopRealtime?.EstimatedArrivalTime);
    const actualDepartureTime = sjpClockTextV1(stopRealtime?.ActualDepartureTime);
    const actualArrivalTime = sjpClockTextV1(stopRealtime?.ActualArrivalTime);
    return {
      stopId: stopId || null,
      stopUid: String(stop?.TransitStop?.StopUid || "").trim() || null,
      stopName: String(stop?.TransitStop?.Description || "").trim() || null,
      sequenceNumber: String(stop?.SequenceNumber || "").trim() || null,
      scheduledDepartureISO: String(stop?.DepartureTime || "").trim() || null,
      scheduledArrivalISO: String(stop?.ArrivalTime || "").trim() || null,
      estimatedDepartureTime,
      estimatedArrivalTime,
      actualDepartureTime,
      actualArrivalTime,
      liveTime: actualDepartureTime || estimatedDepartureTime || actualArrivalTime || estimatedArrivalTime || null,
      realTimeTripStatus: Number.isFinite(Number(stopRealtime?.RealTimeTripStatus))
        ? Number(stopRealtime.RealTimeTripStatus)
        : null
    };
  });
  return {
    ok: true,
    source: "transperth-sjp-mitm-trip-capture-v2252",
    captureBridgeV2252: true,
    tripId,
    tripUid: String(summary?.TripUid || `PerthRestricted:${tripId}`).trim(),
    route: String(summary?.RouteCode || summary?.RouteName || "").trim() || null,
    destination: String(summary?.Headsign || "").trim() || null,
    fleetNumber,
    fleet: fleetNumber,
    vehicleId,
    latitude: position.latitude,
    longitude: position.longitude,
    heading: Number.isFinite(Number(realtime?.CurrentBearing)) ? Number(realtime.CurrentBearing) : null,
    liveUpdatedAt: String(realtime?.LastUpdated || "").trim() || null,
    live: Boolean(realtime && (fleetNumber || vehicleId || position.latitude != null)),
    tripStops,
    capturedAt: new Date(receivedAtMs).toISOString(),
    capturedAtMs: receivedAtMs,
    fetchedAt: new Date(receivedAtMs).toISOString()
  };
}

function capturedSJPStopV2252(stopId, maxAgeMs = SJP_CAPTURE_TTL_MS_V2252) {
  pruneSJPCaptureV2252();
  const key = String(stopId || "").trim();
  const entry = sjpCapturedStopsV2252.get(key);
  if (!entry) return null;
  const ageMs = Math.max(0, Date.now() - Number(entry.createdAt || 0));
  if (ageMs > Math.min(SJP_CAPTURE_TTL_MS_V2252, Math.max(1000, Number(maxAgeMs) || SJP_CAPTURE_TTL_MS_V2252))) {
    return null;
  }
  stats.sjpCaptureReadHitsV2252 += 1;
  return {
    ...structuredCloneSafe(entry.payload),
    captureAgeMs: ageMs,
    cache: { hit: true, kind: "mitm-capture-v2252", ageMs }
  };
}

function capturedSJPTripV2252(tripId, maxAgeMs = SJP_CAPTURE_TTL_MS_V2252) {
  pruneSJPCaptureV2252();
  const key = String(tripId || "").trim();
  const entry = sjpCapturedTripsV2252.get(key);
  if (!entry) return null;
  const ageMs = Math.max(0, Date.now() - Number(entry.createdAt || 0));
  if (ageMs > Math.min(SJP_CAPTURE_TTL_MS_V2252, Math.max(1000, Number(maxAgeMs) || SJP_CAPTURE_TTL_MS_V2252))) {
    return null;
  }
  stats.sjpCaptureReadHitsV2252 += 1;
  return {
    ...structuredCloneSafe(entry.payload),
    captureAgeMs: ageMs,
    cache: { hit: true, kind: "mitm-capture-v2252", ageMs }
  };
}

function pruneSJPStopCacheV1(now = Date.now()) {
  for (const [key, entry] of sjpStopCache.entries()) {
    if (!entry || Number(entry.expiresAt || 0) <= now) sjpStopCache.delete(key);
  }
  while (sjpStopCache.size > MAX_CACHE_ENTRIES) {
    const oldest = sjpStopCache.keys().next().value;
    if (oldest == null) break;
    sjpStopCache.delete(oldest);
  }
}

async function fetchSJPStopV1(stopId, options = {}) {
  const cleanStopId = String(stopId || "").trim();
  if (!/^\d{1,8}$/.test(cleanStopId)) throw new Error("Invalid stop number");
  // v2252: prefer a fresh successful response observed from the official app.
  // This uses response data only and never replays the PhoneApp Authorization.
  const capturedV2252 = options.freshDirectV1 === true
    ? null
    : capturedSJPStopV2252(cleanStopId, options.captureMaxAgeMsV2252);
  if (capturedV2252) return capturedV2252;
  if (!sjpAuthConfiguredV1()) {
    const error = new Error("SJP_AUTHORIZATION is not configured on Render");
    error.code = "SJP_AUTH_NOT_CONFIGURED";
    throw error;
  }
  const requestTime = validSJPRequestTimeV1(options.time) || perthMinuteStampV1();
  const key = `${cleanStopId}|${requestTime}`;
  const now = Date.now();
  pruneSJPStopCacheV1(now);
  if (options.fresh !== true) {
    const cached = sjpStopCache.get(key);
    if (cached && cached.expiresAt > now) {
      stats.sjpCacheHitsV1 += 1;
      return { ...structuredCloneSafe(cached.payload), cache: { hit: true, ageMs: now - cached.createdAt } };
    }
  }
  if (sjpStopInFlight.has(key)) {
    stats.sjpCoalescedV1 += 1;
    return sjpStopInFlight.get(key);
  }

  const promise = (async () => {
    const startedAt = Date.now();
    stats.sjpUpstreamRequestsV1 += 1;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(new Error("sjp-stop-timeout")), SJP_TIMEOUT_MS);
    try {
      const response = await fetch(`${SJP_BASE_URL}/SJP/Stop`, {
        method: "POST",
        signal: controller.signal,
        headers: {
          "Accept": "*/*",
          "Authorization": SJP_AUTHORIZATION,
          "Accept-Language": "en-AU,en;q=0.9",
          "Content-Type": "application/json",
          "User-Agent": SJP_USER_AGENT,
          "av": SJP_AV,
          "rv": SJP_RV
        },
        body: JSON.stringify({
          StopUid: `PerthRestricted:${cleanStopId}`,
          Time: requestTime,
          TransportModes: "Bus;School Bus;Rail;Ferry",
          ReturnNotes: true,
          IsRealTimeChecked: true
        })
      });
      const text = await response.text();
      let upstream = null;
      try { upstream = JSON.parse(text); } catch {}
      if (!response.ok || !upstream || Number(upstream?.Status?.Severity || 0) > 1) {
        stats.sjpUpstreamErrorsV1 += 1;
        const message = upstream?.Status?.Message || upstream?.Status?.Description || text.slice(0, 240) || `HTTP ${response.status}`;
        const error = new Error(`SJP /Stop failed: ${message}`);
        error.status = response.status;
        throw error;
      }
      stats.sjpUpstreamSuccessesV1 += 1;
      const services = (Array.isArray(upstream?.Trips) ? upstream.Trips : [])
        .map(trip => normalizeSJPTripV1(trip, cleanStopId))
        .filter(service => service.route && service.destination && service.tripId)
        .sort((a, b) => sjpServiceSortMsV1(a) - sjpServiceSortMsV1(b));
      const liveCount = services.filter(service => service.live === true).length;
      const payload = {
        ok: true,
        source: "transperth-sjp-stop-v1",
        stopId: cleanStopId,
        stopUid: `PerthRestricted:${cleanStopId}`,
        requestedTime: requestTime,
        upstreamTimeBandMinutes: Number(upstream?.Request?.TimeBand || 0) || null,
        requestedStop: upstream?.RequestedStop || null,
        count: services.length,
        liveCount,
        scheduledCount: Math.max(0, services.length - liveCount),
        services,
        notes: Array.isArray(upstream?.Notes) ? upstream.Notes : [],
        fetchedAt: new Date().toISOString(),
        timings: { upstreamMs: Date.now() - startedAt },
        ...(options.raw === true ? { raw: upstream } : {})
      };
      if (SJP_CACHE_MS > 0 && options.raw !== true) {
        sjpStopCache.set(key, {
          payload: structuredCloneSafe(payload),
          createdAt: Date.now(),
          expiresAt: Date.now() + SJP_CACHE_MS
        });
        pruneSJPStopCacheV1();
      }
      return payload;
    } finally {
      clearTimeout(timer);
    }
  })().finally(() => {
    if (sjpStopInFlight.get(key) === promise) sjpStopInFlight.delete(key);
  });
  sjpStopInFlight.set(key, promise);
  return promise;
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
      source: "136213-browser-v3.3-fresh-live-board",
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
      source: payload?.source || "136213-browser-v3.3-batch",
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
      source: "136213-browser-v3.3-batch-error",
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
        ? "136213-browser-v3.3-complete-grouped-snapshot"
        : "136213-browser-v3.3-complete-grouped-snapshot-failed",
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
    source: "136213-browser-v3.3-hot-first-batched-stops",
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
    service: "transperth-browser-v3.8-sjp-board-v2253",
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
      "POST /capture/sjp (SJP_CAPTURE_TOKEN)",
      "/sjp/captured-stop/20506",
      "/sjp/captured-stops?stops=20506,20507",
      "/sjp/captured-trip/7120331",
      "/sjp/stop/21911",
      "/sjp/stop/26898",
      "/sjp/group?stops=21911,26898",
      "/live-stop/26768?limit=5",
      "/live-stops?stops=27172,27180,27184&perStop=10&completeOnly=1",
      "/live-stop-components-multiplex?stops=27431,27432,27433,27434,27435,27437,27438,27439,27440,27441,27442&perStop=30&liveOnly=0&allowPageFallback=0",
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





function decodeHTMLTextV35(value) {
  const named = {
    amp: "&", lt: "<", gt: ">", quot: '"', apos: "'", nbsp: " ",
    ndash: "ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œ", mdash: "ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â", hellip: "ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â¦"
  };
  return String(value || "").replace(/&(#x?[0-9a-f]+|[a-z]+);/gi, (_, token) => {
    const lower = String(token).toLowerCase();
    if (lower[0] === "#") {
      const hex = lower[1] === "x";
      const code = Number.parseInt(lower.slice(hex ? 2 : 1), hex ? 16 : 10);
      return Number.isFinite(code) ? String.fromCodePoint(code) : " ";
    }
    return Object.prototype.hasOwnProperty.call(named, lower) ? named[lower] : `&${token};`;
  });
}

function cleanHTMLTextV35(value) {
  return decodeHTMLTextV35(value).replace(/\s+/g, " ").trim();
}

function parseHTMLAttributesV35(raw) {
  const out = Object.create(null);
  const source = String(raw || "");
  const pattern = /([^\s=/>]+)(?:\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s"'=<>`]+)))?/g;
  let match;
  while ((match = pattern.exec(source))) {
    const key = String(match[1] || "").toLowerCase();
    if (!key) continue;
    out[key] = decodeHTMLTextV35(match[2] ?? match[3] ?? match[4] ?? "");
  }
  return out;
}

function absoluteURLV35(href, baseURL) {
  if (!href) return null;
  try { return new URL(href, baseURL).toString(); } catch { return null; }
}

function queryValueV35(href, names, baseURL) {
  if (!href) return null;
  try {
    const u = new URL(href, baseURL);
    for (const name of names) {
      const value = u.searchParams.get(name);
      if (value) return cleanHTMLTextV35(value);
    }
  } catch {}
  return null;
}

function parseStopHTMLFastV35(entry, limit, liveOnly) {
  const stopId = String(entry?.stopId || "");
  const html = String(entry?.html || "");
  const baseURL = `https://136213.mobi/RealTime/RealTimeStopResults.aspx?SN=${encodeURIComponent(stopId)}`;
  const stack = [];
  const completedRows = [];
  const headingBuffers = [];
  const bodyParts = [];
  let currentRow = null;
  let headingSerial = 0;

  const finaliseRow = () => {
    if (!currentRow) return;
    completedRows.push(currentRow);
    currentRow = null;
  };

  const tokens = html.match(/<!--[\s\S]*?-->|<![^>]*>|<\/?[A-Za-z][^>]*>|[^<]+/g) || [];
  for (const token of tokens) {
    if (!token.startsWith("<")) {
      const top = stack[stack.length - 1];
      if (top?.suppressed) continue;
      const text = decodeHTMLTextV35(token);
      if (!text.trim()) continue;
      bodyParts.push(text);
      if (currentRow) {
        currentRow.textParts.push(text);
        if (top?.inRouteDisplayName && top?.inStrong) currentRow.routeStrongParts.push(text);
      }
      if (Number.isInteger(top?.headingIndex)) headingBuffers[top.headingIndex]?.parts.push(text);
      continue;
    }
    if (/^<!--|^<![^-]/.test(token)) continue;

    const closing = /^<\//.test(token);
    const nameMatch = token.match(/^<\/?\s*([A-Za-z0-9:-]+)/);
    if (!nameMatch) continue;
    const tag = nameMatch[1].toLowerCase();

    if (closing) {
      let ctx = null;
      while (stack.length) {
        const candidate = stack.pop();
        if (candidate?.tag === tag) { ctx = candidate; break; }
      }
      if (ctx?.headingRoot && Number.isInteger(ctx.headingIndex)) {
        headingBuffers[ctx.headingIndex].closed = true;
      }
      if (ctx?.rowRoot) finaliseRow();
      continue;
    }

    const rawAttrs = token
      .replace(/^<\s*[A-Za-z0-9:-]+/, "")
      .replace(/\/?>\s*$/, "");
    const attrs = parseHTMLAttributesV35(rawAttrs);
    const classes = new Set(String(attrs.class || "").split(/\s+/).filter(Boolean));
    const parent = stack[stack.length - 1] || null;
    const headingRoot = tag === "h1" || tag === "h2" || classes.has("stop-name") || classes.has("page-title");
    const headingIndex = headingRoot
      ? headingSerial++
      : (Number.isInteger(parent?.headingIndex) ? parent.headingIndex : null);
    if (headingRoot) headingBuffers[headingIndex] = { parts: [], closed: false };

    const ctx = {
      tag,
      suppressed: Boolean(parent?.suppressed || tag === "script" || tag === "style" || tag === "noscript"),
      inRouteDisplayName: Boolean(parent?.inRouteDisplayName || classes.has("route-display-name")),
      inStrong: Boolean(parent?.inStrong || tag === "strong"),
      headingRoot,
      headingIndex,
      rowRoot: false
    };

    if (!currentRow && classes.has("tpm_row_timetable")) {
      currentRow = {
        attrs,
        classes,
        textParts: [],
        routeStrongParts: [],
        links: []
      };
      ctx.rowRoot = true;
    }
    if (currentRow && tag === "a" && attrs.href) currentRow.links.push(attrs.href);
    stack.push(ctx);

    const selfClosing = /\/\s*>$/.test(token) || ["br", "img", "meta", "link", "input", "hr"].includes(tag);
    if (selfClosing) {
      const popped = stack.pop();
      if (popped?.headingRoot && Number.isInteger(popped.headingIndex)) headingBuffers[popped.headingIndex].closed = true;
      if (popped?.rowRoot) finaliseRow();
    }
  }
  finaliseRow();

  const bodyText = cleanHTMLTextV35(bodyParts.join(" "));
  const stopName = headingBuffers
    .map(row => cleanHTMLTextV35(row?.parts?.join(" ") || ""))
    .find(Boolean) || null;
  const explicitEmpty = /\bno\s+more\s+services\s+scheduled\b|\bno\s+(?:live\s+|real[- ]?time\s+|upcoming\s+)?(?:services|departures|results)\b|\bthere\s+are\s+no\b/i.test(bodyText);
  const pageIdentifiesStop = new RegExp(`(?:Depart\\s+from\\s+stop|Results\\s+for\\s+Stop)\\s*${stopId}\\b`, "i").test(bodyText);
  const helpMarker = /5\s+departure\s+times\s+will\s+be\s+displayed|times\s+are\s+approximate\s+scheduled\s+times/i.test(bodyText);
  const errorPage = /access\s+denied|captcha|temporarily\s+unavailable|application\s+error|server\s+error|request\s+blocked/i.test(bodyText);
  const authoritativeEmpty = Boolean(
    ATOMIC_ACCEPT_VERIFIED_EMPTY_V35 && completedRows.length === 0 && !errorPage &&
    pageIdentifiesStop && (explicitEmpty || helpMarker)
  );

  const allServices = completedRows.map(row => {
    const text = cleanHTMLTextV35(row.textParts.join(" "));
    if (!text) return null;
    const detailHref = row.links.find(href => /RealTimeFleetTrip|fleet=/i.test(href || "")) || null;
    const detailURL = absoluteURLV35(detailHref, baseURL);
    const route =
      cleanHTMLTextV35(row.attrs["data-route"]) ||
      text.match(/\b([A-Z]?\d{1,4}[A-Z]?|[A-Z]+ CAT|Ferry|Airport Line|Armadale Line|Ellenbrook Line|Fremantle Line|Mandurah Line|Midland Line|Thornlie-Cockburn Line|Yanchep Line)\b/i)?.[1] ||
      null;
    const destination =
      cleanHTMLTextV35(row.routeStrongParts.join(" ")) ||
      cleanHTMLTextV35(row.attrs["data-destination"]) ||
      text.match(/To\s+.+?(?=\s+Depart from stop|\s+\d+\s*MIN|\s+\(sched|$)/i)?.[0]?.trim() ||
      null;
    const fleet =
      cleanHTMLTextV35(row.attrs["data-fleet"]) ||
      queryValueV35(detailHref, ["fleet", "fleetNumber", "vehicle"], baseURL) ||
      text.match(/\bFleet\s*#?\s*(\d{3,5})\b/i)?.[1] ||
      null;
    const tripId =
      cleanHTMLTextV35(row.attrs["data-tripid"]) ||
      cleanHTMLTextV35(row.attrs["data-trip-id"]) ||
      queryValueV35(detailHref, ["tripId", "tripID", "trip", "t"], baseURL) ||
      null;
    const runNumber =
      cleanHTMLTextV35(row.attrs["data-run"]) ||
      text.match(/\b(?:Run|Service)\s*#?\s*([A-Z0-9-]{2,12})\b/i)?.[1] ||
      null;
    const platform =
      cleanHTMLTextV35(row.attrs["data-platform"]) ||
      text.match(/\bPlatform\s*([0-9]+[A-Z]?)\b/i)?.[1] ||
      null;
    const scheduled = /\(sched\.?\)|\bscheduled\b/i.test(text);
    const isLive = !scheduled && (
      row.classes.has("fleet-running") || row.classes.has("live") || Boolean(fleet) ||
      /\bLIVE\b|\barriving\b|\bdeparting\b/i.test(text)
    );
    const due =
      text.match(/\b\d+\s*MIN\b/i)?.[0]?.replace(/\s+/g, " ").toUpperCase() ||
      (/\bNOW\b/i.test(text) ? "NOW" : (/\barriving\b/i.test(text) ? "Arriving" : null));
    const dueMinutesMatch = due?.match(/\d+/);
    const minutesUntilDeparture = dueMinutesMatch ? Number(dueMinutesMatch[0]) : ((due === "Arriving" || due === "NOW") ? 0 : null);
    const time = text.match(/\b\d{1,2}[:.]\d{2}\s*(?:am|pm)\b/i)?.[0]?.replace(/\s+/g, "") || null;
    return {
      route: route ? cleanHTMLTextV35(route) : null,
      destination: destination ? cleanHTMLTextV35(destination) : null,
      stopText: "Depart from stop",
      stopId,
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
  }).filter(service => service && service.route && service.destination);

  const services = (liveOnly ? allServices.filter(service => service.live === true) : allServices).slice(0, limit);
  return {
    stopId,
    stopName: stopName || `Stop ${stopId}`,
    services,
    rawRowCount: allServices.length,
    liveRowCount: allServices.filter(service => service.live === true).length,
    explicitEmpty,
    authoritativeEmpty,
    validStopPage: pageIdentifiesStop && !errorPage,
    hasTimetableMarkup: completedRows.length > 0
  };
}

function parseStopHTMLBatchFastV35(items, limit, liveOnly) {
  const list = Array.isArray(items) ? items.filter(item => item?.ok && item?.html) : [];
  const startedAt = Date.now();
  const parsed = list.map(entry => parseStopHTMLFastV35(entry, limit, liveOnly));
  stats.atomicFastParserDocumentsV35 += parsed.length;
  stats.atomicVerifiedEmptyComponentsV35 += parsed.filter(row => row.authoritativeEmpty === true).length;
  parsed.fastParserMsV35 = Date.now() - startedAt;
  return parsed;
}

function nativeHTTPSGetV35(targetURL, timeoutMs, redirectsLeft = 2) {
  return new Promise(resolve => {
    let settled = false;
    const finish = value => {
      if (settled) return;
      settled = true;
      resolve(value);
    };
    const request = https.get(targetURL, {
      agent: ATOMIC_NATIVE_AGENT_V35,
      headers: {
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Encoding": "identity",
        "Cache-Control": "no-cache, no-store",
        "Pragma": "no-cache",
        "Connection": "keep-alive",
        "Accept-Language": "en-AU,en;q=0.9",
        // Keep one stable, ordinary browser-compatible request profile. Do not
        // rotate identities or headers to evade origin controls.
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148 Safari/604.1"
      }
    }, response => {
      const status = Number(response.statusCode || 0);
      const location = response.headers.location;
      if ([301, 302, 303, 307, 308].includes(status) && location && redirectsLeft > 0) {
        response.resume();
        let redirected = null;
        try { redirected = new URL(location, targetURL).toString(); } catch {}
        if (!redirected) return finish({ ok: false, status, html: "", error: "Invalid redirect" });
        void nativeHTTPSGetV35(redirected, timeoutMs, redirectsLeft - 1).then(finish);
        return;
      }
      const chunks = [];
      let bytes = 0;
      response.on("data", chunk => {
        bytes += chunk.length;
        if (bytes > ATOMIC_COMPONENT_MAX_HTML_BYTES) {
          request.destroy(new Error("MOBI response too large"));
          return;
        }
        chunks.push(chunk);
      });
      response.on("end", () => {
        const html = Buffer.concat(chunks).toString("utf8");
        const ok = status >= 200 && status < 400 && /<html|<!doctype/i.test(html);
        finish({ ok, status, html: ok ? html : "", error: ok ? null : `MOBI HTTP ${status}` });
      });
      response.on("error", error => finish({ ok: false, status, html: "", error: String(error?.message || error) }));
    });
    request.setTimeout(timeoutMs, () => request.destroy(new Error("native-stop-timeout")));
    request.on("error", error => finish({ ok: false, status: 0, html: "", error: String(error?.message || error) }));
  });
}

async function fetchStopHTMLNativeV35(stopId) {
  stats.atomicNativeHTTPFetchesV35 += 1;
  const startedAt = Date.now();
  const response = await nativeHTTPSGetV35(stopUrl(stopId), ATOMIC_NATIVE_HTTP_TIMEOUT_MS_V35);
  if (response.ok) stats.atomicNativeHTTPSuccessesV35 += 1;
  else stats.atomicNativeHTTPErrorsV35 += 1;
  return { stopId: String(stopId), ...response, ms: Date.now() - startedAt, transportV35: "native-https" };
}

async function fetchStopHTMLDirectV32(stopId) {
  await ensureBrowser();
  stats.atomicComponentHTTPFetchesV32 += 1;
  const startedAt = Date.now();
  try {
    const response = await context.request.get(stopUrl(stopId), {
      timeout: ATOMIC_CONTEXT_RETRY_TIMEOUT_MS_V35,
      failOnStatusCode: false,
      headers: {
        "Accept": "text/html,application/xhtml+xml",
        "Cache-Control": "no-cache, no-store",
        "Pragma": "no-cache"
      }
    });
    const status = response.status();
    let html = await response.text();
    if (Buffer.byteLength(html, "utf8") > ATOMIC_COMPONENT_MAX_HTML_BYTES) {
      throw new Error(`MOBI response too large for stop ${stopId}`);
    }
    const ok = status >= 200 && status < 400 && /<html|<!doctype/i.test(html);
    if (ok) stats.atomicComponentHTTPSuccessesV32 += 1;
    else stats.atomicComponentHTTPErrorsV32 += 1;
    return {
      stopId,
      ok,
      status,
      html: ok ? html : "",
      error: ok ? null : `MOBI HTTP ${status}`,
      ms: Date.now() - startedAt
    };
  } catch (error) {
    stats.atomicComponentHTTPErrorsV32 += 1;
    return {
      stopId,
      ok: false,
      status: 0,
      html: "",
      error: String(error?.message || error),
      ms: Date.now() - startedAt
    };
  }
}

async function parseStopHTMLBatchV32(items, limit, liveOnly) {
  const list = Array.isArray(items) ? items.filter(item => item?.ok && item?.html) : [];
  if (!list.length) return [];
  const page = await acquirePage();
  activeBrowserJobs += 1;
  try {
    const out = [];
    for (let offset = 0; offset < list.length; offset += ATOMIC_COMPONENT_PARSE_CHUNK) {
      const chunk = list.slice(offset, offset + ATOMIC_COMPONENT_PARSE_CHUNK);
      const parsed = await page.evaluate(({ entries, limitValue, liveOnlyValue }) => {
        const clean = value => String(value || "").replace(/\s+/g, " ").trim();
        const absolute = (href, baseURL) => {
          if (!href) return null;
          try { return new URL(href, baseURL).toString(); } catch { return null; }
        };
        const queryValue = (href, names, baseURL) => {
          if (!href) return null;
          try {
            const u = new URL(href, baseURL);
            for (const name of names) {
              const value = u.searchParams.get(name);
              if (value) return clean(value);
            }
          } catch {}
          return null;
        };

        return entries.map(entry => {
          const baseURL = `https://136213.mobi/RealTime/RealTimeStopResults.aspx?SN=${encodeURIComponent(entry.stopId)}`;
          const doc = new DOMParser().parseFromString(entry.html, "text/html");
          const rows = Array.from(doc.querySelectorAll(".tpm_row_timetable"));
          const bodyText = clean(doc.body?.textContent || "");
          const headingCandidates = [
            doc.querySelector("h1"),
            doc.querySelector("h2"),
            doc.querySelector(".stop-name"),
            doc.querySelector(".page-title")
          ];
          const stopName = headingCandidates
            .map(node => clean(node?.textContent || ""))
            .find(Boolean) || null;
          const explicitEmpty = /\bno\s+(?:live\s+|real[- ]?time\s+|upcoming\s+)?(?:services|departures|results)\b|\bthere\s+are\s+no\b/i.test(bodyText);

          const allServices = rows.map(row => {
            const text = clean(row.textContent || "");
            if (!text) return null;
            const links = Array.from(row.querySelectorAll("a[href]"));
            const detailHref = links
              .map(link => link.getAttribute("href"))
              .find(href => /RealTimeFleetTrip|fleet=/i.test(href || "")) || null;
            const detailURL = absolute(detailHref, baseURL);
            const route =
              clean(row.getAttribute("data-route")) ||
              text.match(/\b([A-Z]?\d{1,4}[A-Z]?|[A-Z]+ CAT|Airport Line|Armadale Line|Ellenbrook Line|Fremantle Line|Mandurah Line|Midland Line|Thornlie-Cockburn Line|Yanchep Line)\b/i)?.[1] ||
              null;
            const destination =
              clean(row.querySelector(".route-display-name strong")?.textContent) ||
              clean(row.getAttribute("data-destination")) ||
              text.match(/To\s+.+?(?=\s+Depart from stop|\s+\d+\s*MIN|\s+\(sched|$)/i)?.[0]?.trim() ||
              null;
            const stopText =
              Array.from(row.querySelectorAll(".route-display-name"))
                .map(el => clean(el.textContent || ""))
                .find(value => value.toLowerCase().includes("depart from stop")) ||
              "Depart from stop";
            const fleet =
              clean(row.getAttribute("data-fleet")) ||
              clean(row.dataset?.fleet) ||
              queryValue(detailHref, ["fleet", "fleetNumber", "vehicle"], baseURL) ||
              text.match(/\bFleet\s*#?\s*(\d{3,5})\b/i)?.[1] ||
              null;
            const tripId =
              clean(row.getAttribute("data-tripid")) ||
              clean(row.dataset?.tripid) ||
              clean(row.dataset?.tripId) ||
              queryValue(detailHref, ["tripId", "tripID", "trip", "t"], baseURL) ||
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
              stopId: String(entry.stopId),
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
          }).filter(service => service && service.route && service.destination);

          const services = (liveOnlyValue
            ? allServices.filter(service => service.live === true)
            : allServices
          ).slice(0, limitValue);
          return {
            stopId: String(entry.stopId),
            stopName: stopName || `Stop ${entry.stopId}`,
            services,
            rawRowCount: allServices.length,
            liveRowCount: allServices.filter(service => service.live === true).length,
            explicitEmpty,
            hasTimetableMarkup: rows.length > 0
          };
        });
      }, { entries: chunk, limitValue: limit, liveOnlyValue: liveOnly });
      out.push(...parsed);
    }
    return out;
  } finally {
    activeBrowserJobs = Math.max(0, activeBrowserJobs - 1);
    if (!page.isClosed()) releasePage(page);
    else void replaceDeadPage(page, "atomic-html-parser-closed");
  }
}

async function buildAtomicComponentBatchV35(stopIds, perStop, options = {}) {
  const startedAt = Date.now();
  const liveOnly = options.liveOnly !== false;
  const forceRefresh = options.forceRefresh !== false;
  const retryRounds = positiveInt(
    options.retryRounds,
    ATOMIC_COMPONENT_DIRECT_RETRY_ROUNDS,
    0,
    2
  );
  const preferFreshComponentCache = options.preferFreshComponentCache !== false;
  const allowStaleComponentRescue = options.allowStaleComponentRescue !== false;
  const allowPageFallback = options.allowPageFallback === true || ATOMIC_FOREGROUND_PAGE_FALLBACK_V35;
  const resultByStop = new Map();
  const fetchedAt = new Date().toISOString();
  let directFetchMs = 0;
  let directParseMs = 0;
  let directAttemptCount = 0;
  let directRetryRecoveredCount = 0;
  let directWaveCount = 0;
  let freshComponentCacheHitCount = 0;
  let staleComponentRescueCount = 0;

  const cachedResult = (stopId, cached, staleRescue = false) => {
    const payload = cached?.payload;
    if (!payload || payload.ok === false || !Array.isArray(payload.services)) return false;
    resultByStop.set(String(stopId), {
      stopId: String(stopId),
      ok: true,
      stopName: payload.stopName || `Stop ${stopId}`,
      source: staleRescue
        ? "136213-browser-v3.4-stale-component-rescue"
        : "136213-browser-v3.4-fresh-component-cache",
      count: payload.services.length,
      services: payload.services,
      cache: {
        hit: true,
        stale: cached.stale === true,
        ageMs: Number(cached.ageMs || 0),
        directHTTP: false
      },
      directHTTP: false,
      authoritativeEmptyBoard:
        payload.authoritativeEmptyBoard === true ||
        (payload.services.length === 0 && Number(payload.rawRowCount || 0) === 0),
      ms: 0
    });
    return true;
  };

  if (preferFreshComponentCache) {
    for (const rawStopId of stopIds) {
      const stopId = String(rawStopId);
      const cached = getCache(cacheKey(stopId, perStop, liveOnly), false);
      if (cachedResult(stopId, cached, false)) freshComponentCacheHitCount += 1;
    }
  }

  let unresolvedIds = stopIds.map(String).filter(stopId => !resultByStop.has(stopId));

  for (let round = 0; round <= retryRounds && unresolvedIds.length; round += 1) {
    if (round > 0 && ATOMIC_COMPONENT_DIRECT_RETRY_DELAY_MS > 0) {
      await new Promise(resolve => setTimeout(resolve, ATOMIC_COMPONENT_DIRECT_RETRY_DELAY_MS));
    }
    const roundIds = unresolvedIds.slice();
    const directConcurrency = Math.max(
      1,
      Math.min(ATOMIC_COMPONENT_HTTP_CONCURRENCY, roundIds.length)
    );
    directWaveCount += Math.ceil(roundIds.length / directConcurrency);
    const fetchStartedAt = Date.now();
    const direct = await mapLimit(
      roundIds,
      directConcurrency,
      stopId => round === 0
        ? fetchStopHTMLNativeV35(stopId)
        : fetchStopHTMLDirectV32(stopId)
    );
    if (round > 0) stats.atomicContextRetryFetchesV35 += direct.length;
    directFetchMs += Date.now() - fetchStartedAt;
    directAttemptCount += direct.length;
    if (round > 0) stats.atomicComponentHTTPRetryFetchesV33 += direct.length;

    const parseStartedAt = Date.now();
    const parsed = parseStopHTMLBatchFastV35(direct, perStop, liveOnly);
    directParseMs += Date.now() - parseStartedAt;
    const parsedByStop = new Map(parsed.map(row => [String(row.stopId), row]));
    const directByStop = new Map(direct.map(row => [String(row.stopId), row]));
    const nextUnresolved = [];

    for (const stopId of roundIds) {
      const parsedRow = parsedByStop.get(String(stopId));
      const directRow = directByStop.get(String(stopId));
      const directUsable = Boolean(
        directRow?.ok && parsedRow &&
        (parsedRow.hasTimetableMarkup || parsedRow.authoritativeEmpty === true)
      );
      if (!directUsable) {
        nextUnresolved.push(String(stopId));
        continue;
      }
      if (round > 0) directRetryRecoveredCount += 1;
      const services = (parsedRow.services || []).map(service => ({
        ...service,
        observedAt: fetchedAt,
        liveFetchedAt: fetchedAt
      }));
      const payload = {
        ok: true,
        stopId: String(stopId),
        stopName: parsedRow.stopName || `Stop ${stopId}`,
        source: round > 0
          ? "136213-browser-v3.5-all-ids-context-retry"
          : "136213-browser-v3.5-all-ids-native-http",
        freshLive: forceRefresh,
        liveOnly,
        rawRowCount: parsedRow.rawRowCount,
        liveRowCount: parsedRow.liveRowCount,
        count: services.length,
        services,
        fetchedAt,
        timings: {
          browserMs: Number(directRow?.ms || 0),
          rowWaitMs: 0,
          directAttempt: round + 1
        }
      };
      setCache(cacheKey(String(stopId), perStop, liveOnly), payload);
      updateGroupedStopHotnessV29(stopId, services.filter(row => row?.live === true).length);
      resultByStop.set(String(stopId), {
        stopId: String(stopId),
        ok: true,
        stopName: payload.stopName,
        source: payload.source,
        count: services.length,
        services,
        cache: { hit: false, directHTTP: true },
        directHTTP: true,
        directAttempt: round + 1,
        authoritativeEmptyBoard: services.length === 0 && parsedRow.authoritativeEmpty === true,
        ms: Number(directRow?.ms || 0)
      });
    }
    unresolvedIds = nextUnresolved;
  }
  stats.atomicComponentHTTPRetryRecoveriesV33 += directRetryRecoveredCount;

  if (allowStaleComponentRescue && unresolvedIds.length) {
    const stillUnresolved = [];
    for (const stopId of unresolvedIds) {
      const cached = getCache(cacheKey(String(stopId), perStop, liveOnly), true);
      if (cachedResult(stopId, cached, true)) staleComponentRescueCount += 1;
      else stillUnresolved.push(String(stopId));
    }
    unresolvedIds = stillUnresolved;
  }

  let fallbackMs = 0;
  const fallbackIds = allowPageFallback ? unresolvedIds.slice() : [];
  if (!allowPageFallback && unresolvedIds.length) stats.atomicPageFallbacksSkippedV35 += unresolvedIds.length;
  if (fallbackIds.length) {
    stats.atomicComponentHTTPFallbacksV32 += fallbackIds.length;
    const fallbackStartedAt = Date.now();
    const fallbackRows = await mapLimit(
      fallbackIds,
      Math.min(ATOMIC_COMPONENT_PAGE_FALLBACK_CONCURRENCY, fallbackIds.length),
      stopId => scanGroupedStopV29(stopId, perStop, {
        forceRefresh,
        liveOnly,
        allowStale: false,
        cacheResult: true,
        hotFirst: false,
        backgroundComplete: false
      })
    );
    fallbackMs = Date.now() - fallbackStartedAt;
    for (const row of fallbackRows) {
      resultByStop.set(String(row.stopId), { ...row, directHTTP: false });
    }
  }

  const rowsByStop = stopIds.map(stopId => resultByStop.get(String(stopId)) || {
    stopId: String(stopId),
    ok: false,
    stopName: `Stop ${stopId}`,
    source: "136213-browser-v3.5-all-ids-component-missing",
    count: 0,
    services: [],
    error: "Component did not complete"
  });
  const failedStopIds = rowsByStop.filter(row => row?.ok === false).map(row => row.stopId);
  const completedComponentCount = rowsByStop.length - failedStopIds.length;
  const services = rowsByStop.flatMap(row => row?.services || []);
  const complete = failedStopIds.length === 0 && rowsByStop.length === stopIds.length;
  if (complete) stats.atomicComponentBatchCompletedV32 += 1;
  stats.atomicComponentFreshCacheHitsV34 += freshComponentCacheHitCount;
  stats.atomicComponentStaleRescuesV34 += staleComponentRescueCount;
  if (directWaveCount <= 1) stats.atomicComponentOneWaveBuildsV34 += 1;

  return {
    ok: complete,
    source: complete
      ? "136213-browser-v3.5-all-ids-native-multiplex-complete"
      : "136213-browser-v3.5-all-ids-native-multiplex-partial",
    componentSnapshots: true,
    atomicBoard: true,
    sharedSessionHTTPV32: true,
    allComponentIDsOneRequestV33: true,
    allIDsConcurrentV34: true,
    nativeHTTPMultiplexV35: true,
    fastNodeHTMLParserV35: true,
    verifiedEmptyAcceptedV35: ATOMIC_ACCEPT_VERIFIED_EMPTY_V35,
    foregroundPageFallbackV35: allowPageFallback,
    upstreamSingleStopEndpointV34: true,
    upstreamRequestsIssuedTogetherV34: true,
    upstreamRequestCountV34: stopIds.length - freshComponentCacheHitCount,
    upstreamFetchWaveCountV34: directWaveCount,
    upstreamOneWaveV34: directWaveCount <= 1,
    foregroundPageFallbackV34: fallbackIds.length > 0,
    aggregateCacheHitV34: false,
    freshComponentCacheHitCountV34: freshComponentCacheHitCount,
    staleComponentRescueCountV34: staleComponentRescueCount,
    grouped: stopIds.length > 1,
    stopIds,
    perStop,
    componentCount: stopIds.length,
    completedComponentCount,
    failedComponentCount: failedStopIds.length,
    scannedComponentCount: rowsByStop.length,
    failedStopIds,
    groupedScanComplete: complete,
    authoritativeEmptyBoard: complete && services.length === 0,
    rowsByStop,
    services,
    count: services.length,
    fetchedAt: new Date().toISOString(),
    timings: {
      requestTotalMs: Date.now() - startedAt,
      directFetchMs,
      directParseMs,
      nativeHTTPTimeoutMsV35: ATOMIC_NATIVE_HTTP_TIMEOUT_MS_V35,
      contextRetryTimeoutMsV35: ATOMIC_CONTEXT_RETRY_TIMEOUT_MS_V35,
      fallbackMs,
      directHTTPCount: stopIds.length - fallbackIds.length - freshComponentCacheHitCount,
      pageFallbackCount: fallbackIds.length,
      directAttemptCount,
      directRetryRounds: retryRounds,
      directRetryRecoveredCount,
      httpConcurrency: ATOMIC_COMPONENT_HTTP_CONCURRENCY,
      pageFallbackConcurrency: ATOMIC_COMPONENT_PAGE_FALLBACK_CONCURRENCY,
      upstreamFetchWaveCountV34: directWaveCount
    }
  };
}

app.get([
  "/live-stop-components-atomic",
  "/live-stop-components-multiplex"
], async (req, res) => {
  stats.requests += 1;
  stats.atomicComponentBatchRequestsV32 += 1;
  if (!authOk(req)) return res.status(401).json({ ok: false, error: "Unauthorized" });
  const stopIds = uniqueStopIds(req.query.stops || req.query.stopIds || req.query.ids);
  if (!stopIds.length) {
    return res.status(400).json({ ok: false, error: "Missing stop IDs. Use stops=123,456" });
  }
  const perStop = positiveInt(req.query.perStop || req.query.limitPerStop, 5, 1, 30);
  const liveOnly = String(req.query.liveOnly || req.query.live || "1") !== "0";
  const retryRounds = positiveInt(req.query.retryRounds, ATOMIC_COMPONENT_DIRECT_RETRY_ROUNDS, 0, 2);
  const forceRefresh = String(req.query.fresh || req.query.refresh || "0") !== "0";
  const forceAggregateRefresh = String(req.query.forceAggregateRefresh || "0") === "1";
  const preferFreshComponentCache = String(req.query.preferFreshComponentCache || "1") !== "0";
  const allowStaleComponentRescue = String(req.query.allowStaleComponentRescue || "1") !== "0";
  const maxAgeMs = positiveInt(req.query.maxAgeMs, 12000, 0, 120000);
  const allowPageFallback = String(req.query.allowPageFallback || "0") === "1";
  const stableIds = stopIds.slice().sort((a, b) => Number(a) - Number(b));
  const aggregateKey = `atomic-v35|${stableIds.join(",")}|${perStop}|${liveOnly ? 1 : 0}`;

  if (!forceAggregateRefresh) {
    const cached = getBatchCache(aggregateKey, false);
    if (cached && cached.ageMs <= maxAgeMs && cached.payload?.ok === true) {
      stats.atomicComponentAggregateCacheHitsV34 += 1;
      res.set("Cache-Control", "no-store");
      return res.status(200).json({
        ...cached.payload,
        source: "136213-browser-v3.5-all-ids-aggregate-cache",
        aggregateCacheHitV34: true,
        aggregateCacheAgeMsV34: cached.ageMs,
        allIDsConcurrentV34: true,
        nativeHTTPMultiplexV35: true,
        fastNodeHTMLParserV35: true,
        verifiedEmptyAcceptedV35: ATOMIC_ACCEPT_VERIFIED_EMPTY_V35,
        foregroundPageFallbackV35: false,
        upstreamRequestsIssuedTogetherV34: true,
        upstreamRequestCountV34: 0,
        upstreamFetchWaveCountV34: 0,
        upstreamOneWaveV34: true,
        fetchedAt: new Date().toISOString()
      });
    }
  }

  const key = `${aggregateKey}|retry=${retryRounds}|fresh=${forceRefresh ? 1 : 0}`;
  let promise = atomicComponentBatchInFlightV32.get(key);
  if (promise) {
    stats.atomicComponentBatchCoalescedV32 += 1;
  } else {
    promise = buildAtomicComponentBatchV35(stopIds, perStop, {
      liveOnly,
      forceRefresh,
      retryRounds,
      preferFreshComponentCache,
      allowStaleComponentRescue,
      allowPageFallback
    }).finally(() => {
      if (atomicComponentBatchInFlightV32.get(key) === promise) {
        atomicComponentBatchInFlightV32.delete(key);
      }
    });
    atomicComponentBatchInFlightV32.set(key, promise);
  }
  try {
    const payload = await promise;
    if (payload.ok === true) setBatchCache(aggregateKey, payload, { markRequested: true });
    res.set("Cache-Control", "no-store");
    return res.status(payload.ok ? 200 : 207).json(payload);
  } catch (error) {
    stats.batchErrors += 1;
    return res.status(504).json({
      ok: false,
      source: "136213-browser-v3.5-all-ids-native-multiplex-error",
      atomicBoard: true,
      allIDsConcurrentV34: true,
      nativeHTTPMultiplexV35: true,
      fastNodeHTMLParserV35: true,
      stopIds,
      perStop,
      error: String(error?.message || error),
      fetchedAt: new Date().toISOString()
    });
  }
});

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
        ? "136213-browser-v3.3-component-stop-snapshots"
        : "136213-browser-v3.3-component-stop-snapshots-partial",
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
      source: "136213-browser-v3.3-component-stop-snapshots-error",
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
      source: "136213-browser-v3.3-batched-stops",
      stopIds,
      error: String(error.message || error),
      fetchedAt: new Date().toISOString(),
      timings: { requestTotalMs: Date.now() - startedAt }
    });
  }
});

app.post("/capture/sjp", (req, res) => {
  stats.requests += 1;
  stats.sjpCaptureIngestRequestsV2252 += 1;
  if (!sjpCaptureAuthOkV2252(req)) {
    stats.sjpCaptureRejectedV2252 += 1;
    return res.status(401).json({ ok: false, error: "Unauthorized capture ingest" });
  }

  const kind = String(req.body?.kind || req.body?.type || "").trim().toLowerCase();
  const upstream = req.body?.upstream || req.body?.response || req.body?.payload || null;
  const receivedAtMs = Date.now();
  pruneSJPCaptureV2252(receivedAtMs);

  if (kind === "stop") {
    const payload = buildSJPCapturedStopPayloadV2252(upstream, receivedAtMs);
    if (!payload) {
      stats.sjpCaptureRejectedV2252 += 1;
      return res.status(400).json({ ok: false, error: "Invalid SJP /Stop response payload" });
    }
    sjpCapturedStopsV2252.set(payload.stopId, {
      payload,
      createdAt: receivedAtMs,
      expiresAt: receivedAtMs + SJP_CAPTURE_TTL_MS_V2252
    });
    for (const service of payload.services || []) {
      if (!service?.tripId || service.live !== true) continue;
      const tripPayload = {
        ok: true,
        source: "transperth-sjp-mitm-stop-trip-index-v2252",
        captureBridgeV2252: true,
        tripId: service.tripId,
        tripUid: service.tripUid,
        route: service.route,
        destination: service.destination,
        fleetNumber: service.fleetNumber,
        fleet: service.fleetNumber,
        vehicleId: service.vehicleId,
        latitude: service.latitude,
        longitude: service.longitude,
        heading: service.heading,
        liveUpdatedAt: service.liveUpdatedAt,
        live: true,
        stopId: payload.stopId,
        scheduledTime: service.scheduledTime,
        liveTime: service.liveTime,
        estimatedArrivalTime: service.estimatedArrivalTime,
        estimatedDepartureTime: service.estimatedDepartureTime,
        capturedAt: payload.capturedAt,
        capturedAtMs: receivedAtMs,
        fetchedAt: payload.fetchedAt
      };
      sjpCapturedTripsV2252.set(service.tripId, {
        payload: tripPayload,
        createdAt: receivedAtMs,
        expiresAt: receivedAtMs + SJP_CAPTURE_TTL_MS_V2252
      });
    }
    stats.sjpCaptureStopPublishesV2252 += 1;
    pruneSJPCaptureV2252(receivedAtMs);
    return res.status(202).json({
      ok: true,
      kind: "stop",
      stopId: payload.stopId,
      count: payload.count,
      liveCount: payload.liveCount,
      capturedAt: payload.capturedAt
    });
  }

  if (kind === "trip") {
    const payload = buildSJPCapturedTripPayloadV2252(upstream, receivedAtMs);
    if (!payload) {
      stats.sjpCaptureRejectedV2252 += 1;
      return res.status(400).json({ ok: false, error: "Invalid SJP /Trip response payload" });
    }
    sjpCapturedTripsV2252.set(payload.tripId, {
      payload,
      createdAt: receivedAtMs,
      expiresAt: receivedAtMs + SJP_CAPTURE_TTL_MS_V2252
    });
    stats.sjpCaptureTripPublishesV2252 += 1;
    pruneSJPCaptureV2252(receivedAtMs);
    return res.status(202).json({
      ok: true,
      kind: "trip",
      tripId: payload.tripId,
      fleetNumber: payload.fleetNumber,
      live: payload.live,
      capturedAt: payload.capturedAt
    });
  }

  stats.sjpCaptureRejectedV2252 += 1;
  return res.status(400).json({ ok: false, error: "kind must be stop or trip" });
});

app.get("/sjp/captured-stop/:stopId", (req, res) => {
  stats.requests += 1;
  if (!authOk(req)) return res.status(401).json({ ok: false, error: "Unauthorized" });
  const stopId = String(req.params.stopId || "").trim();
  if (!/^\d{1,8}$/.test(stopId)) return res.status(400).json({ ok: false, error: "Invalid stop number" });
  const maxAgeMs = positiveInt(req.query.maxAgeMs, SJP_CAPTURE_TTL_MS_V2252, 1000, SJP_CAPTURE_TTL_MS_V2252);
  const payload = capturedSJPStopV2252(stopId, maxAgeMs);
  res.set("Cache-Control", "no-store");
  if (!payload) {
    return res.status(404).json({
      ok: false,
      source: "transperth-sjp-mitm-capture-v2252",
      stopId,
      error: "No fresh captured SJP stop response"
    });
  }
  return res.status(200).json(payload);
});

app.get("/sjp/captured-stops", (req, res) => {
  stats.requests += 1;
  if (!authOk(req)) return res.status(401).json({ ok: false, error: "Unauthorized" });
  const stopIds = Array.from(new Set(
    String(req.query.stops || req.query.stopIds || req.query.stop || "")
      .split(/[\s,;|]+/)
      .map(value => value.trim())
      .filter(value => /^\d{1,8}$/.test(value))
  )).slice(0, 96);
  if (!stopIds.length) {
    return res.status(400).json({ ok: false, error: "Provide stops=20506,20507" });
  }
  const maxAgeMs = positiveInt(
    req.query.maxAgeMs,
    SJP_CAPTURE_TTL_MS_V2252,
    1000,
    SJP_CAPTURE_TTL_MS_V2252
  );
  const components = [];
  const hitStopIds = [];
  const missingStopIds = [];
  const services = [];
  let newestCapturedAtMs = 0;
  let oldestCaptureAgeMs = null;
  for (const stopId of stopIds) {
    const payload = capturedSJPStopV2252(stopId, maxAgeMs);
    if (!payload) {
      missingStopIds.push(stopId);
      continue;
    }
    hitStopIds.push(stopId);
    const componentServices = (Array.isArray(payload.services) ? payload.services : []).map(service => ({
      ...service,
      stopId: String(service?.stopId || stopId),
      groupedComponentStopId: stopId,
      source: String(service?.source || "transperth-sjp-stop-v1"),
      provider: "realtime.transperth.info",
      origin: "transperth-sjp-captured-stop-v2253"
    }));
    services.push(...componentServices);
    const capturedAtMs = Number(payload.capturedAtMs || Date.parse(payload.capturedAt || "") || 0);
    if (capturedAtMs > newestCapturedAtMs) newestCapturedAtMs = capturedAtMs;
    const ageMs = Number(payload.captureAgeMs || 0);
    if (Number.isFinite(ageMs)) {
      oldestCaptureAgeMs = oldestCaptureAgeMs == null ? ageMs : Math.max(oldestCaptureAgeMs, ageMs);
    }
    components.push({
      stopId,
      stopName: String(payload.requestedStop?.Description || payload.stopName || `Stop ${stopId}`),
      count: componentServices.length,
      liveCount: componentServices.filter(service => service.live === true).length,
      scheduledCount: componentServices.filter(service => service.live !== true).length,
      captureAgeMs: ageMs,
      capturedAt: payload.capturedAt || null,
      authoritativeEmpty: componentServices.length === 0
    });
  }
  services.sort((a, b) => sjpServiceSortMsV1(a) - sjpServiceSortMsV1(b));
  res.set("Cache-Control", "no-store");
  return res.status(200).json({
    ok: true,
    source: "transperth-sjp-mitm-capture-batch-v2253",
    captureBridgeV2252: true,
    sjpBusBoardV2253: true,
    requestedStopIds: stopIds,
    hitStopIds,
    missingStopIds,
    allCaptured: missingStopIds.length === 0,
    componentCount: components.length,
    count: services.length,
    liveCount: services.filter(service => service.live === true).length,
    scheduledCount: services.filter(service => service.live !== true).length,
    oldestCaptureAgeMs,
    components,
    services,
    capturedAt: newestCapturedAtMs > 0 ? new Date(newestCapturedAtMs).toISOString() : null,
    fetchedAt: new Date().toISOString()
  });
});

app.get("/sjp/captured-trip/:tripId", (req, res) => {
  stats.requests += 1;
  if (!authOk(req)) return res.status(401).json({ ok: false, error: "Unauthorized" });
  const tripId = String(req.params.tripId || "").trim();
  if (!/^\d{1,12}$/.test(tripId)) return res.status(400).json({ ok: false, error: "Invalid trip id" });
  const maxAgeMs = positiveInt(req.query.maxAgeMs, SJP_CAPTURE_TTL_MS_V2252, 1000, SJP_CAPTURE_TTL_MS_V2252);
  const payload = capturedSJPTripV2252(tripId, maxAgeMs);
  res.set("Cache-Control", "no-store");
  if (!payload) {
    return res.status(404).json({
      ok: false,
      source: "transperth-sjp-mitm-trip-capture-v2252",
      tripId,
      error: "No fresh captured SJP trip response"
    });
  }
  return res.status(200).json(payload);
});

app.get("/sjp/stop/:stopId", async (req, res) => {
  stats.requests += 1;
  stats.sjpStopRequestsV1 += 1;
  if (!authOk(req)) return res.status(401).json({ ok: false, error: "Unauthorized" });

  const stopId = String(req.params.stopId || "").trim();
  if (!/^\d{1,8}$/.test(stopId)) {
    return res.status(400).json({ ok: false, error: "Invalid stop number" });
  }
  const requestedTime = validSJPRequestTimeV1(req.query.time) || perthMinuteStampV1();
  const raw = String(req.query.raw || "0") === "1";
  const fresh = String(req.query.fresh || req.query.refresh || "0") === "1";
  const startedAt = Date.now();
  try {
    const payload = await fetchSJPStopV1(stopId, { time: requestedTime, raw, fresh });
    res.set("Cache-Control", "no-store");
    return res.status(200).json({
      ...payload,
      windowMinutesRequested: 120,
      timings: { ...(payload.timings || {}), totalMs: Date.now() - startedAt }
    });
  } catch (error) {
    const authMissing = error?.code === "SJP_AUTH_NOT_CONFIGURED";
    return res.status(authMissing ? 503 : (Number(error?.status) || 502)).json({
      ok: false,
      source: "transperth-sjp-stop-v1-error",
      stopId,
      requestedTime,
      error: String(error?.message || error),
      authConfigured: sjpAuthConfiguredV1(),
      fetchedAt: new Date().toISOString(),
      timings: { totalMs: Date.now() - startedAt }
    });
  }
});

app.get("/sjp/group", async (req, res) => {
  stats.requests += 1;
  stats.sjpGroupRequestsV1 += 1;
  if (!authOk(req)) return res.status(401).json({ ok: false, error: "Unauthorized" });

  const stopIds = uniqueStopIds(req.query.stops || req.query.stopIds || req.query.ids);
  if (stopIds.length < 2) {
    return res.status(400).json({ ok: false, error: "Provide at least two stop IDs with stops=123,456" });
  }
  const requestedTime = validSJPRequestTimeV1(req.query.time) || perthMinuteStampV1();
  const fresh = String(req.query.fresh || req.query.refresh || "0") === "1";
  const includeRaw = String(req.query.raw || "0") === "1";
  const visibleLimit = positiveInt(req.query.visibleLimit, stopIds.length >= 6 ? 10 : 200, 1, 500);
  const startedAt = Date.now();

  try {
    const rowsByStop = await mapLimit(
      stopIds,
      Math.min(SJP_GROUP_CONCURRENCY, stopIds.length),
      async stopId => {
        try {
          const payload = await fetchSJPStopV1(stopId, {
            time: requestedTime,
            fresh,
            raw: includeRaw
          });
          return {
            stopId,
            ok: true,
            count: payload.count,
            liveCount: payload.liveCount,
            scheduledCount: payload.scheduledCount,
            upstreamTimeBandMinutes: payload.upstreamTimeBandMinutes,
            services: payload.services,
            ...(includeRaw ? { raw: payload.raw } : {})
          };
        } catch (error) {
          return {
            stopId,
            ok: false,
            count: 0,
            services: [],
            error: String(error?.message || error)
          };
        }
      }
    );

    const failedStopIds = rowsByStop.filter(row => row.ok !== true).map(row => row.stopId);
    const services = rowsByStop
      .flatMap(row => (row.services || []).map(service => ({
        ...service,
        groupedComponentStopId: row.stopId
      })))
      .sort((a, b) => sjpServiceSortMsV1(a) - sjpServiceSortMsV1(b));
    const liveCount = services.filter(service => service.live === true).length;
    const largeGroup = stopIds.length >= 6;
    const visibleServices = largeGroup ? services.slice(0, visibleLimit) : services;
    const complete = failedStopIds.length === 0;

    res.set("Cache-Control", "no-store");
    return res.status(complete ? 200 : 207).json({
      ok: complete || services.length > 0,
      source: complete ? "transperth-sjp-group-v1-complete" : "transperth-sjp-group-v1-partial",
      grouped: true,
      stopIds,
      requestedTime,
      windowMinutesRequested: 120,
      componentCount: stopIds.length,
      completedComponentCount: stopIds.length - failedStopIds.length,
      failedComponentCount: failedStopIds.length,
      failedStopIds,
      largeGroup,
      visibleLimit: largeGroup ? visibleLimit : null,
      count: services.length,
      liveCount,
      scheduledCount: Math.max(0, services.length - liveCount),
      services,
      visibleServices,
      rowsByStop,
      fetchedAt: new Date().toISOString(),
      timings: { requestTotalMs: Date.now() - startedAt, concurrency: SJP_GROUP_CONCURRENCY }
    });
  } catch (error) {
    return res.status(502).json({
      ok: false,
      source: "transperth-sjp-group-v1-error",
      stopIds,
      requestedTime,
      error: String(error?.message || error),
      authConfigured: sjpAuthConfiguredV1(),
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
    console.log(`Transperth browser v3.6 pressure-governor listening on port ${PORT}; pool=${POOL_SIZE}; mobiConcurrency=${ATOMIC_COMPONENT_HTTP_CONCURRENCY}`);
    console.log(`Playwright Chromium: ${chromium.executablePath()}`);
    if (PREWARM_KNOWN_GROUPS) {
      void prewarmKnownGroupedStops().then(() => {
        console.log("Known grouped-stop caches prewarmed.");
      });
    } else {
      console.log("v3.6 pressure governor: startup grouped prewarm disabled.");
    }

    if (BATCH_BACKGROUND_REFRESH_ENABLED) {
      const groupedRefreshTimer = setInterval(() => {
        void refreshRecentlyRequestedGroupedStops();
      }, BATCH_REFRESH_INTERVAL_MS);
      groupedRefreshTimer.unref?.();
    } else {
      console.log("v3.6 pressure governor: Render-side grouped background refresh disabled; caller/cron owns cadence.");
    }

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
