const express = require("express");
const { chromium } = require("playwright");

const app = express();
const PORT = process.env.PORT || 3000;
const BROWSER_TOKEN = process.env.BROWSER_TOKEN || "dev-token";

let browser;

function authOk(req) {
  return (req.headers.authorization || "") === `Bearer ${BROWSER_TOKEN}`;
}

function browserOptions() {
  return {
    viewport: { width: 390, height: 844 },
    userAgent:
      "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148 Safari/604.1"
  };
}

function stopUrl(stopId) {
  return `https://136213.mobi/RealTime/RealTimeStopResults.aspx?SN=${encodeURIComponent(stopId)}`;
}

app.get("/", (req, res) => {
  res.json({
    ok: true,
    service: "transperth-browser",
    endpoints: ["/live-stop/26768?limit=5"]
  });
});

app.get("/live-stop/:stopId", async (req, res) => {
  if (!authOk(req)) {
    return res.status(401).json({ ok: false, error: "Unauthorized" });
  }

  const stopId = req.params.stopId;
  const limit = Math.max(1, Math.min(Number(req.query.limit || 5), 20));

  const page = await browser.newPage(browserOptions());

  try {
    await page.goto(stopUrl(stopId), {
      waitUntil: "networkidle",
      timeout: 20000
    });

    const services = await page.evaluate((limit) => {
      const rows = Array.from(document.querySelectorAll(".tpm_row_timetable"));

      return rows
        .map(row => {
          const text = (row.innerText || row.textContent || "")
            .replace(/\s+/g, " ")
            .trim();

          if (!text) return null;

          const route = text.match(/\b([A-Z]?\d{1,4}[A-Z]?|[A-Z]+ CAT)\b/)?.[1] || null;

          const destination =
            row.querySelector(".route-display-name strong")?.innerText?.trim() ||
            text.match(/To\s+.+?(?=\s+Depart from stop|\s+\d+\s*MIN|\s+\(sched)/)?.[0]?.trim() ||
            null;

          const stopText =
            Array.from(row.querySelectorAll(".route-display-name"))
              .map(el => el.innerText.trim())
              .find(t => t.toLowerCase().includes("depart from stop")) ||
            `Depart from stop`;

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
    }, limit);

    res.json({
      ok: true,
      stopId,
      stopName: `Stop ${stopId}`,
      source: "136213-browser",
      count: services.length,
      services,
      fetchedAt: new Date().toISOString()
    });
  } catch (error) {
    res.status(500).json({
      ok: false,
      stopId,
      error: String(error.message || error)
    });
  } finally {
    await page.close();
  }
});

app.listen(PORT, async () => {
  browser = await chromium.launch({ headless: true });
  console.log(`Transperth browser service running on http://localhost:${PORT}`);
  console.log(`Token: ${BROWSER_TOKEN}`);
});
