// europass_bulk_fetch_pool.js
// Concurrency across 10 countries, resume-safe via europass_meta.json, per-page NDJSON output.

const fs = require("fs").promises;
const path = require("path");

// If you're on Node < 18, uncomment the next line:
// global.fetch ||= require("node-fetch");

const BASE_URL = "https://europa.eu/europass/eportfolio/api/qdr/europass/qdr-search/search";

const PAGE_SIZE = 10;            // Increased page size (resumes safely)
const MAX_RETRIES = 3;
const FETCH_TIMEOUT_MS = 45000;   // Abort any single request after 45s
const SAVE_TO_DISK = true;        // NDJSON per (country, level)
const CONCURRENCY = 10;           // 🔟 countries in parallel
const TYPE = "qualification";     // or "learning-opportunity"
const OUT_DIR = "files";
const META_PATH = "europass_meta.json";
const MAX_PAGES_PER_LEVEL = 10000; // safety cap

// Optional simple global rate limit (requests/sec across all workers)
const MAX_RPS = 3;
let tokens = MAX_RPS;
setInterval(() => { tokens = Math.min(tokens + MAX_RPS, MAX_RPS); }, 1000);
async function rateLimit() { while (tokens <= 0) await sleep(50); tokens--; }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// Heartbeat & stall detection
let lastProgressAt = Date.now();
function noteProgress() { lastProgressAt = Date.now(); }
setInterval(() => {
  const idleSec = Math.round((Date.now() - lastProgressAt) / 1000);
  console.log(`⏱️ heartbeat: last progress ${idleSec}s ago`);
  if (idleSec > 300) {
    console.warn("🚨 No progress in >5 minutes. Requests have timeouts; if still stuck, Ctrl+C and rerun (resume is safe).");
  }
}, 60000);

const country_code = [
  "ALA","ALB","AUT","BEL","BGR","CYP","CZE","DNK","EST","FIN","FRA","GUF","DEU","GRC","GLP",
  "HUN","ISL","IRL","ITA","LVA","LIE","LTU","LUX","MLT","MTQ","MYT","MDA","MNE","NLD","MKD",
  "NOR","POL","PRT","REU","ROU","MAF","SRB","SVK","SVN","ESP","SWE","TUR","UKR"
];
const eqfLevel = [1,2,3,4,5,6,7,8];

// Allow subset via env: COUNTRY=DEU,FRA
const COUNTRY_FILTER = process.env.COUNTRY?.split(",").map(s => s.trim().toUpperCase()).filter(Boolean);
const countries = COUNTRY_FILTER?.length
  ? country_code.filter(c => COUNTRY_FILTER.includes(c))
  : country_code;

const headers = {
  Accept: "application/json, text/plain, */*",
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
  "Accept-Language": "en-US,en;q=0.9",
  Referer: "https://europa.eu/",
  Origin: "https://europa.eu",
};

// ---------------------- META HANDLING (CONCURRENT-SAFE) ----------------------

let metaWriteLock = Promise.resolve(); // serialize writes; never leave rejected

async function loadMeta() {
  try {
    const raw = await fs.readFile(META_PATH, "utf-8");
    return JSON.parse(raw);
  } catch {
    return {};
  }
}

async function safeWriteMetaFile(meta) {
  const tmpFile = `${META_PATH}.tmp.${process.pid}.${Date.now()}.${Math.random().toString(36).slice(2)}`;
  await fs.writeFile(tmpFile, JSON.stringify(meta, null, 2), "utf-8");
  for (let attempt = 0; attempt < 5; attempt++) {
    try {
      await fs.rename(tmpFile, META_PATH);
      return;
    } catch (err) {
      if (err.code === "EPERM" || err.code === "ENOENT") {
        await sleep(100 * (attempt + 1));
        continue;
      } else {
        console.error("❌ Failed to save meta:", err);
        try { await fs.unlink(tmpFile); } catch {}
        throw err;
      }
    }
  }
  console.warn("⚠️ Could not safely rename temp meta file after several attempts.");
  try { await fs.unlink(tmpFile); } catch {}
}

async function saveMeta(meta) {
  // Always catch so chain never becomes permanently rejected
  metaWriteLock = metaWriteLock.then(() =>
    safeWriteMetaFile(meta).catch(err => {
      console.error("⚠️ saveMeta error (continuing):", err?.message || err);
    })
  );
  return metaWriteLock;
}

function getMeta(meta, country, level) {
  if (!meta[country]) meta[country] = {};
  if (!meta[country][level]) {
    meta[country][level] = {
      from: 0,
      completed: false,
      file: path.join(OUT_DIR, `${country}_eqf${level}.ndjson`),
      lastUpdated: null,
      totalPages: 0,
      totalItems: 0,
    };
  }
  return meta[country][level];
}

function markUpdated(entry, { from, pageItems }) {
  entry.from = from;
  entry.lastUpdated = new Date().toISOString();
  entry.totalPages += 1;
  entry.totalItems += pageItems;
}

function markComplete(entry) {
  entry.completed = true;
  entry.lastUpdated = new Date().toISOString();
}

// ---------------------- NETWORK & PARSING ----------------------

function buildUrl(country, level, from) {
  return `${BASE_URL}?keywords=&size=${PAGE_SIZE}&from=${from}` +
    `&sortType=publication%20date&language=en&type=${TYPE}` +
    `&location=http://publications.europa.eu/resource/authority/country/${country}` +
    `&eqfLevel=http://data.europa.eu/snb/eqf/${level}&version=1.8`;
}

function extractItems(json) {
  if (!json || typeof json !== "object") return [];
  const candidates = [
    json.items,
    json.results,
    json.hits?.hits,
    json.data?.items,
    json.data?.results,
    Array.isArray(json) ? json : null,
  ].filter(Array.isArray);
  if (candidates.length > 0) return candidates[0];

  for (const v of Object.values(json)) {
    if (Array.isArray(v) && v.length && typeof v[0] === "object") return v;
  }
  return [];
}

async function fetchWithRetry(url, init, retries = MAX_RETRIES, delayMs = 800, timeoutMs = FETCH_TIMEOUT_MS) {
  let lastErr;
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      await rateLimit();
      const jitter = Math.random() * 150;
      if (jitter) await sleep(jitter);

      const ac = new AbortController();
      const t = setTimeout(() => ac.abort(), timeoutMs);

      const res = await fetch(url, { ...init, signal: ac.signal });
      clearTimeout(t);

      if (!res.ok) {
        if (res.status >= 500 || res.status === 429) {
          throw new Error(`HTTP ${res.status} ${res.statusText}`);
        } else {
          const text = await res.text().catch(() => "");
          throw new Error(`HTTP ${res.status}: ${res.statusText}\n${text.slice(0, 500)}`);
        }
      }
      return res;
    } catch (err) {
      lastErr = err;
      if (attempt < retries) {
        const backoff = delayMs * Math.pow(2, attempt);
        await sleep(backoff);
        continue;
      }
      throw lastErr;
    }
  }
}

// ---------------------- OUTPUT (NDJSON) ----------------------

async function ensureDir(dir) {
  await fs.mkdir(dir, { recursive: true });
}

async function appendNdjson(filePath, items) {
  if (!items || items.length === 0) return;
  const tmp = filePath + "." + Date.now() + ".tmp";
  const lines = items.map(o => JSON.stringify(o)).join("\n") + "\n";
  await fs.writeFile(tmp, lines, "utf-8");
  const data = await fs.readFile(tmp);
  await fs.appendFile(filePath, data);
  await fs.unlink(tmp);
}

// ---------------------- ID HANDLING (avoid duplicates if pagination shifts) ----------------------

function getItemId(it) {
  return it?.id ?? it?.uuid ?? it?.identifier ?? it?.escoIdentifier ?? it?.uri ?? null;
}

// ---------------------- PER-LEVEL FETCH (RESUMABLE) ----------------------

async function inferFromFromFile(filePath) {
  try {
    const buf = await fs.readFile(filePath, "utf-8");
    const lines = buf.split("\n").filter(Boolean).length;
    // Resume offset must be multiple of PAGE_SIZE
    return Math.floor(lines / PAGE_SIZE) * PAGE_SIZE;
  } catch {
    return 0;
  }
}

async function fetchAllFor(country, level, meta) {
  const entry = getMeta(meta, country, level);
  if (entry.completed) {
    console.log(`   ⏭️  Skip ${country} EQF ${level} (already completed)`);
    return entry;
  }

  await ensureDir(OUT_DIR);

  let from = entry.from || 0;

  // If resuming but file missing, restart level from 0 to avoid gaps
  try {
    await fs.access(entry.file);
  } catch {
    if (from > 0) {
      console.warn(`   ⚠️  ${entry.file} missing but resume 'from'=${from}. Restarting level from 0.`);
      from = 0;
      entry.from = 0;
      await saveMeta(meta);
    }
  }

  // Align 'from' with NDJSON if mismatch (e.g., after edits or PAGE_SIZE change)
  if (from > 0) {
    const inferred = await inferFromFromFile(entry.file);
    if (inferred !== from) {
      console.warn(`   ℹ️  Adjusting resume offset from ${from} -> ${inferred} based on file lines`);
      from = inferred;
      entry.from = inferred;
      await saveMeta(meta);
    }
  }

  const seen = new Set(); // prevent duplicate writes if API reorders

  while (true) {
    if (entry.totalPages >= MAX_PAGES_PER_LEVEL) {
      console.warn(`   ⛔ Max pages reached for ${country} EQF ${level}. Stopping.`);
      markComplete(entry);
      await saveMeta(meta);
      break;
    }

    const url = buildUrl(country, level, from);
    console.log(`   🌐 ${country} EQF ${level} — from=${from}`);
    const res = await fetchWithRetry(url, { method: "GET", headers });
    const text = await res.text();

    let json;
    try {
      json = JSON.parse(text);
    } catch {
      console.warn(`   ⚠️  Non-JSON for ${country} EQF ${level} @ from=${from}. Stopping this level.`);
      break;
    }

    const items = extractItems(json);
    const count = items.length;

    if (count === 0) {
      markComplete(entry);
      await saveMeta(meta);
      console.log(`   ✅ Completed ${country} EQF ${level} (pages=${entry.totalPages}, items=${entry.totalItems})`);
      break;
    }

    // De-dup if needed
    const pageIds = items.map(getItemId);
    const newItems = items.filter((_, i) => {
      const id = pageIds[i];
      if (!id) return true; // if no id field, allow
      if (seen.has(id)) return false;
      seen.add(id);
      return true;
    });

    if (SAVE_TO_DISK) {
      await appendNdjson(entry.file, newItems);
    }

    from += PAGE_SIZE;
    markUpdated(entry, { from, pageItems: newItems.length });
    await saveMeta(meta);
    noteProgress(); // heartbeat progress marker

    await sleep(150); // politeness
  }

  return entry;
}

// ---------------------- COUNTRY WORKER ----------------------

async function processCountry(country, meta) {
  console.log(`\n▶️  COUNTRY ${country} — starting`);
  for (const lvl of eqfLevel) {
    try {
      await fetchAllFor(country, lvl, meta);
    } catch (err) {
      console.error(`   ❌ Error ${country} EQF ${lvl}:`, err?.message || err);
      await saveMeta(meta); // persist progress even on error
    }
  }
  console.log(`✅ COUNTRY ${country} — done`);
}

// ---------------------- SIMPLE PROMISE POOL ----------------------

async function runPool(items, workerFn, concurrency) {
  const queue = [...items];
  const workers = Array.from({ length: concurrency }, async () => {
    while (queue.length) {
      const item = queue.shift();
      await workerFn(item);
    }
  });
  await Promise.all(workers);
}

// ---------------------- GRACEFUL SHUTDOWN ----------------------

function setupGraceful(metaRefFn) {
  const handler = async () => {
    console.log("\n🛑 Caught shutdown signal — saving meta...");
    try { await saveMeta(metaRefFn()); } finally { process.exit(0); }
  };
  process.on("SIGINT", handler);
  process.on("SIGTERM", handler);
}

// ---------------------- MAIN ----------------------

async function main() {
  let meta = await loadMeta();

  // Initialize meta scaffold for targeted countries
  for (const c of countries) {
    for (const lvl of eqfLevel) getMeta(meta, c, lvl);
  }
  await saveMeta(meta);

  setupGraceful(() => meta);

  const start = Date.now();

  await runPool(countries, async (c) => {
    // Reload meta just before processing each country to reduce stale reads
    meta = await loadMeta();
    await processCountry(c, meta);
  }, CONCURRENCY);

  // Final summary
  meta = await loadMeta();
  let totalItems = 0, completedLevels = 0, totalLevels = countries.length * eqfLevel.length;
  for (const c of countries) {
    for (const lvl of eqfLevel) {
      const e = getMeta(meta, c, lvl);
      totalItems += e.totalItems || 0;
      if (e.completed) completedLevels += 1;
    }
  }

  console.log("\n================ SUMMARY ================");
  console.log(`Levels completed: ${completedLevels}/${totalLevels}`);
  console.log(`Total items collected: ${totalItems}`);
  console.log(`Elapsed: ${Math.round((Date.now() - start)/1000)}s`);
}

main().catch(err => {
  console.error("Fatal error:", err);
  process.exit(1);
});
