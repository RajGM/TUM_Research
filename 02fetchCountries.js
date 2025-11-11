// fetch_qualification_details.js
// countryFiles/*.ndjson -> fetch each "uri" -> qualificationData/<COUNTRY>/<ID>.json
// Resume-safe per-country metadata. 100 concurrent requests. Node 18+ required.
// Milestones: log after every +100 successes or +100 failures; show last 100 of each.

const fs = require("fs");
const fsp = fs.promises;
const path = require("path");
const readline = require("readline");

const IN_DIR = path.resolve(__dirname, "countryFiles");
const OUT_BASE = path.resolve(__dirname, "qualificationData");
const META_DIR = path.resolve(__dirname, "meta");

const CONCURRENCY = 100;
const MAX_RETRIES = 3;
const TIMEOUT_MS = 30_000;
const MAX_RPS = 20;

let tokens = MAX_RPS;
setInterval(() => { tokens = Math.min(tokens + MAX_RPS, MAX_RPS); }, 1000);
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
async function rateLimit() { while (tokens <= 0) await sleep(25); tokens--; }

function pushRing(arr, item, cap = 100) {
  arr.push(item);
  if (arr.length > cap) arr.shift();
}

function parseIdFromUri(uri) {
  try {
    const u = new URL(uri);
    const parts = u.pathname.split("/").filter(Boolean);
    return parts[parts.length - 1] || null;
  } catch {
    const m = String(uri).match(/([^/]+)\/?$/);
    return m ? m[1] : null;
  }
}

async function ensureDir(dir) {
  await fsp.mkdir(dir, { recursive: true });
}

async function fetchWithRetry(url, retries = MAX_RETRIES, timeoutMs = TIMEOUT_MS) {
  let lastErr;
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      await rateLimit();
      const ac = new AbortController();
      const t = setTimeout(() => ac.abort(), timeoutMs);

      const res = await fetch(url, {
        method: "GET",
        headers: {
          "Accept": "application/json, text/plain, */*",
          "Host": "europa.eu",
          "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Mobile Safari/537.36",
        },
        signal: ac.signal,
      });
      clearTimeout(t);

      if (!res.ok) {
        if (res.status >= 500 || res.status === 429) throw new Error(`HTTP ${res.status}`);
        const txt = await res.text().catch(() => "");
        throw new Error(`HTTP ${res.status}: ${res.statusText} ${txt.slice(0,200)}`);
      }

      const ct = res.headers.get("content-type") || "";
      if (ct.includes("json")) return await res.json();

      const txt = await res.text();
      try { return JSON.parse(txt); } catch { return { _raw: txt }; }
    } catch (err) {
      lastErr = err;
      if (attempt < retries) {
        await sleep(400 * Math.pow(2, attempt));
        continue;
      }
      throw lastErr;
    }
  }
}

function listCountryFiles() {
  if (!fs.existsSync(IN_DIR)) return [];
  return fs.readdirSync(IN_DIR)
    .filter(f => f.toLowerCase().endsWith(".ndjson"))
    .map(f => path.join(IN_DIR, f))
    .sort();
}

function getCountryFromFile(filePath) {
  return path.basename(filePath, path.extname(filePath)).toUpperCase();
}

async function loadCountryMeta(country) {
  await ensureDir(META_DIR);
  const metaPath = path.join(META_DIR, `${country}_meta.json`);
  try {
    const data = await fsp.readFile(metaPath, "utf-8");
    return { metaPath, meta: JSON.parse(data) };
  } catch {
    const meta = {
      country,
      lastLine: 0,
      attempted: 0,
      succeeded: 0,
      failed: 0,
      skippedExisting: 0,
      lastUpdated: null
    };
    return { metaPath, meta };
  }
}

async function saveCountryMeta(metaPath, meta) {
  const tmp = `${metaPath}.tmp.${process.pid}.${Date.now()}.${Math.random().toString(36).slice(2)}`;
  await fsp.writeFile(tmp, JSON.stringify(meta, null, 2), "utf-8");
  for (let i = 0; i < 5; i++) {
    try { await fsp.rename(tmp, metaPath); return; }
    catch { await sleep(100 * (i + 1)); }
  }
  try { await fsp.unlink(tmp); } catch {}
}

/** Async generator: yields { lineNo, obj } for each JSON line */
async function* readNdjsonWithLineNumbers(filePath, startLineExclusive = 0) {
  const rl = readline.createInterface({
    input: fs.createReadStream(filePath, { encoding: "utf-8" }),
    crlfDelay: Infinity,
  });
  let lineNo = 0;
  for await (const line of rl) {
    lineNo++;
    if (lineNo <= startLineExclusive) continue;
    const trimmed = line.trim();
    if (!trimmed) continue;
    let obj = null;
    try { obj = JSON.parse(trimmed); } catch { obj = null; }
    yield { lineNo, obj };
  }
}

function milestoneReached(current, lastLogged) {
  // return true when we crossed another 100 boundary (100, 200, 300, ...)
  return Math.floor(current / 100) > Math.floor(lastLogged / 100);
}

function logMilestone(country, meta, last100Success, last100Fail, label) {
  console.log(`\n🏁 [${country}] Milestone ${label}`);
  console.log(`   attempted=${meta.attempted}  succeeded=${meta.succeeded}  failed=${meta.failed}  skipped=${meta.skippedExisting}  lastLine=${meta.lastLine}`);

  if (last100Success.length) {
    console.log(`   Last ${last100Success.length} successes:`);
    for (const s of last100Success) {
      console.log(`     ✔ line ${s.lineNo} id=${s.id}`);
    }
  }
  if (last100Fail.length) {
    console.log(`   Last ${last100Fail.length} failures:`);
    for (const f of last100Fail) {
      console.log(`     ✖ line ${f.lineNo} id=${f.id} err=${f.err}`);
    }
  }
}

async function processCountryFile(filePath) {
  const country = getCountryFromFile(filePath);
  const { metaPath, meta } = await loadCountryMeta(country);
  const outDir = path.join(OUT_BASE, country);
  await ensureDir(outDir);

  console.log(`\n▶️  Processing ${country} (resume from line ${meta.lastLine})`);

  let active = 0;
  let dirty = false;

  // ring buffers for last 100 successes/failures
  const last100Success = [];
  const last100Fail = [];

  // milestone tracking
  let lastLoggedSuccess = meta.succeeded || 0;
  let lastLoggedFail = meta.failed || 0;

  const flushTimer = setInterval(async () => {
    if (!dirty) return;
    meta.lastUpdated = new Date().toISOString();
    await saveCountryMeta(metaPath, meta);
    dirty = false;
  }, 3000);

  const submit = async (fn) => {
    active++;
    fn().finally(() => active--);
  };
  const drain = async () => { while (active >= CONCURRENCY) await sleep(10); };

  for await (const { lineNo, obj } of readNdjsonWithLineNumbers(filePath, meta.lastLine)) {
    meta.lastLine = lineNo;
    dirty = true;

    const uri = obj?.uri;
    if (!uri) {
      meta.failed++;
      pushRing(last100Fail, { lineNo, id: "-", err: "no uri" });
      if (milestoneReached(meta.failed, lastLoggedFail)) {
        lastLoggedFail = meta.failed;
        logMilestone(country, meta, last100Success, last100Fail, "failure +100");
        meta.lastUpdated = new Date().toISOString();
        await saveCountryMeta(metaPath, meta);
        dirty = false;
      }
      continue;
    }

    const id = parseIdFromUri(uri);
    if (!id) {
      meta.failed++;
      pushRing(last100Fail, { lineNo, id: "-", err: "bad id" });
      if (milestoneReached(meta.failed, lastLoggedFail)) {
        lastLoggedFail = meta.failed;
        logMilestone(country, meta, last100Success, last100Fail, "failure +100");
        meta.lastUpdated = new Date().toISOString();
        await saveCountryMeta(metaPath, meta);
        dirty = false;
      }
      continue;
    }

    const outPath = path.join(outDir, `${id}.json`);
    if (fs.existsSync(outPath)) {
      meta.skippedExisting++;
      continue;
    }

    const task = async () => {
      try {
        meta.attempted++;
        const data = await fetchWithRetry(uri);
        await fsp.writeFile(outPath, JSON.stringify(data, null, 2), "utf-8");
        meta.succeeded++;
        pushRing(last100Success, { lineNo, id });

        // success milestone?
        if (milestoneReached(meta.succeeded, lastLoggedSuccess)) {
          lastLoggedSuccess = meta.succeeded;
          logMilestone(country, meta, last100Success, last100Fail, "success +100");
          meta.lastUpdated = new Date().toISOString();
          await saveCountryMeta(metaPath, meta); // immediate meta persist
          dirty = false;
        }
      } catch (e) {
        meta.failed++;
        pushRing(last100Fail, { lineNo, id, err: e?.message || String(e) });

        // failure milestone?
        if (milestoneReached(meta.failed, lastLoggedFail)) {
          lastLoggedFail = meta.failed;
          logMilestone(country, meta, last100Success, last100Fail, "failure +100");
          meta.lastUpdated = new Date().toISOString();
          await saveCountryMeta(metaPath, meta); // immediate meta persist
          dirty = false;
        }
      } finally {
        dirty = true; // mark that counters changed
      }
    };

    await drain();
    submit(task);

    if (lineNo % 200 === 0) await sleep(1); // yield
  }

  while (active > 0) await sleep(50);
  clearInterval(flushTimer);

  meta.lastUpdated = new Date().toISOString();
  await saveCountryMeta(metaPath, meta);

  console.log(`✅ Done ${country}: attempted=${meta.attempted}, succeeded=${meta.succeeded}, failed=${meta.failed}, skipped=${meta.skippedExisting}, lastLine=${meta.lastLine}`);
}

(async function main() {
  await ensureDir(OUT_BASE);
  await ensureDir(META_DIR);
  const files = listCountryFiles();
  if (!files.length) {
    console.log("No .ndjson files found in countryFiles/");
    process.exit(0);
  }

  // process one country file at a time
  for (const file of files) {
    try {
      await processCountryFile(file);
    } catch (err) {
      console.error(`❌ Error in ${path.basename(file)}:`, err?.message || err);
    }
  }

  console.log("\n🎉 All country files processed.");
})();
