/**
 * fetch_levels.js
 * CommonJS script — run: node fetch_levels.js
 *
 * Requirements:
 * - Node 18+ has global fetch. For Node <18 install node-fetch:
 *     npm install node-fetch
 *   The script will automatically use node-fetch if global fetch is not present.
 *
 * Behavior:
 * - iterates EQF levels 1..5
 * - paginates using `from = 0, SIZE, 2*SIZE, ...` until page returns no courses
 * - retries non-200 / network errors with exponential backoff
 * - appends deduplicated courses (by uri) to output/level{n}.ndjson
 * - maintains output/level{n}.index.json (uri list) and .progress.json (lastFrom,totalSaved)
 * - regenerates output/level{n}.json after each completed page (safe: waits for writes to finish)
 */

const fs = require("fs");
const path = require("path");
const readline = require("readline");
const { setTimeout: wait } = require("timers/promises");

// fetch: use global if available, otherwise fallback to node-fetch
let fetchFn = global.fetch;
if (!fetchFn) {
  try {
    // eslint-disable-next-line node/no-extraneous-require
    fetchFn = require("node-fetch");
  } catch (e) {
    console.error("No global fetch and node-fetch is not installed. Install with: npm install node-fetch");
    process.exit(1);
  }
}

const BASE_URL = "https://europa.eu/europass/eportfolio/api/qdr/europass/qdr-search/search";
const SIZE = 150;
const MAX_RETRIES = 6;
const INITIAL_BACKOFF_MS = 800;
const OUTPUT_DIR = path.resolve(process.cwd(), "output");

if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

/* ---------- helpers for paths ---------- */
const ndjsonPathFor = (level) => path.join(OUTPUT_DIR, `level${level}.ndjson`);
const jsonPathFor = (level) => path.join(OUTPUT_DIR, `level${level}.json`);
const progressPathFor = (level) => path.join(OUTPUT_DIR, `level${level}.progress.json`);
const indexPathFor = (level) => path.join(OUTPUT_DIR, `level${level}.index.json`);

function buildUrl(level, from) {
  const params = new URLSearchParams({
    keywords: "",
    size: String(SIZE),
    from: String(from),
    sortType: "publication date",
    language: "en",
    type: "learning-opportunity",
    eqfLevel: `http://data.europa.eu/snb/eqf/${level}`,
    version: "1.8",
  });
  return `${BASE_URL}?${params.toString()}`;
}

/* ---------- fetch with retry/backoff ---------- */
async function fetchPageWithRetry(url) {
  let attempt = 0;
  while (attempt < MAX_RETRIES) {
    try {
      const res = await fetchFn(url, { method: "GET", headers: { Accept: "application/json" } });
      if (res.status === 200) {
        const json = await res.json();
        return { ok: true, data: json, status: res.status };
      } else {
        attempt++;
        const backoff = INITIAL_BACKOFF_MS * 2 ** (attempt - 1);
        console.warn(`Non-200 (${res.status}) for ${url}. retry ${attempt}/${MAX_RETRIES} after ${backoff}ms`);
        await wait(backoff);
      }
    } catch (err) {
      attempt++;
      const backoff = INITIAL_BACKOFF_MS * 2 ** (attempt - 1);
      console.warn(`Error fetching ${url}: ${err}. retry ${attempt}/${MAX_RETRIES} after ${backoff}ms`);
      await wait(backoff);
    }
  }
  return { ok: false, error: `Failed after ${MAX_RETRIES} retries` };
}

/* ---------- index & progress helpers ---------- */
function loadIndex(level) {
  const p = indexPathFor(level);
  if (!fs.existsSync(p)) return new Set();
  try {
    const arr = JSON.parse(fs.readFileSync(p, "utf8"));
    return new Set(Array.isArray(arr) ? arr : []);
  } catch (e) {
    console.warn(`Couldn't parse index for level ${level}, starting fresh.`);
    return new Set();
  }
}

function saveIndex(level, uriSet) {
  const p = indexPathFor(level);
  fs.writeFileSync(p, JSON.stringify([...uriSet], null, 2));
}

function loadProgress(level) {
  const p = progressPathFor(level);
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch (e) {
    console.warn(`Couldn't parse progress for level ${level}, starting fresh.`);
    return null;
  }
}

function saveProgress(level, progressObj) {
  fs.writeFileSync(progressPathFor(level), JSON.stringify(progressObj, null, 2));
}

/* ---------- append NDJSON (awaitable) ---------- */
function appendUniqueCoursesToNdjson(level, courses, uriSet) {
  return new Promise((resolve, reject) => {
    if (!courses || courses.length === 0) return resolve(0);
    const ndp = ndjsonPathFor(level);
    const stream = fs.createWriteStream(ndp, { flags: "a" });
    let appended = 0;

    try {
      for (const c of courses) {
        const uri = c && c.uri;
        if (!uri) continue;
        if (uriSet.has(uri)) continue;
        uriSet.add(uri);
        if (!stream.write(JSON.stringify(c) + "\n")) {
          // write returned false -> node internal buffer; still OK
        }
        appended++;
      }
      stream.end();
      stream.on("finish", () => resolve(appended));
      stream.on("error", (err) => reject(err));
    } catch (err) {
      try { stream.destroy(); } catch (e) {}
      reject(err);
    }
  });
}

/* ---------- regenerate final JSON (awaits NDJSON reads and JSON write finish) ---------- */
async function regenerateFinalJson(level, lastFrom) {
  const ndp = ndjsonPathFor(level);
  const finalp = jsonPathFor(level);
  const metaPlaceholder = { lastFrom: lastFrom, totalSaved: 0 };

  if (!fs.existsSync(ndp)) {
    fs.writeFileSync(finalp, JSON.stringify({ metadata: metaPlaceholder, courses: [] }, null, 2));
    return;
  }

  const rl = readline.createInterface({
    input: fs.createReadStream(ndp),
    crlfDelay: Infinity,
  });

  const outStream = fs.createWriteStream(finalp, { flags: "w" });
  outStream.write('{"metadata": ' + JSON.stringify(metaPlaceholder) + ', "courses": [');

  let first = true;
  let count = 0;
  for await (const line of rl) {
    const l = line.trim();
    if (!l) continue;
    if (!first) outStream.write(",");
    outStream.write(l);
    first = false;
    count++;
  }

  outStream.write("]}");

  // Wait for full flush of the final JSON file before reading/updating metadata
  await new Promise((resolve, reject) => {
    outStream.end();
    outStream.on("finish", resolve);
    outStream.on("error", reject);
  });

  // Read, update metadata with accurate totalSaved, and rewrite (small operation; metadata small)
  const raw = fs.readFileSync(finalp, "utf8");
  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch (err) {
    console.error("Failed to parse final JSON even after waiting for finish:", err);
    throw err;
  }
  parsed.metadata = { lastFrom: lastFrom, totalSaved: count };
  fs.writeFileSync(finalp, JSON.stringify(parsed, null, 2));
}

/* ---------- process a single level ---------- */
async function processLevel(level) {
  console.log(`\n=== START level ${level} ===`);
  const uriSet = loadIndex(level);
  const prog = loadProgress(level);

  let from = 0;
  let totalSaved = uriSet.size || 0;

  if (prog && Number.isInteger(prog.lastFrom)) {
    // resume from next page (we assume lastFrom is last fully saved page)
    from = prog.lastFrom + SIZE;
    totalSaved = prog.totalSaved ?? totalSaved;
    console.log(`Resuming level ${level} from=${from} (saved=${totalSaved})`);
  } else {
    console.log(`Starting level ${level} from=0`);
  }

  while (true) {
    const url = buildUrl(level, from);
    console.log(`Fetching level ${level} from=${from} ...`);
    const result = await fetchPageWithRetry(url);
    if (!result.ok) {
      console.error(`Failed to fetch page for level ${level} from=${from}: ${result.error}`);
      // Save progress so we can resume next run
      saveProgress(level, { lastFrom: Math.max(0, from - SIZE), totalSaved });
      saveIndex(level, uriSet);
      console.log(`Progress saved for level ${level}. Re-run to resume.`);
      break;
    }

    const data = result.data;
    const courses = Array.isArray(data.courses) ? data.courses : null;
    const pagination = data.paginationInfos ?? null;

    if (!Array.isArray(courses)) {
      console.warn(`Unexpected response format at from=${from} for level ${level}. Aborting this level.`);
      saveProgress(level, { lastFrom: from, totalSaved });
      saveIndex(level, uriSet);
      break;
    }

    if (courses.length === 0) {
      console.log(`No more courses for level ${level} at from=${from} (empty page). Finalizing...`);
      const lastFrom = Math.max(0, from - SIZE);
      await regenerateFinalJson(level, lastFrom);
      saveProgress(level, { lastFrom: lastFrom, totalSaved });
      saveIndex(level, uriSet);
      break;
    }

    // Append unique courses to NDJSON and wait until write finishes
    const appended = await appendUniqueCoursesToNdjson(level, courses, uriSet);
    totalSaved += appended;

    // Save index & progress immediately after success
    saveIndex(level, uriSet);
    saveProgress(level, { lastFrom: from, totalSaved });

    // Regenerate final JSON (waits for writes to finish internally)
    await regenerateFinalJson(level, from);

    console.log(`Level ${level} page from=${from} appended ${appended} new items (totalSaved=${totalSaved}).`);

    // If paginationInfos indicates last page, finalize and break
    if (pagination && Number.isInteger(pagination.currentPageNumber) && Number.isInteger(pagination.totalPageCount)) {
      if (pagination.currentPageNumber >= pagination.totalPageCount) {
        console.log(`Reached last page according to paginationInfos for level ${level}. Finalizing...`);
        const lastFrom = from;
        await regenerateFinalJson(level, lastFrom);
        saveProgress(level, { lastFrom, totalSaved });
        saveIndex(level, uriSet);
        break;
      }
    }

    // move to next page
    from += SIZE;
  }

  console.log(`=== DONE level ${level} (totalSaved=${totalSaved}) ===`);
}

/* ---------- main: sequentially process levels 1..5 ---------- */
async function main() {
  for (let level = 1; level <= 8; level++) {
    try {
      await processLevel(level);
    } catch (err) {
      console.error(`Unexpected error processing level ${level}:`, err);
    }
  }
  console.log("\nAll levels processed (or paused on error).");
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
