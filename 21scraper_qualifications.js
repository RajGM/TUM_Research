/**
 * scrape-learning-opportunities-batch.js
 *
 * - Reads level1index.json ... level8index.json from inputDir (default: output_learningOpportunities)
 * - Each file contains a big array of URLs like:
 *     "http://data.europa.eu/snb/data/learningOpportunity/<UUID>",
 * - For each URL, call the QDR API:
 *     https://europa.eu/europass/eportfolio/api/qdr/europass/qdr-search/learning-opportunity?uri=<encoded>&language=en&version=1.8
 * - Save the returned JSON to outputDir/json_learningOpportunities/<UUID>.json
 *
 * Usage:
 *   npm install axios
 *   node scrape-learning-opportunities-batch.js
 *
 * Configuration at top of file.
 */

const fs = require('fs');
const fsp = fs.promises;
const path = require('path');
const axios = require('axios');

//
// CONFIG
//
const inputDir = path.join(process.cwd(), 'output_qualifications'); // folder with levelXindex.json
const outputDir = path.join(process.cwd(), 'json_qualifications'); // where outputs will be saved
const errorsDir = path.join(process.cwd(), 'json_qualifications_errors'); // per-URL error dumps
const indexFiles = Array.from({ length: 8 }, (_, i) => `level${i + 1}.index.json`); // level1index.json .. level8index.json
const concurrency = 100;      // number of concurrent requests
const maxRetries = 3;        // attempts per URL
const requestTimeout = 30000; // ms
const language = 'en';
const version = '1.8';

//
// utils
//
function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
}

function buildApiUrlFromLoUri(loUri) {
  const base = 'https://europa.eu/europass/eportfolio/api/qdr/europass/qdr-search/qualification';
  const encoded = encodeURIComponent(loUri);
  return `${base}?uri=${encoded}&language=${encodeURIComponent(language)}&version=${encodeURIComponent(version)}`;
}

function extractUuidFromLoUrl(url) {
  if (!url || typeof url !== 'string') return null;
  const trimmed = url.replace(/\/+$/, '');
  const last = trimmed.split('/').pop();
  // simple validation: uuid-like string has hyphens
  return last || null;
}

function safeFilename(name) {
  return name.replace(/[^\w.-]/g, '_');
}

async function ensureDirs() {
  await fsp.mkdir(outputDir, { recursive: true });
  await fsp.mkdir(errorsDir, { recursive: true });
}

async function readIndexFile(filePath) {
  const raw = await fsp.readFile(filePath, 'utf-8');
  const parsed = JSON.parse(raw);
  if (!Array.isArray(parsed)) {
    throw new Error(`Index file ${filePath} did not contain an array`);
  }
  return parsed;
}

async function writeJsonFile(filePath, obj) {
  const tmp = `${filePath}.tmp-${Date.now()}`;
  await fsp.writeFile(tmp, JSON.stringify(obj, null, 2), 'utf-8');
  await fsp.rename(tmp, filePath);
}

async function fetchApiJson(apiUrl, attempt = 1) {
  try {
    const resp = await axios.get(apiUrl, {
      responseType: 'json',
      timeout: requestTimeout,
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; fetch-lo-batch/1.0)',
        'Accept': 'application/json, */*;q=0.1',
        'Referer': 'http://data.europa.eu/'
      },
      validateStatus: (s) => s >= 200 && s < 500
    });
    return { status: resp.status, data: resp.data };
  } catch (err) {
    if (attempt < maxRetries) {
      const backoff = 1000 * Math.pow(2, attempt - 1);
      console.warn(`Request failed (attempt ${attempt}) -> retrying after ${backoff}ms: ${apiUrl}`);
      await sleep(backoff);
      return fetchApiJson(apiUrl, attempt + 1);
    }
    // final failure
    throw err;
  }
}

/**
 * Simple async pool implementation:
 * items: array of inputs
 * worker: async function(item) -> result
 * concurrency: number
 */
async function asyncPool(items, worker, concurrency) {
  const results = [];
  const executing = new Set();

  for (const item of items) {
    const p = Promise.resolve().then(() => worker(item));
    results.push(p);

    executing.add(p);

    const remove = () => executing.delete(p);
    p.then(remove).catch(remove);

    if (executing.size >= concurrency) {
      // wait for any to finish
      await Promise.race(executing);
    }
  }
  return Promise.all(results);
}

//
// Main
//
(async () => {
  console.log('Starting batch scrape of learning opportunities');
  console.log('Input directory:', inputDir);
  console.log('Output directory:', outputDir);
  console.log('Errors directory:', errorsDir);
  console.log('Index files to process:', indexFiles.join(', '));
  console.log('Concurrency:', concurrency, 'Max retries:', maxRetries);

  await ensureDirs();

  // Load all URLs from all index files (streaming file-by-file to avoid too-large memory spike)
  let totalUrls = 0;
  for (const idxFile of indexFiles) {
    const fullPath = path.join(inputDir, idxFile);
    if (!(await fsp.stat(fullPath).catch(() => false))) {
      console.warn(`Index file not found, skipping: ${fullPath}`);
      continue;
    }

    console.log('Reading index file:', fullPath);
    let arr;
    try {
      arr = await readIndexFile(fullPath);
    } catch (err) {
      console.error(`Failed reading/parsing ${fullPath}:`, err.message);
      continue;
    }

    console.log(`Found ${arr.length} URLs in ${idxFile}`);
    totalUrls += arr.length;

    // worker for a single URL
    const worker = async (loUrl) => {
      if (!loUrl || typeof loUrl !== 'string') return { url: loUrl, skipped: true, reason: 'invalid-url' };

      const uuid = extractUuidFromLoUrl(loUrl);
      if (!uuid) {
        const note = `Could not extract UUID from URL: ${loUrl}`;
        console.warn(note);
        // save this in error folder for manual inspection
        await fsp.writeFile(path.join(errorsDir, `bad-url-${Date.now()}.txt`), loUrl, 'utf-8').catch(() => {});
        return { url: loUrl, skipped: true, reason: 'bad-url' };
      }

      const outFileName = safeFilename(uuid) + '.json';
      const outPath = path.join(outputDir, outFileName);

      // skip if already exists
      if (await fsp.stat(outPath).catch(() => false)) {
        return { url: loUrl, uuid, skipped: true, reason: 'exists' };
      }

      const apiUrl = buildApiUrlFromLoUri(loUrl);

      try {
        const res = await fetchApiJson(apiUrl, 1);
        // if status is not 200, still save body for inspection
        if (res && (res.status === 200 || res.status === 201)) {
          await writeJsonFile(outPath, res.data);
          return { url: loUrl, uuid, saved: true, file: outPath };
        } else {
          // non-200 (e.g., 404, 403) - save whatever returned
          const errObj = {
            status: res ? res.status : 'noresponse',
            url: apiUrl,
            body: res ? res.data : null
          };
          const errFile = path.join(errorsDir, `${safeFilename(uuid)}.error.json`);
          await writeJsonFile(errFile, errObj);
          return { url: loUrl, uuid, saved: false, reason: `status-${res ? res.status : 'noresp'}`, errorFile: errFile };
        }
      } catch (err) {
        // final failure after retries - dump error and response if available
        const eObj = {
          url: apiUrl,
          message: err.message,
          stack: err.stack || null,
          code: err.code || null,
          responseStatus: err.response && err.response.status ? err.response.status : null,
          responseBody: err.response && err.response.data ? err.response.data : null
        };
        const errFile = path.join(errorsDir, `${safeFilename(uuid)}.error.json`);
        try { await writeJsonFile(errFile, eObj); } catch (writeErr) { console.error('Failed to write error file', writeErr); }
        console.error(`Failed url: ${loUrl} -> saved error to ${errFile}`);
        return { url: loUrl, uuid, saved: false, reason: 'exception', errorFile: errFile };
      }
    };

    // run worker in limited-concurrency pool for this index file
    console.log(`Starting processing of ${idxFile} with concurrency ${concurrency} ...`);
    const startTime = Date.now();
    await asyncPool(arr, worker, concurrency)
      .then((results) => {
        const saved = results.filter((r) => r && r.saved).length;
        const skipped = results.filter((r) => r && r.skipped).length;
        const failed = results.filter((r) => r && r.saved === false && !r.skipped).length;
        console.log(`Finished ${idxFile}: saved=${saved}, skipped=${skipped}, failed=${failed}`);
      })
      .catch((e) => {
        console.error('Unexpected error while processing pool for', idxFile, e && e.message ? e.message : e);
      });
    const elapsed = (Date.now() - startTime) / 1000;
    console.log(`Processed ${idxFile} in ${elapsed.toFixed(1)}s`);
  }

  console.log(`All index files processed. Total URLs discovered (approx): ${totalUrls}`);
  console.log('Done.');
})();
