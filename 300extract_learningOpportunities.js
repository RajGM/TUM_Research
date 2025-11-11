/**
 * extract_lo_csv_concurrent.js
 *
 * Designed for large volumes (100k+) of JSON files.
 * - Controlled concurrency using a lightweight semaphore
 * - Streaming CSV output with backpressure handling
 * - Minimal memory footprint
 *
 * Edit INPUT_FOLDER, OUTPUT_CSV, and CONCURRENCY as desired, then run:
 *   node extract_lo_csv_concurrent.js
 */

const fs = require('fs');
const fsp = require('fs').promises;
const path = require('path');
const os = require('os');

// ---------- CONFIG ----------
const INPUT_FOLDER = path.resolve(__dirname, 'json_learningOpportunities'); // edit
const OUTPUT_CSV = path.resolve(__dirname, 'learning_opportunities_output.csv'); // edit
const CONCURRENCY = Math.max(4, os.cpus().length * 2); // tune this (lower if IO-bound on slow disks)
const LOG_EVERY = 5000; // progress log frequency
// ----------------------------

function escapeForCsv(value) {
  if (value === null || value === undefined) return '';
  const s = String(value);
  if (s.includes('"') || s.includes(',') || s.includes('\n') || s.includes('\r')) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

// simple semaphore for concurrency control
class Semaphore {
  constructor(max) {
    this.max = max;
    this.current = 0;
    this.waiters = [];
  }
  async acquire() {
    if (this.current < this.max) {
      this.current += 1;
      return;
    }
    await new Promise(resolve => this.waiters.push(resolve));
    this.current += 1;
  }
  release() {
    this.current -= 1;
    const next = this.waiters.shift();
    if (next) next();
  }
}

function firstCountryPrefLabelFromProvidedBy(obj) {
  if (!obj) return '';
  const pbArr = obj.providedBy;
  if (Array.isArray(pbArr)) {
    for (const pb of pbArr) {
      if (!pb) continue;
      const locs = pb.location;
      if (Array.isArray(locs)) {
        for (const loc of locs) {
          if (!loc) continue;
          const addrs = loc.address;
          if (Array.isArray(addrs)) {
            for (const a of addrs) {
              if (a?.countryCode?.prefLabel) return a.countryCode.prefLabel;
            }
          }
        }
      }
    }
  }
  const pub = obj.publisher;
  if (pub && Array.isArray(pub.location)) {
    for (const loc of pub.location) {
      const addrs = loc.address;
      if (Array.isArray(addrs)) {
        for (const a of addrs) {
          if (a?.countryCode?.prefLabel) return a.countryCode.prefLabel;
        }
      }
    }
  }
  return '';
}

function extractLearningOutcomeSummary(obj) {
  const las = obj.learningAchievementSpecification?.learningOutcomeSummary;
  return (las && typeof las.noteLiteral === 'string') ? las.noteLiteral.trim() : '';
}

function extractLearningOutcomeArray(obj) {
  const out = [];
  const los = obj.learningAchievementSpecification?.learningOutcome;
  if (!Array.isArray(los)) return out;
  for (const lo of los) {
    const title = lo?.title ?? '';
    const additionalNote = Array.isArray(lo?.additionalNote)
      ? lo.additionalNote.map(an => (an && typeof an.noteLiteral === 'string') ? an.noteLiteral : '').filter(Boolean)
      : [];
    out.push({ title, additionalNote });
  }
  return out;
}

async function getJsonFiles(folder) {
  const names = await fsp.readdir(folder);
  return names.filter(n => n.toLowerCase().endsWith('.json')).map(n => path.join(folder, n));
}

async function processOneFile(filePath) {
  const raw = await fsp.readFile(filePath, 'utf8');
  const obj = JSON.parse(raw);

  const title = obj.title ?? '';
  const country = firstCountryPrefLabelFromProvidedBy(obj) || '';
  const eqf = obj.learningAchievementSpecification?.EQFLevel?.prefLabel || obj.EQFLevel?.prefLabel || '';
  const learningOutcomeSummary = extractLearningOutcomeSummary(obj) || '';
  const learningOutcomeArr = extractLearningOutcomeArray(obj);

  const loJson = JSON.stringify(learningOutcomeArr || []);

  const row = [
    escapeForCsv(path.basename(filePath)),
    escapeForCsv(title),
    escapeForCsv(country),
    escapeForCsv(eqf),
    escapeForCsv(learningOutcomeSummary),
    escapeForCsv(loJson)
  ].join(',');

  return row;
}

async function writeLineWithBackpressure(stream, line) {
  if (!stream.write(line + '\n')) {
    await new Promise(resolve => stream.once('drain', resolve));
  }
}

(async function main() {
  try {
    const stat = await fsp.stat(INPUT_FOLDER);
    if (!stat.isDirectory()) {
      console.error('INPUT_FOLDER is not a directory:', INPUT_FOLDER);
      process.exit(1);
    }
  } catch (err) {
    console.error('Cannot access INPUT_FOLDER:', err.message);
    process.exit(1);
  }

  console.log('Scanning folder for .json files...');
  const files = await getJsonFiles(INPUT_FOLDER);
  console.log(`Found ${files.length} JSON files. Using concurrency = ${CONCURRENCY}`);

  // open write stream
  const outStream = fs.createWriteStream(OUTPUT_CSV, { encoding: 'utf8' });
  // write header
  const header = [
    'file',
    'title',
    'countryCode.prefLabel',
    'EQFLevel.prefLabel',
    'learningOutcomeSummary.noteLiteral',
    'learningOutcome'
  ].join(',');
  outStream.write(header + '\n');

  const sem = new Semaphore(CONCURRENCY);
  let processed = 0;
  let failed = 0;

  // Kick off tasks but limit concurrency manually
  const tasks = files.map(filePath => (async () => {
    await sem.acquire();
    try {
      const line = await processOneFile(filePath);
      await writeLineWithBackpressure(outStream, line);
      processed += 1;
      if (processed % LOG_EVERY === 0) {
        console.log(`Processed ${processed}/${files.length} files...`);
      }
    } catch (err) {
      failed += 1;
      console.error(`Error processing ${filePath}: ${err.message}`);
    } finally {
      sem.release();
    }
  })());

  // Wait for all tasks to finish
  await Promise.all(tasks);

  // close stream
  await new Promise((resolve, reject) => {
    outStream.end(() => resolve());
    outStream.on('error', reject);
  });

  console.log(`Done. Processed: ${processed}, Failed: ${failed}. Output: ${OUTPUT_CSV}`);
})().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
