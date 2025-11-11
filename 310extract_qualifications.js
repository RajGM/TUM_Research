// extract_all_to_csv.js
// Usage: node extract_all_to_csv.js /path/to/json/folder
// Writes output_all.csv in the current working directory
// Dependency-free; uses streams + a simple concurrency limiter.

const fs = require('fs');
const fsp = require('fs').promises;
const path = require('path');

const OUTPUT_CSV = 'output_all.csv';
const CONCURRENCY = 50; // adjust to your machine (e.g. 10-200)

// CSV escaping helper
function escapeCsv(value) {
  if (value === null || value === undefined) return '""';
  if (typeof value === 'object') value = JSON.stringify(value);
  const s = String(value).replace(/"/g, '""');
  return `"${s}"`;
}

// safe getter for nested paths
function safeGet(obj, pathArray) {
  let cur = obj;
  for (const p of pathArray) {
    if (!cur) return undefined;
    cur = cur[p];
  }
  return cur;
}

function extractLearningOutcomes(json) {
  const los = json.learningOutcome;
  if (!Array.isArray(los)) return [];
  return los.map(lo => {
    const title = lo.title || '';
    const addNotes = Array.isArray(lo.additionalNote)
      ? lo.additionalNote.map(n => n.noteLiteral || '').filter(Boolean)
      : [];
    return { title, additionalNotes: addNotes.join('\n\n') };
  });
}

// Extract single file -> object for CSV
async function processFile(filePath) {
  try {
    const txt = await fsp.readFile(filePath, 'utf8');
    const json = JSON.parse(txt);

    const qualificationName = json.title || '';

    // publisher -> location[0] -> address[0] -> countryCode.prefLabel
    const country = safeGet(json, ['publisher', 'location', 0, 'address', 0, 'countryCode', 'prefLabel']) || '';

    const qualificationLevel = safeGet(json, ['EQFLevel', 'prefLabel']) || '';

    const description = safeGet(json, ['learningOutcomeSummary', 'noteLiteral']) || '';

    const entryRequirement = safeGet(json, ['entryRequirement', 'noteLiteral']) || '';

    const learningOutcomes = extractLearningOutcomes(json);

    return {
      qualificationName,
      country,
      qualificationLevel,
      description,
      learningOutcomes,
      entryRequirement
    };
  } catch (err) {
    // Return error marker so caller can log
    return { __error: true, filePath, message: err.message };
  }
}

// Recursively collect all .json files (async)
async function collectJsonFiles(dir) {
  const results = [];
  async function walk(current) {
    const entries = await fsp.readdir(current, { withFileTypes: true });
    for (const ent of entries) {
      const full = path.join(current, ent.name);
      if (ent.isDirectory()) {
        await walk(full);
      } else if (ent.isFile() && ent.name.toLowerCase().endsWith('.json')) {
        results.push(full);
      }
    }
  }
  await walk(dir);
  return results;
}

// Simple concurrency pool for async tasks
function runWithConcurrency(tasks, limit) {
  return new Promise((resolve) => {
    const results = new Array(tasks.length);
    let i = 0;
    let running = 0;
    let finished = 0;

    function runNext() {
      if (finished === tasks.length) return resolve(results);
      while (running < limit && i < tasks.length) {
        const idx = i++;
        running++;
        tasks[idx]()
          .then(res => { results[idx] = res; })
          .catch(err => { results[idx] = { __error:true, message: err.message }; })
          .finally(() => {
            running--;
            finished++;
            runNext();
          });
      }
    }
    runNext();
  });
}

async function main() {
  const folder = process.argv[2] || '.';
  const absFolder = path.resolve(folder);

  // Create write stream for CSV and write header
  const ws = fs.createWriteStream(OUTPUT_CSV, { encoding: 'utf8' });
  const headers = [
    'qualificationName',
    'country',
    'qualificationLevel',
    'description',
    'learningOutcomes',
    'entryRequirement',
    'sourceFile'
  ];
  ws.write(headers.join(',') + '\n');

  try {
    console.log('Scanning for JSON files in', absFolder);
    const filePaths = await collectJsonFiles(absFolder);
    console.log(`Found ${filePaths.length} JSON files.`);

    if (filePaths.length === 0) {
      ws.end();
      console.log('No files to process. Exiting.');
      return;
    }

    // Build tasks array
    const tasks = filePaths.map(fp => async () => {
      const res = await processFile(fp);
      return { res, fp };
    });

    console.log(`Processing files with concurrency=${CONCURRENCY}...`);

    const startTime = Date.now();
    // We will use runWithConcurrency to control concurrency, but stream rows as results come back
    // To avoid holding all results in memory, process in batches of size CONCURRENCY:
    // We'll create chunks of tasks and run each chunk with concurrency limit equal to chunk size.
    // Simpler: runWithConcurrency returns array in original order; we will write as results are ready.
    const results = await runWithConcurrency(tasks, CONCURRENCY);

    let successCount = 0;
    let errorCount = 0;

    for (const item of results) {
      if (!item) continue;
      const { res, fp } = item;
      if (!res) continue;
      if (res.__error) {
        errorCount++;
        console.error(`Error: ${res.filePath || fp} -> ${res.message || res}`);
        continue;
      }

      // Prepare CSV row
      const loForCsv = res.learningOutcomes.map(lo => ({ title: lo.title, additionalNotes: lo.additionalNotes }));
      const cells = [
        escapeCsv(res.qualificationName),
        escapeCsv(res.country),
        escapeCsv(res.qualificationLevel),
        escapeCsv(res.description),
        escapeCsv(loForCsv),
        //escapeCsv(res.entryRequirement),
        escapeCsv(fp)
      ];
      ws.write(cells.join(',') + '\n');
      successCount++;
    }

    const elapsed = (Date.now() - startTime) / 1000;
    ws.end();
    console.log(`Done. Processed ${successCount} files successfully, ${errorCount} errors. Elapsed ${elapsed.toFixed(1)}s`);
    console.log(`CSV written to ${path.resolve(OUTPUT_CSV)}`);
  } catch (err) {
    ws.end();
    console.error('Fatal error:', err);
  }
}

main();
