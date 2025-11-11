// filter_and_shard_csv_drop_entry_autoshard.js
// Usage:
//   node filter_and_shard_csv_drop_entry_autoshard.js [inputCsvPath] [outputPrefix] [rowsPerFile]
// Examples:
//   node filter_and_shard_csv_drop_entry_autoshard.js output_all.csv filtered   # single output file (no sharding)
//   node filter_and_shard_csv_drop_entry_autoshard.js output_all.csv filtered 10000  # shard every 10k rows

const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

const inputPath = process.argv[2] || 'output_all.csv';
const outputPrefix = process.argv[3] || 'filtered';
const rowsPerFileArg = process.argv[4];

// Interpret rowsPerFile:
// - if omitted, or <= 0 => no sharding (single output file)
// - if > 0 => shard with that many rows per file
const rowsPerFile = rowsPerFileArg ? parseInt(rowsPerFileArg, 10) : null;
const doSharding = Number.isInteger(rowsPerFile) && rowsPerFile > 0;

if (!fs.existsSync(inputPath)) {
  console.error('Input file not found:', inputPath);
  process.exit(1);
}

// Utility to decide if a value counts as "empty"
function isEmptyVal(v) {
  if (v === undefined || v === null) return true;
  const s = String(v).trim();
  if (s === '') return true;
  if (s === '[]') return true;
  if (s.toLowerCase && s.toLowerCase() === 'null') return true;
  return false;
}

// CSV escape: double quotes and wrap
function escapeCsvCell(value) {
  if (value === undefined || value === null) return '""';
  if (typeof value === 'object') {
    try { value = JSON.stringify(value); } catch { value = String(value); }
  }
  const s = String(value).replace(/"/g, '""');
  return `"${s}"`;
}

let outHeaders = null;
let writer = null;
let fileIndex = 1;
let rowsInCurrentFile = 0;
let totalMatched = 0;
let totalSeen = 0;
let outputFiles = [];

function getOutputFilename(idx) {
  return doSharding
    ? `${outputPrefix}_${idx}.csv`
    : `${outputPrefix}.csv`;
}

function openNewWriter() {
  if (writer) writer.end();
  const outFile = getOutputFilename(fileIndex);
  writer = fs.createWriteStream(outFile, { encoding: 'utf8' });
  // write header
  writer.write(outHeaders.map(h => escapeCsvCell(h)).join(',') + '\n');
  if (!outputFiles.includes(outFile)) outputFiles.push(outFile);
  console.log(`→ Opened ${outFile}`);
  rowsInCurrentFile = 0;
  if (doSharding) fileIndex++;
}

function closeWriter() {
  if (writer) {
    writer.end();
    writer = null;
  }
}

const readStream = fs.createReadStream(inputPath);
readStream
  .pipe(csv())
  .on('headers', (hdrs) => {
    // drop 'sourceFile' and 'entryRequirement' from output headers
    outHeaders = hdrs.filter(h => h !== 'sourceFile' && h !== 'entryRequirement');

    // Ensure required columns exist (warn if missing)
    const required = ['qualificationName', 'qualificationLevel', 'description', 'learningOutcomes', 'country'];
    const missing = required.filter(r => !hdrs.includes(r));
    if (missing.length) {
      console.warn('Warning: input CSV missing expected columns:', missing.join(', '));
      console.warn('The script will still run but may filter out rows because columns are absent.');
    }

    // Always open writer once headers are known
    openNewWriter();
  })
  .on('data', (row) => {
    totalSeen++;
    // Filter condition: all five fields present and non-empty
    const pass =
      !isEmptyVal(row.qualificationName) &&
      !isEmptyVal(row.qualificationLevel) &&
      !isEmptyVal(row.description) &&
      !isEmptyVal(row.learningOutcomes) &&
      !isEmptyVal(row.country);

    if (!pass) return;

    // Build output row without sourceFile and without entryRequirement
    const outRow = outHeaders.map(h => {
      return escapeCsvCell(row.hasOwnProperty(h) ? row[h] : '');
    }).join(',');

    writer.write(outRow + '\n');
    rowsInCurrentFile++;
    totalMatched++;

    // rotate file if sharding is enabled and threshold reached
    if (doSharding && rowsInCurrentFile >= rowsPerFile) {
      openNewWriter();
    }

    // occasional progress log
    if (totalMatched % 1000 === 0) {
      process.stdout.write(`\rMatched rows: ${totalMatched} (seen ${totalSeen})`);
    }
  })
  .on('end', () => {
    closeWriter();
    console.log('\nDone.');
    console.log(`Total rows scanned: ${totalSeen}`);
    console.log(`Total matched and written: ${totalMatched}`);
    console.log(`Output files: ${outputFiles.length > 0 ? outputFiles.join(', ') : 'none'}`);
  })
  .on('error', (err) => {
    console.error('Error while reading CSV:', err);
    closeWriter();
  });
