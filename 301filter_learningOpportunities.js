/**
 * filter_complete_rows.js
 *
 * Reads learning_opportunities_output.csv
 * Writes learning_opportunities_output_filtered.csv
 * Keeps only rows where all required columns are non-empty.
 */

const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

const INPUT_CSV = path.resolve(__dirname, 'learning_opportunities_output.csv');
const OUTPUT_CSV = path.resolve(__dirname, 'learning_opportunities_output_filtered.csv');

const REQUIRED_COLS = [
  'title',
  'countryCode.prefLabel',
  'EQFLevel.prefLabel',
  'learningOutcomeSummary.noteLiteral',
  'learningOutcome'
];

function isNonEmpty(val) {
  if (val === null || val === undefined) return false;
  const s = String(val).trim();
  if (s === '' || s === '[]') return false;
  return true;
}

// --- Main ---
async function main() {
  console.log(`Reading from: ${INPUT_CSV}`);
  console.log(`Writing to:   ${OUTPUT_CSV}`);

  const output = fs.createWriteStream(OUTPUT_CSV, { encoding: 'utf8' });
  let headerWritten = false;
  let kept = 0;
  let total = 0;

  fs.createReadStream(INPUT_CSV)
    .pipe(csv())
    .on('headers', (headers) => {
      // Write header row once
      output.write(headers.join(',') + '\n');
      headerWritten = true;
    })
    .on('data', (row) => {
      total++;
      const ok = REQUIRED_COLS.every(col => isNonEmpty(row[col]));
      if (ok) {
        // Write row back as CSV line
        const line = Object.values(row).map(escapeForCsv).join(',');
        output.write(line + '\n');
        kept++;
      }
    })
    .on('end', () => {
      output.end();
      console.log(`Done. Total rows: ${total}, Kept: ${kept}, Dropped: ${total - kept}`);
    })
    .on('error', (err) => {
      console.error('Error reading CSV:', err);
    });
}

function escapeForCsv(value) {
  if (value === null || value === undefined) return '';
  const s = String(value);
  if (s.includes('"') || s.includes(',') || s.includes('\n') || s.includes('\r')) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

main();
