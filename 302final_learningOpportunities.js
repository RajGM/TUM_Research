/**
 * transform_lo_csv.js
 *
 * Reads:  learning_opportunities_output.csv
 * Writes: learning_opportunities_transformed.csv
 *
 * Produces columns (order):
 *   title,
 *   countryCode.prefLabel,
 *   EQFLevel_prefLabel,
 *   EQFLevel_numeric,            <- new (only X)
 *   learningOutcomeSummary.noteLiteral,
 *   learningOutcome_additionalNote  <- concatenated plain text of all additionalNote entries
 *
 * Omits: file
 */

const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

const INPUT_CSV = path.resolve(__dirname, 'learning_opportunities_output_filtered.csv');
const OUTPUT_CSV = path.resolve(__dirname, 'learning_opportunities_transformed.csv');

// Adjust separator/joiner if you prefer something else
const ADDITIONAL_NOTE_JOINER = ' ; ';

function escapeForCsv(value) {
  if (value === null || value === undefined) return '';
  const s = String(value);
  if (s.includes('"') || s.includes(',') || s.includes('\n') || s.includes('\r')) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function extractEqfNumeric(eqfLabel) {
  if (!eqfLabel) return '';
  const m = String(eqfLabel).match(/(\d+)/);
  return m ? m[1] : '';
}

function extractAdditionalNotesFromLearningOutcome(loRaw) {
  if (!loRaw) return '';
  const str = String(loRaw).trim();
  // Quick return for trivial empty array text
  if (str === '[]' || str === '') return '';

  // Try JSON.parse
  try {
    const parsed = JSON.parse(str);
    if (Array.isArray(parsed)) {
      const out = [];
      for (const entry of parsed) {
        if (!entry) continue;
        const add = entry.additionalNote;
        if (Array.isArray(add)) {
          for (const a of add) {
            if (a !== null && a !== undefined) {
              const s = String(a).trim();
              if (s) out.push(s);
            }
          }
        } else if (typeof add === 'string' && add.trim()) {
          out.push(add.trim());
        }
      }
      return out.join(ADDITIONAL_NOTE_JOINER);
    }
    // if parsed but not array, try to salvage
  } catch (err) {
    // fallback below
  }

  // Fallback regex: try to extract text content inside additionalNote arrays
  // This captures string contents (naive but usually effective for malformed JSON)
  const fallbackMatches = [];
  const regex = /"additionalNote"\s*:\s*\[\s*"((?:[^"\\]|\\.)*)"/g; // captures first string in each additionalNote array
  let m;
  while ((m = regex.exec(str)) !== null) {
    // unescape basic escaped quotes/slashes
    let s = m[1].replace(/\\"/g, '"').replace(/\\\\/g, '\\');
    s = s.trim();
    if (s) fallbackMatches.push(s);
  }

  // Another fallback: capture any text sequences inside additionalNote brackets without relying on quotes
  if (fallbackMatches.length === 0) {
    const regex2 = /"additionalNote"\s*:\s*\[\s*([^\]]+?)\s*\]/g;
    while ((m = regex2.exec(str)) !== null) {
      const inner = m[1];
      // split by comma-ish boundaries and remove wrapping quotes/spaces
      const parts = inner.split(/,(?=(?:[^"]*"[^"]*")*[^"]*$)/).map(p => p.replace(/^["'\s]+|["'\s]+$/g, '').trim()).filter(Boolean);
      for (const p of parts) {
        if (p) fallbackMatches.push(p);
      }
    }
  }

  return fallbackMatches.join(ADDITIONAL_NOTE_JOINER);
}

// Desired output header & order (omit 'file')
const OUTPUT_HEADERS = [
  'title',
  'countryCode.prefLabel',
  'EQFLevel_prefLabel',
  'EQFLevel_numeric',
  'learningOutcomeSummary.noteLiteral',
  'learningOutcome_additionalNote'
];

(async function main() {
  if (!fs.existsSync(INPUT_CSV)) {
    console.error('Input CSV not found:', INPUT_CSV);
    process.exit(1);
  }

  console.log('Reading:', INPUT_CSV);
  console.log('Writing:', OUTPUT_CSV);

  const writeStream = fs.createWriteStream(OUTPUT_CSV, { encoding: 'utf8' });
  writeStream.write(OUTPUT_HEADERS.join(',') + '\n');

  let total = 0;
  let written = 0;

  fs.createReadStream(INPUT_CSV)
    .pipe(csv())
    .on('data', (row) => {
      total++;

      // Pull columns (account for original headers with dots)
      const title = row['title'] ?? '';
      const country = row['countryCode.prefLabel'] ?? row['countryCode_prefLabel'] ?? '';
      const eqfLabel = row['EQFLevel_prefLabel'] ?? row['EQFLevel.prefLabel'] ?? row['EQFLevel'] ?? '';
      const loSummary = row['learningOutcomeSummary.noteLiteral'] ?? row['learningOutcomeSummary_noteLiteral'] ?? '';
      const learningOutcomeRaw = row['learningOutcome'] ?? '';

      const eqfNumeric = extractEqfNumeric(eqfLabel);
      const additionalNotesText = extractAdditionalNotesFromLearningOutcome(learningOutcomeRaw);

      const outRow = [
        escapeForCsv(title),
        escapeForCsv(country),
        escapeForCsv(eqfLabel),
        escapeForCsv(eqfNumeric),
        escapeForCsv(loSummary),
        escapeForCsv(additionalNotesText)
      ].join(',');

      writeStream.write(outRow + '\n');
      written++;
      if (total % 10000 === 0) console.log(`Processed ${total} rows...`);
    })
    .on('end', () => {
      writeStream.end(() => {
        console.log(`Done. Processed: ${total}, Written: ${written}`);
      });
    })
    .on('error', (err) => {
      console.error('Error reading CSV:', err);
      writeStream.end();
    });
})();
