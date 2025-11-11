// add_levelnum.js
// Usage: node add_levelnum.js [inputCsv] [outputCsv]
// Defaults: cleaned_qualifications.csv -> cleaned_qualifications_with_levelnum.csv

const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

const inputPath = process.argv[2] || 'cleaned_qualifications.csv';
const outputPath = process.argv[3] || 'final_qualifications.csv';

if (!fs.existsSync(inputPath)) {
  console.error('Input file not found:', inputPath);
  process.exit(1);
}

// Escape CSV cell
function escapeCsvCell(value) {
  if (value === undefined || value === null) return '""';
  if (typeof value === 'object') {
    try { value = JSON.stringify(value); } catch { value = String(value); }
  }
  const s = String(value).replace(/"/g, '""');
  return `"${s}"`;
}

// Extract numeric part from "Level X"
function extractLevelNum(text) {
  if (!text) return '';
  const match = String(text).match(/\bLevel\s*(\d+)/i);
  if (match) return match[1];
  return '';
}

let outHeaders = null;
let writer = null;
let total = 0;

function openWriter() {
  writer = fs.createWriteStream(outputPath, { encoding: 'utf8' });
}

function closeWriter() {
  if (writer) writer.end();
}

const rs = fs.createReadStream(inputPath);

rs.pipe(csv())
  .on('headers', (hdrs) => {
    outHeaders = hdrs.slice();
    const idx = outHeaders.indexOf('qualificationLevel');
    if (idx === -1) {
      console.error('qualificationLevel column not found!');
      process.exit(1);
    }
    // insert new column right after qualificationLevel
    outHeaders.splice(idx + 1, 0, 'qualificationLevelNum');

    openWriter();
    writer.write(outHeaders.map(h => escapeCsvCell(h)).join(',') + '\n');
  })
  .on('data', (row) => {
    total++;

    const idx = outHeaders.indexOf('qualificationLevelNum');
    const levelNum = extractLevelNum(row.qualificationLevel);

    // build row array in header order
    const rowOut = [];
    for (const h of outHeaders) {
      if (h === 'qualificationLevelNum') {
        rowOut.push(escapeCsvCell(levelNum));
      } else {
        rowOut.push(escapeCsvCell(row[h]));
      }
    }
    writer.write(rowOut.join(',') + '\n');

    if (total % 1000 === 0) process.stdout.write(`\rProcessed ${total}`);
  })
  .on('end', () => {
    closeWriter();
    console.log(`\nDone. Processed ${total} rows. Output: ${path.resolve(outputPath)}`);
  })
  .on('error', (err) => {
    console.error('Error:', err);
    closeWriter();
  });
