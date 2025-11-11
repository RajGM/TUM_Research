// merge-csvs.js
// Usage:
//   node merge-csvs.js learningOpportunities.csv qualifications.csv polished.csv
//
// Example:
//   node merge-csvs.js learningOpportunities.csv qualifications.csv polished.csv

const fs = require('fs');
const readline = require('readline');
const path = require('path');

async function mergeCSVs(file1, file2, outputFile) {
  const out = fs.createWriteStream(outputFile, { encoding: 'utf8' });

  // Helper to stream a CSV, skipping header if told to
  async function appendCSV(file, skipHeader = false) {
    const rl = readline.createInterface({
      input: fs.createReadStream(file, { encoding: 'utf8' }),
      crlfDelay: Infinity
    });
    let firstLine = true;
    for await (const line of rl) {
      if (firstLine && skipHeader) {
        firstLine = false;
        continue;
      }
      firstLine = false;
      out.write(line + '\n');
    }
  }

  console.log(`📂 Merging:\n  1️⃣ ${file1}\n  2️⃣ ${file2}\n➡️  ${outputFile}`);

  // Write first file entirely (including header)
  await appendCSV(file1, false);

  // Append second file skipping header
  await appendCSV(file2, true);

  out.end();
  await new Promise(resolve => out.on('finish', resolve));

  console.log(`✅ Done! Combined file saved as ${outputFile}`);
}

// ---------- main ----------
(async () => {
  const [,, f1, f2, outFile] = process.argv;

  if (!f1 || !f2 || !outFile) {
    console.error('Usage: node merge-csvs.js <file1.csv> <file2.csv> <output.csv>');
    process.exit(1);
  }

  try {
    await mergeCSVs(f1, f2, outFile);
  } catch (err) {
    console.error('❌ Error merging CSVs:', err.message);
    process.exit(1);
  }
})();
