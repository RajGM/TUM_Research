// Usage:
//   node to-csv-all-folders.js [rootDir] [outCsvPath] [--no-require-learning-outcome]
// Examples:
//   node to-csv-all-folders.js qualificationsData all_quals.csv
//   node to-csv-all-folders.js qualificationsData all_quals.csv --no-require-learning-outcome

const fs = require('fs');
const fsp = require('fs').promises;
const path = require('path');

const ROOT = process.argv[2] || 'qualificationData';
const OUT_CSV = process.argv[3] || 'qualificationsValid.csv';
const REQUIRE_LO = !process.argv.includes('--no-require-learning-outcome');

// ---------- helpers ----------
function get(o, pathArr, def = null) {
  return pathArr.reduce((acc, k) => (acc && acc[k] !== undefined ? acc[k] : undefined), o) ?? def;
}

function normalizePrefLabel(v) {
  if (v == null) return null;
  if (typeof v === 'string') return v;
  if (Array.isArray(v)) {
    const en = v.find(x => x?.en || x?.['en-GB'] || x?.['en-US']);
    if (en) return en.en || en['en-GB'] || en['en-US'];
    const firstStr = v.find(x => typeof x === 'string');
    if (firstStr) return firstStr;
    const firstObj = v.find(x => x && typeof x === 'object' && (x['@value'] || x.value));
    if (firstObj) return firstObj['@value'] || firstObj.value;
    return String(v[0]);
  }
  if (typeof v === 'object') {
    if (v.en || v['en-GB'] || v['en-US']) return v.en || v['en-GB'] || v['en-US'];
    if (v['@value']) return v['@value'];
    if (v.value) return v.value;
    for (const k of Object.keys(v)) if (typeof v[k] === 'string') return v[k];
  }
  return String(v);
}

function findCountryPrefLabel(obj) {
  if (!obj || typeof obj !== 'object') return null;
  if (obj.countryCode && typeof obj.countryCode === 'object') {
    const lbl = normalizePrefLabel(obj.countryCode.prefLabel);
    if (lbl) return lbl;
  }
  if (Array.isArray(obj)) {
    for (const item of obj) {
      const found = findCountryPrefLabel(item);
      if (found) return found;
    }
  } else {
    for (const key of Object.keys(obj)) {
      const found = findCountryPrefLabel(obj[key]);
      if (found) return found;
    }
  }
  return null;
}

// learningOutcome[*].additionalNote[*].noteLiteral -> array of strings
function extractLearningOutcomeNotes(data) {
  const los = Array.isArray(data.learningOutcome) ? data.learningOutcome : [];
  const notes = [];
  for (const lo of los) {
    const aNotes = Array.isArray(lo?.additionalNote) ? lo.additionalNote : [];
    for (const n of aNotes) {
      const lit = n?.noteLiteral;
      if (lit == null) continue;
      if (Array.isArray(lit)) {
        for (const x of lit) {
          const s = normalizePrefLabel(x);
          if (s) notes.push(String(s));
        }
      } else {
        const s = normalizePrefLabel(lit);
        if (s) notes.push(String(s));
      }
    }
  }
  return notes;
}

function parseQualificationLevelNum(levelText) {
  if (!levelText) return '';
  const m = String(levelText).match(/(\d+(?:\.\d+)?)/);
  return m ? m[1] : '';
}

// CSV escaping per RFC 4180
function csvEscape(value) {
  if (value == null) return '';
  const s = String(value);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

async function listDirs(dir) {
  const entries = await fsp.readdir(dir, { withFileTypes: true });
  return entries.filter(d => d.isDirectory()).map(d => d.name).sort();
}

async function* iterJsonFiles(dir) {
  const entries = await fsp.readdir(dir, { withFileTypes: true });
  for (const e of entries) {
    if (e.isFile() && e.name.toLowerCase().endsWith('.json')) {
      yield path.join(dir, e.name);
    }
  }
}

// ---------- main ----------
(async () => {
  const ws = fs.createWriteStream(OUT_CSV, { encoding: 'utf8' });
  ws.write([
    'title',
    'country',
    'qualificationLevel',
    'qualificationLevelNum',
    'description',
    'learningOutcome',
    'uri' // NEW: last column
  ].join(',') + '\n');

  const folders = await listDirs(ROOT);

  for (const folder of folders) {
    const folderPath = path.join(ROOT, folder);
    process.stderr.write(`\n▶ Processing folder: ${folder}\n`);

    let folderRows = 0;

    for await (const filePath of iterJsonFiles(folderPath)) {
      try {
        const raw = await fsp.readFile(filePath, 'utf8');
        const data = JSON.parse(raw);

        const title = get(data, ['title'], '');
        const eqfPref = normalizePrefLabel(get(data, ['EQFLevel', 'prefLabel'], null));
        const qualificationLevel = eqfPref || '';
        const qualificationLevelNum = parseQualificationLevelNum(qualificationLevel);
        const description = get(data, ['description'], '');
        const countryPref = findCountryPrefLabel(data) || folder;
        const loNotes = extractLearningOutcomeNotes(data);
        if (REQUIRE_LO && loNotes.length === 0) continue;

        // Prefer top-level "uri", but also try common alternates.
        const uri =
          get(data, ['uri'], '') ||
          get(data, ['@id'], '') ||
          get(data, ['id'], '');

        const row = [
          csvEscape(title),
          csvEscape(countryPref),
          csvEscape(qualificationLevel),
          csvEscape(qualificationLevelNum),
          csvEscape(description),
          csvEscape(loNotes.join(' | ')),
          csvEscape(uri) // NEW: last column
        ].join(',');

        if (!ws.write(row + '\n')) {
          await new Promise(resolve => ws.once('drain', resolve));
        }
        folderRows++;
      } catch (err) {
        process.stderr.write(`  ⚠️  ${path.basename(filePath)}: ${err.message}\n`);
      }
    }

    process.stderr.write(`  ✓ ${folderRows} rows written from ${folder}\n`);
  }

  ws.end();
  await new Promise(resolve => ws.on('finish', resolve));
  process.stderr.write(`\n✅ Done. CSV written to ${OUT_CSV}\n`);
})().catch(err => {
  console.error(err);
  process.exit(1);
});
