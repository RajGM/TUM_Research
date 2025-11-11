// clean_csv_filter_noise.js
// Usage: node clean_csv_filter_noise.js [inputCsvPath] [outputCsvPath]
// Defaults: filtered_qualifications.csv -> cleaned_qualifications.csv

const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

const inputPath = process.argv[2] || 'filtered_qualifications.csv';
const outputPath = process.argv[3] || 'cleaned_qualifications.csv';

if (!fs.existsSync(inputPath)) {
  console.error('Input file not found:', inputPath);
  process.exit(1);
}

// Basic HTML entity decode
function decodeEntities(str) {
  if (!str) return str;
  return str
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&#39;/g, "'")
    .replace(/&#34;/g, '"');
}

// Remove HTML tags and collapse whitespace
function stripHtmlTags(s) {
  if (!s) return '';
  let out = String(s);
  out = out.replace(/<[^>]*>/g, ' ');
  out = decodeEntities(out);
  out = out.replace(/{\s*[A-Za-z0-9_-]*\s*/g, ' ');
  out = out.replace(/[{}]/g, ' ');
  out = out.replace(/[\x00-\x1F\x7F]/g, ' ');
  out = out.replace(/\uFFFD/g, ' '); // replacement char
  out = out.replace(/\s+/g, ' ').trim();
  return out;
}

// Try to parse learningOutcomes (robust)
function parseLearningOutcomesRaw(raw) {
  if (!raw) return [];
  let s = String(raw).trim();

  // direct parse attempt
  try {
    const parsed = JSON.parse(s);
    if (Array.isArray(parsed)) return parsed;
    if (parsed && parsed.learningOutcome && Array.isArray(parsed.learningOutcome)) return parsed.learningOutcome;
    if (parsed && typeof parsed === 'object') return [parsed];
  } catch (e) {
    // heuristics: try to fix single quotes / bare keys
    let attempt = s;
    attempt = attempt.replace(/([\[{,]\s*)'([^']+?)'(\s*[:,\]}])/g, '$1"$2"$3');
    attempt = attempt.replace(/:\s*'([^']*?)'/g, ': "$1"');
    attempt = attempt.replace(/(\")?([a-zA-Z0-9_]+)(\")?\s*:/g, '"$2":');
    try {
      const parsed2 = JSON.parse(attempt);
      if (Array.isArray(parsed2)) return parsed2;
      if (parsed2 && parsed2.learningOutcome && Array.isArray(parsed2.learningOutcome)) return parsed2.learningOutcome;
      if (parsed2 && typeof parsed2 === 'object') return [parsed2];
    } catch (e2) {
      // fallback: extract titles/notes via regex
      const items = [];
      const titleRe = /["']?title["']?\s*:\s*["']([^"']+)["']/gi;
      const notesRe = /["']?(?:additionalNote|additionalNotes|noteLiteral|additionalnote)["']?\s*:\s*["']([^"']*)["']/gi;
      let m;
      while ((m = titleRe.exec(s)) !== null) {
        items.push({ title: m[1] });
      }
      if (items.length > 0) {
        let i = 0;
        while ((m = notesRe.exec(s)) !== null && i < items.length) {
          items[i].additionalNotes = m[1];
          i++;
        }
        return items;
      }

      // last-ditch: split into blocks on '},{' or ']|['
      const maybeItems = s.split(/\},\s*\{/).map(x => x.replace(/^[\[{]|\}[\]]$/g, '').trim()).filter(Boolean);
      if (maybeItems.length > 0) {
        return maybeItems.map(block => {
          const t = (block.match(/title[^:]*[:]\s*["']([^"']+)["']/) || [null, block])[1];
          const a = (block.match(/additionalNote[^:]*[:]\s*["']([^"']*)["']/) || [null, ''])[1];
          return { title: t || '', additionalNotes: a || '' };
        });
      }

      return [];
    }
  }
  return [];
}

// Detect garbage / noise LO entries
function isGarbageLO(text) {
  if (!text) return true;
  const t = text.trim();
  if (t.length === 0) return true;
  // extremely short
  if (t.length < 3) return true;
  // patterns: "1." or "2." etc
  if (/^\d+\.$/.test(t)) return true;
  // NA, NA NA, N A, n/a, n\a, none, -, etc
  if (/^(?:n\s*\/\s*a|na(?:\s+na)?|n\s*a|none|[-–—]+)$/i.test(t)) return true;
  // just a single digit
  if (/^\d+$/.test(t)) return true;
  // if it's just punctuation
  if (/^[\p{P}\p{S}\s]+$/u.test(t)) return true;
  return false;
}

// Remove prefixes like Title:, Notes:, remove replacement char, strip html
function removePrefixesAndNoise(text) {
  if (!text) return '';
  let t = String(text);
  t = t.replace(/\uFFFD/g, ' ');
  // remove leading labels at line starts
  t = t.replace(/(^|\n)\s*(Title|Titles|Learning Outcome|LearningOutcome|Notes|Note)\s*[:\-\u2014]\s*/gi, '$1');
  // remove inline Label: occurrences
  t = t.replace(/\b(Title|Notes|Note|Learning Outcome|LearningOutcome)\s*[:]\s*/gi, '');
  t = stripHtmlTags(t);
  return t;
}

// Compose final LO text: parse -> clean -> filter garbage -> join
function learningOutcomesToText(losRaw) {
  const parsed = parseLearningOutcomesRaw(losRaw);
  if (!Array.isArray(parsed) || parsed.length === 0) return '';
  const cleaned = parsed.map(item => {
    let title = '';
    let notes = '';
    if (typeof item === 'string') {
      title = item;
    } else if (item && typeof item === 'object') {
      title = item.title || item.titleLiteral || item.prefLabel || item.name || '';
      if (Array.isArray(item.additionalNote)) {
        notes = item.additionalNote.map(n => (n && (n.noteLiteral || n.note || n['noteLiteral'])) || '').filter(Boolean).join('\n\n');
      } else if (item.additionalNotes) {
        notes = item.additionalNotes;
      } else if (item.additionalNote && typeof item.additionalNote === 'string') {
        notes = item.additionalNote;
      } else if (item.noteLiteral) {
        notes = item.noteLiteral;
      } else {
        notes = item.note || item.notes || '';
      }
    }

    title = removePrefixesAndNoise(title);
    notes = removePrefixesAndNoise(notes);

    // join title+notes carefully
    let combined = '';
    if (title && notes) combined = `${title} — ${notes}`;
    else combined = title || notes || '';

    // final collapse whitespace
    combined = combined.replace(/\s+/g, ' ').trim();
    return combined;
  }).filter(Boolean);

  // Filter out garbage entries
  const filtered = cleaned.filter(x => !isGarbageLO(x));

  return filtered.join('\n\n');
}

// CSV escaping
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
let total = 0;
let written = 0;

function openWriter() {
  writer = fs.createWriteStream(outputPath, { encoding: 'utf8' });
}

function closeWriter() {
  if (writer) writer.end();
}

const rs = fs.createReadStream(inputPath);

rs
  .pipe(csv())
  .on('headers', (hdrs) => {
    outHeaders = hdrs.slice();
    if (!outHeaders.includes('description')) outHeaders.push('description');
    if (!outHeaders.includes('learningOutcomes')) outHeaders.push('learningOutcomes');
    openWriter();
    writer.write(outHeaders.map(h => escapeCsvCell(h)).join(',') + '\n');
  })
  .on('data', (row) => {
    total++;

    // Clean description (strip HTML/entities and replacement char)
    const cleanedDescription = stripHtmlTags(row.description);

    // Clean LOs and filter out garbage like "1." / "NA NA"
    const cleanedLO = learningOutcomesToText(row.learningOutcomes);

    row.description = cleanedDescription;
    row.learningOutcomes = cleanedLO;

    const outLine = outHeaders.map(h => escapeCsvCell(row.hasOwnProperty(h) ? row[h] : '')).join(',');
    writer.write(outLine + '\n');
    written++;

    if (total % 1000 === 0) process.stdout.write(`\rProcessed ${total} rows, written ${written}`);
  })
  .on('end', () => {
    closeWriter();
    console.log(`\nDone. Processed ${total} rows. Written ${written} rows. Output: ${path.resolve(outputPath)}`);
  })
  .on('error', (err) => {
    console.error('Error while processing CSV:', err);
    closeWriter();
  });
