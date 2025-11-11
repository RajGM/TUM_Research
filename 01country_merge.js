// merge_country_ndjson.js
// Make per-country NDJSON files by concatenating all COUNTRY_eqfX.ndjson (streaming, no memory bloat)

const fs = require("fs");
const fsp = fs.promises;
const path = require("path");
const readline = require("readline");

const IN_DIR = path.resolve(__dirname, "files");
const OUT_DIR = path.resolve(__dirname, "countryFiles");

// Regex to match: ABC_eqf1.ndjson
const FILE_RE = /^([A-Z]{3})_eqf(\d+)\.ndjson$/i;

async function ensureDir(dir) {
  await fsp.mkdir(dir, { recursive: true });
}

function listInputFiles() {
  const all = fs.readdirSync(IN_DIR);
  return all
    .filter(f => FILE_RE.test(f))
    .map(f => {
      const m = f.match(FILE_RE);
      return {
        country: m[1].toUpperCase(),
        level: Number(m[2]),
        file: path.join(IN_DIR, f),
      };
    });
}

async function concatNdjson(outPath, inputPaths) {
  // Remove existing output to start fresh
  try { await fsp.unlink(outPath); } catch {}
  // We'll append per source file
  const out = fs.createWriteStream(outPath, { flags: "a" });

  let written = 0;

  for (const src of inputPaths) {
    console.log(`  • merging ${path.basename(src)} -> ${path.basename(outPath)}`);
    await new Promise((resolve, reject) => {
      const rl = readline.createInterface({
        input: fs.createReadStream(src, { encoding: "utf-8" }),
        crlfDelay: Infinity,
      });

      rl.on("line", (line) => {
        const trimmed = line.trim();
        if (!trimmed) return; // skip blanks
        // Write as-is, ensure newline
        out.write(trimmed + "\n");
        written++;
      });

      rl.on("close", resolve);
      rl.on("error", reject);
    });
  }

  await new Promise(r => out.end(r));
  return written;
}

(async function main() {
  await ensureDir(OUT_DIR);

  const items = listInputFiles();
  if (items.length === 0) {
    console.log(`No input files matching "*_eqfX.ndjson" in ${IN_DIR}`);
    process.exit(0);
  }

  // Group by country
  const map = new Map();
  for (const it of items) {
    if (!map.has(it.country)) map.set(it.country, []);
    map.get(it.country).push(it);
  }

  // Sort each country's files by level (ascending), then by filename
  for (const [country, arr] of map.entries()) {
    arr.sort((a, b) => a.level - b.level || a.file.localeCompare(b.file));
  }

  let grandTotal = 0;
  console.log(`Found ${map.size} countries to merge.`);
  for (const [country, arr] of map.entries()) {
    const outPath = path.join(OUT_DIR, `${country}.ndjson`);
    const sources = arr.map(x => x.file);

    console.log(`\n▶️  Country ${country}: ${sources.length} level files`);
    const count = await concatNdjson(outPath, sources);
    grandTotal += count;
    console.log(`   ✅ Wrote ${count} records -> ${path.relative(process.cwd(), outPath)}`);
  }

  console.log(`\n🎉 Done. Total records written across all countries: ${grandTotal}`);
})().catch(err => {
  console.error("Fatal error:", err);
  process.exit(1);
});
