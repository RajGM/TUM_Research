
/**
 * clean_with_openai_concurrent.js
 *
 * Usage:
 *   node clean_with_openai_concurrent.js file1.csv
 *
 * Improvements over sequential version:
 *   - Makes CONCURRENT_LIMIT parallel API requests (default 30)
 *   - Writes results as they arrive (streaming writes)
 *   - Each row processed independently with retries
 *   - Better throughput and faster processing
 *   - NO external concurrency library needed
 *
 * Requirements:
 *   - Set OPENAI_API_KEY environment variable
 *   - npm install csv-parser csv-writer node-fetch@2 fs-extra
 */

const fs = require("fs");
const path = require("path");
const csv = require("csv-parser");
const createCsvWriter = require("csv-writer").createObjectCsvWriter;
const fetch = require("node-fetch");
const fse = require("fs-extra");

const OPENAI_KEY = '';
if (!OPENAI_KEY) {
  console.error("Please set OPENAI_API_KEY in environment");
  process.exit(1);
}

const MODEL = "gpt-4o-mini";
const CONCURRENT_LIMIT = Number(30);
const MAX_RETRIES = Number(3);
const CHECKPOINT_PATH = path.join(process.cwd(), "checkpoint.json");
const COLS_TO_CLEAN = ["description", "learningOutcomes"];

/* ------------------ Simple Concurrency Control ------------------ */

class ConcurrencyPool {
  constructor(limit) {
    this.limit = limit;
    this.running = 0;
    this.queue = [];
  }

  async run(fn) {
    while (this.running >= this.limit) {
      await new Promise(resolve => this.queue.push(resolve));
    }
    
    this.running++;
    try {
      return await fn();
    } finally {
      this.running--;
      const resolve = this.queue.shift();
      if (resolve) resolve();
    }
  }
}

/* ------------------ Utilities ------------------ */

function loadCheckpoint() {
  if (!fs.existsSync(CHECKPOINT_PATH)) return null;
  try {
    return JSON.parse(fs.readFileSync(CHECKPOINT_PATH, "utf8"));
  } catch (e) {
    console.warn("Warning: failed to parse checkpoint, ignoring.", e.message);
    return null;
  }
}

function saveCheckpoint(obj) {
  fs.writeFileSync(CHECKPOINT_PATH, JSON.stringify(obj, null, 2), "utf8");
}

async function readCsv(filePath) {
  return new Promise((resolve, reject) => {
    const rows = [];
    fs.createReadStream(filePath)
      .pipe(csv())
      .on("data", (data) => rows.push(data))
      .on("end", () => resolve(rows))
      .on("error", reject);
  });
}

/* Build system prompt for single-row cleaning */
function buildSystemPrompt() {
  return `You are a strict text-cleaner. You MUST follow these rules exactly:
1) You will receive a single row with "description" and "learningOutcomes" fields.
2) Remove ONLY filler/boilerplate phrases (listed below). DO NOT paraphrase or reword anything else.
3) If no filler is found, return the original text exactly.
4) If entire input is filler, return empty string.
5) Return valid JSON object with:
   - "description": cleaned description text
   - "description_removed": array of exact substrings removed
   - "learningOutcomes": cleaned learningOutcomes text  
   - "learningOutcomes_removed": array of exact substrings removed
6) Output ONLY valid JSON, no explanation.

Filler phrases to remove (case-insensitive):
- "Please contact provider for more information", "Please contact the provider for more information"
- "For more information contact the provider", "National Qualification Framework (NQF)"
- "National Qualification Framework", "See website for details", "See our website for details"
- "For more information", "Contact provider", "Fees and charges apply"
- "Subject to change without notice", "Enrol now", "Limited places available"
- Phone numbers and emails when standalone
- Leading bullets: "1. ", "1) ", "a. ", "a) ", "(a) ", "-", "•"`.trim();
}

/* Call OpenAI API for a single row with retries */
async function cleanSingleRow(rowData, rowIndex, retries = MAX_RETRIES) {
  const messages = [
    { role: "system", content: buildSystemPrompt() },
    {
      role: "user",
      content: `INPUT_JSON:\n${JSON.stringify({
        description: rowData.description || "",
        learningOutcomes: rowData.learningOutcomes || "",
      })}\n\nReturn cleaned JSON object.`,
    },
  ];

  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const resp = await fetch("https://api.openai.com/v1/chat/completions", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${OPENAI_KEY}`,
        },
        body: JSON.stringify({
          model: MODEL,
          messages,
          temperature: 0,
          response_format: { type: "json_object" },
        }),
      });

      if (!resp.ok) {
        const txt = await resp.text();
        throw new Error(`API error ${resp.status}: ${txt}`);
      }

      const data = await resp.json();
      const content = data.choices?.[0]?.message?.content;
      
      if (!content) {
        throw new Error("Empty response from API");
      }

      // Parse and validate
      let parsed = JSON.parse(content.trim());
      
      // Validate required fields
      if (typeof parsed.description === 'undefined' || typeof parsed.learningOutcomes === 'undefined') {
        throw new Error("Missing required fields in response");
      }

      return {
        success: true,
        rowIndex,
        cleaned: {
          description: String(parsed.description || ""),
          learningOutcomes: String(parsed.learningOutcomes || ""),
        },
        removed: {
          description: Array.isArray(parsed.description_removed) ? parsed.description_removed : [],
          learningOutcomes: Array.isArray(parsed.learningOutcomes_removed) ? parsed.learningOutcomes_removed : [],
        },
      };
    } catch (err) {
      console.warn(`Row ${rowIndex} attempt ${attempt}/${retries} failed: ${err.message}`);
      
      if (attempt < retries) {
        await new Promise((r) => setTimeout(r, 500 * attempt));
      } else {
        return {
          success: false,
          rowIndex,
          error: err.message,
          original: rowData,
        };
      }
    }
  }
}

/* ------------------ Main Processing ------------------ */

async function processFileWithConcurrency(filePath) {
  console.log(`\nProcessing: ${filePath}`);
  const basename = path.basename(filePath);
  const tempCsv = path.join(process.cwd(), `.${basename}.processing.csv`);
  const auditPath = path.join(process.cwd(), `cleaned_${basename}_audit.json`);
  const errorPath = path.join(process.cwd(), `cleaned_${basename}_errors.json`);

  // Load rows
  const rows = await readCsv(filePath);
  console.log(`Read ${rows.length} rows`);

  const headerColumns = rows[0] ? Object.keys(rows[0]) : COLS_TO_CLEAN;

  // Initialize or resume checkpoint
  let checkpoint = loadCheckpoint();
  if (!checkpoint || checkpoint.currentFile !== filePath) {
    checkpoint = {
      currentFile: filePath,
      processedRows: [],
      tempCsv,
      auditPath,
      status: "in-progress",
    };
    saveCheckpoint(checkpoint);
    console.log("Created new checkpoint");
  } else {
    checkpoint.processedRows = checkpoint.processedRows || [];
    console.log(`Resuming: ${checkpoint.processedRows.length} rows already processed`);
  }

  const processedSet = new Set(checkpoint.processedRows);

  // Setup CSV writer for incremental writes
  const csvWriter = createCsvWriter({
    path: tempCsv,
    header: headerColumns.map((h) => ({ id: h, title: h })),
    append: fs.existsSync(tempCsv),
  });

  // Load existing audit and errors
  let auditArray = fs.existsSync(auditPath) ? JSON.parse(fs.readFileSync(auditPath, "utf8")) : [];
  let errorArray = fs.existsSync(errorPath) ? JSON.parse(fs.readFileSync(errorPath, "utf8")) : [];

  // Prepare work queue (only unprocessed rows)
  const workQueue = rows
    .map((row, idx) => ({ row, idx }))
    .filter(({ idx }) => !processedSet.has(idx));

  console.log(`Processing ${workQueue.length} remaining rows with ${CONCURRENT_LIMIT} concurrent requests...`);

  // Concurrency control
  const pool = new ConcurrencyPool(CONCURRENT_LIMIT);
  let completed = processedSet.size;
  const startTime = Date.now();

  // Process all rows concurrently
  const promises = workQueue.map(({ row, idx }) =>
    pool.run(async () => {
      const result = await cleanSingleRow(row, idx);

      if (result.success) {
        // Build cleaned row
        const cleanedRow = { ...row };
        cleanedRow.description = result.cleaned.description;
        cleanedRow.learningOutcomes = result.cleaned.learningOutcomes;

        // Write immediately (synchronized by file system)
        await csvWriter.writeRecords([cleanedRow]);

        // Add to audit
        auditArray.push({
          rowIndex: idx,
          original: {
            description: row.description || "",
            learningOutcomes: row.learningOutcomes || "",
          },
          cleaned: result.cleaned,
          removed: result.removed,
        });

        // Update checkpoint
        processedSet.add(idx);
        completed++;

        if (completed % 10 === 0) {
          // Periodic saves
          fs.writeFileSync(auditPath, JSON.stringify(auditArray, null, 2));
          checkpoint.processedRows = Array.from(processedSet);
          saveCheckpoint(checkpoint);
          
          const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
          const rate = (completed / (Date.now() - startTime) * 1000).toFixed(1);
          console.log(`Progress: ${completed}/${rows.length} (${Math.round(completed/rows.length*100)}%) - ${rate} rows/sec - ${elapsed}s elapsed`);
        }
      } else {
        // Log error
        errorArray.push(result);
        console.error(`FAILED row ${idx}: ${result.error}`);
      }

      return result;
    })
  );

  // Wait for all to complete
  await Promise.all(promises);

  // Final saves
  fs.writeFileSync(auditPath, JSON.stringify(auditArray, null, 2));
  if (errorArray.length > 0) {
    fs.writeFileSync(errorPath, JSON.stringify(errorArray, null, 2));
    console.warn(`${errorArray.length} rows failed - see ${errorPath}`);
  }

  // Replace original file
  const backupPath = `${filePath}.bak.${Date.now()}`;
  await fse.copy(filePath, backupPath);
  await fse.move(tempCsv, filePath, { overwrite: true });

  // Mark complete
  checkpoint.status = "done";
  checkpoint.completedAt = new Date().toISOString();
  checkpoint.processedRows = Array.from(processedSet);
  saveCheckpoint(checkpoint);

  const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
  const avgRate = (rows.length / (Date.now() - startTime) * 1000).toFixed(1);
  
  console.log(`\n✓ Completed: ${rows.length} rows processed in ${totalTime}s (${avgRate} rows/sec)`);
  console.log(`  Backup: ${backupPath}`);
  console.log(`  Audit: ${auditPath}`);
  if (errorArray.length > 0) {
    console.log(`  Errors: ${errorPath}`);
  }
}

/* ------------------ Main ------------------ */

(async () => {
  try {
    const args = process.argv.slice(2);
    if (args.length < 1) {
      console.error("Usage: node clean_with_openai_concurrent.js file.csv");
      process.exit(1);
    }

    // Check for in-progress checkpoint
    const ck = loadCheckpoint();
    if (ck && ck.status === "in-progress" && ck.currentFile) {
      console.log("Resuming:", ck.currentFile);
      await processFileWithConcurrency(ck.currentFile);
    }

    // Process requested files
    for (const f of args) {
      if (!fs.existsSync(f)) {
        console.error("File not found:", f);
        continue;
      }
      if (ck && f === ck.currentFile) continue; // Already processed above
      await processFileWithConcurrency(f);
    }

    console.log("\n✓ All files processed");
  } catch (err) {
    console.error("\n✗ Fatal error:", err.message);
    process.exit(1);
  }
})();