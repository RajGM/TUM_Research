# 🧠 Europass Data Ingestion & Processing Pipeline

**Version:** 2025.11  
**Author:** Raj Gaurav Maurya  
**Type:** Research/Industry Technical Documentation  
**Purpose:** End-to-end description of the automated Europass Qualification and Learning Opportunity data pipeline.

---

## 🗺️ Overview

This pipeline orchestrates a multi-stage data collection and transformation workflow from the [Europass ePortfolio QDR API](https://europa.eu/europass/eportfolio/api/qdr/europass/qdr-search/search).

It performs **massive, resumable, concurrent crawling** across all EU and EEA countries, covering both **qualifications** and **learning opportunities** according to EQF (European Qualifications Framework) levels.

Each numbered script forms a step in a **sequentially dependent pipeline**, responsible for one logical phase of data processing.

---

## 🔢 Pipeline Sequence Summary

| Step | Script | Purpose | Input | Output |
|------|---------|----------|--------|---------|
| 00 | `00save2.js` | Bulk concurrent fetch of Europass QDR search results by country and EQF level. | Europass QDR API | Per-(country, level) `.ndjson` files + `europass_meta.json` |
| 01 | `01country_merge.js` | Merge per-level `.ndjson` into one file per country. | `/files` | `/countryFiles/<country>.ndjson` |
| 02 | `02fetchCountries.js` | Fetch each qualification detail by URI, resume-safe. | `/countryFiles/*.ndjson` | `/qualificationData/<country>/*.json` + meta |
| 03 | `03extractData.js` | Extract structured attributes (title, level, outcomes, description) and export CSV. | `/qualificationData` | `qualificationsValid.csv` |
| 10 | `10fetch_learningOpportunities.js` | Fetch paginated learning opportunities (EQF 1–5). | Europass API | `/output_learningOpportunities/level{n}.ndjson` |
| 11 | `11fetch_qualification.js` | Fetch paginated qualifications (EQF 1–8). | Europass API | `/output_qualifications/level{n}.ndjson` |
| 12 | `12scrape_europass.js` | Generic Europass scraper (base template). | Europass API | `/output/level{n}.ndjson` |
| 20 | `20scraper_opportunities.js` | Deep scrape learning opportunities by UUID. | `levelXindex.json` | `/json_learningOpportunities/*.json` |
| 21 | `21scraper_qualifications.js` | Deep scrape qualifications by UUID. | `levelXindex.json` | `/json_qualifications/*.json` |

---

## ⚙️ Step-by-Step Breakdown

### **00. `00save2.js` – Europass Bulk Fetch Engine**

- **Goal:** Download all Europass records for all EQF levels (1–8) across 40+ European countries.
- **Concurrency:** Up to **10 countries in parallel**.
- **Rate limiting:** 3 requests/second across all workers.
- **Fault tolerance:**  
  - Retries up to 3× per request.  
  - Resumable via `europass_meta.json`.  
  - Heartbeat every 60s; stall detection.
- **Output:**
  - `/files/<country>_eqf<level>.ndjson`
  - `europass_meta.json` (safe-write atomic commits)
- **Modes:** `TYPE = "qualification"` or `"learning-opportunity"`.

---

### **01. `01country_merge.js` – Countrywise Merge Utility**

- **Goal:** Combine all EQF-level NDJSON files into single country-level NDJSONs.
- **Method:** Stream-based concatenation (zero memory bloat).
- **Pattern:** Matches `XXX_eqfY.ndjson` filenames.
- **Output:** `/countryFiles/<ISO3>.ndjson`
- **Efficiency:** Handles millions of records via streaming.

---

### **02. `02fetchCountries.js` – Qualification Detail Fetcher**

- **Goal:** Resolve each qualification URI to full JSON via the Europass API.
- **Concurrency:** 100 requests in parallel.
- **Rate limiting:** ~20 requests/second total.
- **Error handling:** Exponential backoff (3 retries).
- **Resume-safe:** Per-country `meta.json` tracks:
  - Last processed line
  - Success/failure counters
  - Skipped entries
- **Logging:**  
  - Milestones every +100 successes/failures  
  - Retains last 100 event samples
- **Output:**
  - `/qualificationData/<COUNTRY>/<UUID>.json`
  - `/meta/<COUNTRY>_meta.json`

---

### **03. `03extractData.js` – CSV Extraction & Normalization**

- **Goal:** Transform raw JSON qualifications into structured CSV for analysis.
- **Features:**
  - Extracts: `title`, `country`, `qualificationLevel`, `description`, `learningOutcome`, `uri`
  - Supports flag `--no-require-learning-outcome` to include all rows.
  - Normalizes multilingual labels using `normalizePrefLabel()`.
  - Extracts learning outcome text from nested `learningOutcome.additionalNote.noteLiteral`.
- **Output:**  
  `qualificationsValid.csv`

---

### **10. `10fetch_learningOpportunities.js` – Paginated Learning Opportunity Fetcher**

- **Goal:** Fetch learning opportunities (EQF levels 1–5) from Europass.
- **Process:**
  - Iterates `from` offsets until no new records.
  - Deduplicates via URI index.
  - Persists progress using `.index.json` and `.progress.json`.
  - Rebuilds a combined `.json` after each page.
- **Resumable:** Continues from last saved `from` offset.
- **Output:**  
  `/output_learningOpportunities/level{n}.*`

---

### **11. `11fetch_qualification.js` – Paginated Qualification Fetcher**

- Same logic as Step 10 but fetches **qualifications** instead of learning opportunities.
- Covers EQF levels **1–8**.
- **Output:**  
  `/output_qualifications/level{n}.*`

---

### **12. `12scrape_europass.js` – Generic Scraper Template**

- Base version of the fetcher scripts.
- Intended for custom experiments or mixed-type data retrieval.
- Identical architecture to Steps 10–11.

---

### **20. `20scraper_opportunities.js` – Deep Learning Opportunity Scraper**

- **Goal:** Download full JSON detail for each learning opportunity.
- **Input:** `levelXindex.json` from `/output_learningOpportunities`.
- **Implementation:**  
  - Uses `axios` with retry logic (up to 3×).  
  - Concurrency = 10 requests.  
  - Saves successful JSONs, and detailed error payloads for failures.
- **Output:**
  - `/json_learningOpportunities/<UUID>.json`
  - `/json_learningOpportunities_errors/*.error.json`

---

### **21. `21scraper_qualifications.js` – Deep Qualification Scraper**

- **Goal:** Fetch and persist detailed qualification JSONs.
- **Input:** `levelXindex.json` from `/output_qualifications`.
- **Concurrency:** 100 simultaneous requests.
- **Error recovery:** Saves problematic responses in `/json_qualifications_errors`.
- **Output:**
  - `/json_qualifications/<UUID>.json`
  - `/json_qualifications_errors/*.error.json`

---

## 🧩 Data Flow Diagram (Textual)

