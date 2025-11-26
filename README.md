# 🧠 Europass Data Ingestion & Processing Pipeline

**Version:** 2025.11  
**Author:** Raj Gaurav Maurya  
**Type:** Research/Industry Technical Documentation  
**Purpose:** Full end-to-end documentation of the Europass Qualification and Learning Opportunity data ingestion, transformation, and AI-assisted cleaning pipeline.   
**Affiliation:** M.Sc. Data & Society, Technical University of Munich  

**Research Leadership & Collaboration:**  
- **Lead Researcher:** Clara Krämer (PhD Researcher, TUM School of Social Sciences & Technology)  
- **Supervisor:** Dr. Florian Egli (TUM – Public Policy for the Green Transition)  
- **Role of Author:** Engineering & development of the full Europass data ingestion, transformation, cleaning, and AI-assisted processing pipeline.

## 0. Context & Project Motivation (Compressed)

This pipeline documents the full data engineering workflow (July–November 2025) developed for a TUM research project examining how real European qualifications can be mapped onto simulated “reskilling journeys” for Europe’s green transition. The aim is to estimate realistic training pathways by linking ESCO skills to actual qualification descriptions.

The Europass dataset (~3GB XML) required extensive processing, including:  
- Inspecting and parsing raw XML structure  
- Scraping missing qualification descriptions (83% lacked text)  
- Standardising **Qualification / Country / EQF Level / Description** fields  
- Cleaning noise, boilerplate, filler text, and malformed entries  
- Resolving country-code inconsistencies  
- Managing **30M+ external URLs**, leading to several pipeline redesigns  

Outcomes include:  
- **100k+ cleaned qualification & training records**  
- A browsable **knowledge graph**  
- A web frontend for exploration  
- Prepared data for downstream **ESCO skill–qualification cosine similarity analysis**  

---

## 🗺️ Overview

This repository defines a **multi-stage data engineering pipeline** for the extraction, enrichment, and cleaning of Europass qualifications and learning opportunities across all EQF levels (1–8).

The pipeline performs:
- **Concurrent scraping** of the Europass QDR API.
- **Structured merging** by country and qualification type.
- **Hierarchical extraction** of metadata and learning outcomes.
- **Text normalization and AI-assisted cleaning** of descriptions.
- **Final dataset merging** for analytics and sharing.

Each file in the sequence has a numeric prefix (e.g., `00`, `300`, `500`) that defines the **execution order**.

---

## 🔢 Full Pipeline Sequence

| Step | Script | Purpose | Input | Output |
|------|---------|----------|--------|---------|
| 00 | `00save2.js` | Bulk concurrent fetch from Europass API by country & EQF level | Europass API | `/files/*.ndjson` |
| 01 | `01country_merge.js` | Merge EQF-level files into one per country | `/files` | `/countryFiles/<country>.ndjson` |
| 02 | `02fetchCountries.js` | Fetch full qualification JSONs per URI | `/countryFiles` | `/qualificationData` |
| 03 | `03extractData.js` | Extract structured qualification data into CSV | `/qualificationData` | `qualificationsValid.csv` |
| 10 | `10fetch_learningOpportunities.js` | Paginated fetch of learning opportunities | Europass API | `/output_learningOpportunities` |
| 11 | `11fetch_qualification.js` | Paginated fetch of qualifications | Europass API | `/output_qualifications` |
| 12 | `12scrape_europass.js` | Generic Europass scraper template | Europass API | `/output/` |
| 20 | `20scraper_opportunities.js` | Deep scrape of learning opportunity JSONs | `index.json` | `/json_learningOpportunities` |
| 21 | `21scraper_qualifications.js` | Deep scrape of qualification JSONs | `index.json` | `/json_qualifications` |
| 300 | `300extract_learningOpportunities.js` | Extracts learning opportunity JSONs into CSV | `/json_learningOpportunities` | `learning_opportunities_output.csv` |
| 301 | `301filter_learningOpportunities.js` | Filters incomplete rows from LO CSV | `learning_opportunities_output.csv` | `learning_opportunities_output_filtered.csv` |
| 302 | `302final_learningOpportunities.js` | Cleans, transforms & normalizes LO data | `learning_opportunities_output_filtered.csv` | `learning_opportunities_transformed.csv` |
| 303 | `303polished_learningOpportunities.js` | Cleans enumeration markers in text | `learning_opportunities_transformed.csv` | `learning_opportunities_nomore.csv` |
| 310 | `310extract_qualifications.js` | Extracts qualification JSONs into CSV | `/json_qualifications` | `output_all.csv` |
| 311 | `311filter_qualifications.js` | Filters and shards large qualification CSV | `output_all.csv` | `filtered_qualifications.csv` |
| 313 | `313clean_qualification.js` | Cleans noise, HTML, and garbage LOs | `filtered_qualifications.csv` | `cleaned_qualifications.csv` |
| 312 | `312final_qualifications.js` | Adds numeric EQF level column | `cleaned_qualifications.csv` | `final_qualifications.csv` |
| 400 | `400AIClean.js` | AI-assisted cleaning of text using OpenAI API | `final_qualifications.csv` or LO file | `*_cleaned.csv`, audit logs |
| 500 | `500mergeSharing.js` | Merges final LO and Qualification datasets | `learning_opportunities_nomore.csv`, `final_qualifications.csv` | `europass_combined.csv` |

---

## ⚙️ Detailed Stage Descriptions

### 🧩 Stage 300–303: Learning Opportunity CSV Processing

#### **300extract_learningOpportunities.js**
- Extracts structured CSV from raw JSON files (`/json_learningOpportunities`).
- Handles 100k+ files efficiently using:
  - A custom **Semaphore** for concurrency control.
  - Streamed CSV writing with **backpressure management**.
  - Minimal memory footprint.
- Fields extracted:
  - `title`
  - `countryCode.prefLabel`
  - `EQFLevel.prefLabel`
  - `learningOutcomeSummary.noteLiteral`
  - `learningOutcome` (JSON array)

**Output:** `learning_opportunities_output.csv`

---

#### **301filter_learningOpportunities.js**
- Filters out incomplete rows with missing core fields.
- Required columns:
  - `title`
  - `countryCode.prefLabel`
  - `EQFLevel.prefLabel`
  - `learningOutcomeSummary.noteLiteral`
  - `learningOutcome`
- Produces a filtered CSV retaining only valid entries.

**Output:** `learning_opportunities_output_filtered.csv`

---

#### **302final_learningOpportunities.js**
- Transforms filtered CSV into enriched, normalized dataset:
  - Adds derived column `EQFLevel_numeric` (extracted from text).
  - Combines all `learningOutcome_additionalNote` values into flattened strings.
- Includes fallback parsing for malformed JSON via regex.

**Output:** `learning_opportunities_transformed.csv`

---

#### **303polished_learningOpportunities.js**
- Performs **final text cleanup**:
  - Removes enumerations like `1)`, `2.`, `a)`, etc.
  - Normalizes multi-line text.
- Uses **PapaParse** for robust CSV parsing.
- Outputs a polished, publication-ready version.

**Output:** `learning_opportunities_nomore.csv`

---

### 🧩 Stage 310–313: Qualification CSV Processing

#### **310extract_qualifications.js**
- Extracts CSV from JSON qualification data.
- Extracted columns:
  - `qualificationName`
  - `country`
  - `qualificationLevel`
  - `description`
  - `learningOutcomes`
  - `entryRequirement`
- Includes full recursive directory traversal and concurrency control.

**Output:** `output_all.csv`

---

#### **311filter_qualifications.js**
- Filters rows for data completeness and optionally shards the dataset.
- Drops unnecessary columns (`entryRequirement`, `sourceFile`).
- Options:
  - Run with `rowsPerFile` argument to shard into smaller CSVs.
  - Example:
    ```bash
    node 311filter_qualifications.js output_all.csv filtered 10000
    ```
**Output:** `filtered_qualifications.csv` (or multiple shards)

---

#### **313clean_qualification.js**
- Cleans and normalizes qualification CSV data.
- Removes:
  - HTML tags and entities
  - Garbage LOs (e.g., “1.”, “NA NA”, or pure punctuation)
  - Redundant prefixes like “Title:”, “Notes:”
- Parses embedded JSON arrays in `learningOutcomes`.
- Collapses whitespace and merges title + notes fields.

**Output:** `cleaned_qualifications.csv`

---

#### **312final_qualifications.js**
- Adds a **numeric EQF level** column (`qualificationLevelNum`).
- Extracts number from strings like “Level 5” → `5`.
- Produces ready-to-use CSV for analytics.

**Output:** `final_qualifications.csv`

---

### 🧩 Stage 400: AI-Assisted Cleaning

#### **400AIClean.js**
- Integrates with **OpenAI GPT model** (`gpt-4o-mini`) for advanced text sanitization.
- Targets columns:
  - `description`
  - `learningOutcomes`
- Removes boilerplate or filler phrases such as:
  - “Please contact provider for more information”
  - “For more information, see website”
  - “National Qualification Framework (NQF)”
- Features:
  - **30 concurrent API requests**.
  - Checkpoint-based resumable design.
  - Generates:
    - `checkpoint.json`
    - `cleaned_<file>_audit.json`
    - `cleaned_<file>_errors.json`
- Safe, parallel OpenAI cleaning using JSON response enforcement.

**Output:** Cleaned CSV with AI-refined text.

---

### 🧩 Stage 500: Dataset Merging & Sharing

#### **500mergeSharing.js**
- Final dataset merger.
- Combines two CSVs (e.g., qualifications and learning opportunities) into a single, unified dataset.
- Skips duplicate headers automatically.
- Streams files efficiently (handles multi-GB merges).

Usage:
```bash
node 500mergeSharing.js learning_opportunities_nomore.csv final_qualifications.csv europass_combined.csv
```

        ┌──────────────────────────────┐
        │ Europass API (QDR Search)    │
        └──────────────┬───────────────┘
                       ▼
          [00–03] Qualification Fetch & Extraction
                       │
                       ▼
          [10–12] Learning Opportunity Fetchers
                       │
                       ▼
          [20–21] Deep Scrapers (JSON Level)
                       │
                       ▼
          [300–303] Learning Opportunity CSVs
                       │
                       ▼
          [310–313] Qualification CSVs
                       │
                       ▼
          [400] AI Cleaning (GPT-4o-mini)
                       │
                       ▼
          [500] Merge for Publication
                       │
                       ▼
             ✅ Final Dataset (CSV)

---

## 🚀 Execution Order Summary

```bash
node 00save2.js
node 01country_merge.js
node 02fetchCountries.js
node 03extractData.js
node 10fetch_learningOpportunities.js
node 11fetch_qualification.js
node 12scrape_europass.js
node 20scraper_opportunities.js
node 21scraper_qualifications.js
node 300extract_learningOpportunities.js
node 301filter_learningOpportunities.js
node 302final_learningOpportunities.js
node 303polished_learningOpportunities.js
node 310extract_qualifications.js
node 311filter_qualifications.js
node 313clean_qualification.js
node 312final_qualifications.js
node 400AIClean.js
node 500mergeSharing.js
```
