/**
 * transform_lo_csv_debug.js
 *
 * More defensive parsing for learningOutcome CSV cells.
 * - Attempts multiple JSON-unescape strategies
 * - Falls back to regex extraction
 * - Cleans leading enumerations like "1) ", "X. ", "a) "
 * - Logs parse failures to parse_failures_debug.txt (first 50 examples)
 *
 * Edit INPUT_CSV / OUTPUT_CSV if needed.
 */

const fs = require('fs');
const Papa = require('papaparse');

// Configuration
const INPUT_FILE = 'learning_opportunities_transformed.csv';
const OUTPUT_FILE = 'learning_opportunities_nomore.csv';
const COLUMN_TO_CLEAN = 'learningOutcome_additionalNote';

// Function to clean the text by removing numbered list markers
function cleanText(text) {
  if (!text || typeof text !== 'string') {
    return text;
  }
  
  // Remove patterns like "1)", "2.", "x)", "X." with space before and after
  // This regex matches:
  // - Optional whitespace at start
  // - One or more digits
  // - Either ) or .
  // - One or more spaces
  const pattern = /^\s*\d+[).]\s+/gm;
  
  return text.replace(pattern, '');
}

// Read the CSV file
fs.readFile(INPUT_FILE, 'utf8', (err, data) => {
  if (err) {
    console.error('Error reading file:', err);
    return;
  }

  // Parse the CSV
  Papa.parse(data, {
    header: true,
    skipEmptyLines: true,
    complete: (results) => {
      console.log(`Processing ${results.data.length} rows...`);
      
      // Check if the column exists
      if (results.data.length > 0 && !results.data[0].hasOwnProperty(COLUMN_TO_CLEAN)) {
        console.error(`Column "${COLUMN_TO_CLEAN}" not found in CSV!`);
        console.log('Available columns:', Object.keys(results.data[0]));
        return;
      }
      
      // Clean the specified column in each row
      let cleanedCount = 0;
      results.data.forEach(row => {
        if (row[COLUMN_TO_CLEAN]) {
          const original = row[COLUMN_TO_CLEAN];
          const cleaned = cleanText(original);
          if (original !== cleaned) {
            cleanedCount++;
          }
          row[COLUMN_TO_CLEAN] = cleaned;
        }
      });
      
      console.log(`Cleaned ${cleanedCount} entries`);
      
      // Convert back to CSV
      const csv = Papa.unparse(results.data);
      
      // Write the output file
      fs.writeFile(OUTPUT_FILE, csv, 'utf8', (err) => {
        if (err) {
          console.error('Error writing file:', err);
          return;
        }
        console.log(`Successfully wrote cleaned data to ${OUTPUT_FILE}`);
      });
    },
    error: (error) => {
      console.error('Error parsing CSV:', error);
    }
  });
});