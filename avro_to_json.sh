#!/bin/bash

# --- Configuration ---
# Get the directory where the script itself is located.
# This makes the script work correctly no matter where you run it from,
# as long as avro-tools.jar is in the same directory as this script.
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")

# Path to your avro-tools.jar
# Assumes avro-tools.jar is in the same directory as this script.
AVRO_TOOLS_JAR="${SCRIPT_DIR}/avro-tools-1.11.1.jar"

# Directory where your .avro files are located
# This uses your specified path in the home directory.
AVRO_FILES_DIRECTORY="$HOME/avro_flat"

# Name of the output file for combined JSON Lines
OUTPUT_JSON_FILE="avro_rows.json"

# --- Pre-flight Checks ---
echo "Starting Avro to JSON conversion and concatenation..."
echo "Avro tools JAR: ${AVRO_TOOLS_JAR}"
echo "Avro files source directory: ${AVRO_FILES_DIRECTORY}"
echo "Output file: ${OUTPUT_JSON_FILE}"

# Check if avro-tools.jar exists
if ! ls "${AVRO_TOOLS_JAR}" > /dev/null 2>&1; then
  echo "ERROR: avro-tools.jar not found at '${AVRO_TOOLS_JAR}'."
  echo "Please ensure the JAR file is in the script's directory and its name matches the wildcard pattern (e.g., avro-tools-1.11.1.jar)."
  exit 1
fi

# Check if the Avro files directory exists
if [ ! -d "${AVRO_FILES_DIRECTORY}" ]; then
  echo "ERROR: Avro files directory not found: '${AVRO_FILES_DIRECTORY}'"
  echo "Please create this directory or adjust the AVRO_FILES_DIRECTORY variable in the script."
  exit 1
fi

# --- Processing ---

# 1. Remove any existing output file to start fresh
echo "Removing existing '${OUTPUT_JSON_FILE}' if it exists..."
rm -f "${OUTPUT_JSON_FILE}"

# Initialize counters in the main shell
processed_files=0
converted_successfully=0

# 2. Loop through each .avro file directly. This avoids the subshell issue.
#    The `"${AVRO_FILES_DIRECTORY}"/*.avro` will expand to all matching files.
for avro_file in "${AVRO_FILES_DIRECTORY}"/*.avro; do
  # Check if the file actually exists and is a regular file.
  # This handles the case where no .avro files are found (the glob expands to literal "*.avro").
  if [ -f "$avro_file" ]; then
    processed_files=$((processed_files + 1))
    echo "  Converting '$avro_file'..."

    # Convert the current .avro file to JSON Lines and append its output
    java -jar "${AVRO_TOOLS_JAR}" tojson "$avro_file" >> "${OUTPUT_JSON_FILE}"

    # Check the exit status of the last command (`java -jar`)
    if [ $? -ne 0 ]; then
      echo "  ERROR: Failed to convert '$avro_file'. The combined '${OUTPUT_JSON_FILE}' might be incomplete."
      echo "  Please check the Avro file's integrity or your Java environment."
      exit 1 # Exit with a non-zero status to indicate failure
    fi
    converted_successfully=$((converted_successfully + 1))
  fi
done

# --- Summary ---
echo ""
echo "--- Conversion Summary ---"
if [ "$processed_files" -eq 0 ]; then
  echo "No .avro files were found and processed in '${AVRO_FILES_DIRECTORY}'."
  echo "Please ensure your .avro files are in the specified directory and match the '*.avro' pattern."
else
  echo "Total .avro files found and attempted to process: ${processed_files}"
  echo "Successfully converted: ${converted_successfully}"
  if [ "$processed_files" -ne "$converted_successfully" ]; then
    echo "WARNING: Some files might have failed conversion or were not regular files."
  fi
  echo "Combined JSON output saved to: ${OUTPUT_JSON_FILE}"
fi

echo ""
echo "You can now run the comparison script with ${OUTPUT_JSON_FILE}."
echo "Example: ./compare_timestamps binlog_metadata.json ${OUTPUT_JSON_FILE}"
