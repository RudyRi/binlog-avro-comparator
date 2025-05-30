package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath" // <<< IMPORT THIS PACKAGE
	"regexp"
	"strconv"
	"strings"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <current_binlog_filename>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "This program reads binlog text parser output from stdin and converts it to JSON.\n")
		os.Exit(1)
	}
	// currentBinlogFileFromArg will be the full path, e.g., /home/rudyrivera/my_mysql_binlogs/mysql-bin.000001
	currentBinlogFileFromArg := os.Args[1]
	// Extract just the filename (basename) to be stored in the JSON
	binlogFileBasename := filepath.Base(currentBinlogFileFromArg) // <<< USE FILEPATH.BASE

	scanner := bufio.NewScanner(os.Stdin)
	var currentEvent map[string]interface{}

	eventHeaderRegex := regexp.MustCompile(`^=== (.+?) ===$`)
	keyValueRegex := regexp.MustCompile(`^([^:]+): (.+)$`)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if len(line) == 0 {
			continue
		}

		if line == "--" {
			continue
		}

		if matches := eventHeaderRegex.FindStringSubmatch(line); len(matches) > 1 {
			if currentEvent != nil {
				currentEvent["binlog_file"] = binlogFileBasename // <<< STORE BASENAME
				jsonOutput, err := json.Marshal(currentEvent)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error marshalling event to JSON: %v\n", err)
				} else {
					fmt.Println(string(jsonOutput))
				}
			}
			currentEvent = make(map[string]interface{})

			headerEventType := matches[1]
			var parsedEventType string
			if strings.Contains(headerEventType, "WriteRowsEventV2") {
				parsedEventType = "WriteRowsEventV2"
			} else if strings.Contains(headerEventType, "UpdateRowsEventV2") {
				parsedEventType = "UpdateRowsEventV2"
			} else if strings.Contains(headerEventType, "DeleteRowsEventV2") {
				parsedEventType = "DeleteRowsEventV2"
			} else {
				parsedEventType = strings.TrimSuffix(headerEventType, "Event")
			}
			currentEvent["event_type"] = parsedEventType
			continue
		}

		if currentEvent == nil {
			continue
		}

		if matches := keyValueRegex.FindStringSubmatch(line); len(matches) > 2 {
			key := strings.TrimSpace(matches[1])
			value := strings.TrimSpace(matches[2])
			normalizedKey := strings.ToLower(strings.ReplaceAll(key, " ", "_"))

			switch key {
			case "Date":
				t, err := time.Parse("2006-01-02 15:04:05", value)
				if err == nil {
					currentEvent["timestamp"] = t.Format(time.RFC3339)
				} else {
					fmt.Fprintf(os.Stderr, "Warning: Failed to parse 'Date' timestamp '%s': %v for key '%s'\n", value, err, key)
					currentEvent[normalizedKey] = value
				}
			case "Log position":
				if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
					currentEvent["log_position"] = intVal
				} else {
					fmt.Fprintf(os.Stderr, "Warning: Failed to parse 'Log position' '%s': %v\n", value, err)
					currentEvent[normalizedKey] = value
				}
			case "Table", "Schema", "Query", "XID", "GTID_NEXT", "Commit flag", "LAST_COMMITTED", "SEQUENCE_NUMBER", "Transaction length", "Immediate server version", "Orignal server version", "TableID", "Flags", "Column count", "Slave proxy ID", "Execution time", "Error code", "server_version", "version":
				if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
					currentEvent[normalizedKey] = intVal
				} else {
					currentEvent[normalizedKey] = value
				}

			case "Immediate commmit timestamp", "Orignal commmit timestamp":
				if strings.Contains(value, "(") && strings.HasSuffix(value, "Z)") {
					startIndex := strings.LastIndex(value, "(")
					extractedTimestamp := value[startIndex+1 : len(value)-1]
					_, err := time.Parse(time.RFC3339Nano, extractedTimestamp)
					if err == nil {
						currentEvent[normalizedKey] = extractedTimestamp
					} else {
						fmt.Fprintf(os.Stderr, "Warning: Could not parse extracted RFC3339Nano ('%s') from '%s' for key '%s': %v\n", extractedTimestamp, value, key, err)
						currentEvent[normalizedKey] = value
					}
				} else {
					const binlogHighPrecisionLayout = "2006-01-02 15:04:05.999999999 -0700 MST"
					t, err := time.Parse(binlogHighPrecisionLayout, value)
					if err == nil {
						currentEvent[normalizedKey] = t.Format(time.RFC3339Nano)
					} else {
						fmt.Fprintf(os.Stderr, "Warning: Failed to parse high-precision timestamp '%s' with known layouts for key '%s': %v\n", value, key, err)
						currentEvent[normalizedKey] = value
					}
				}

			case "Event type":
				if strings.Contains(value, "WriteRowsEventV2") {
					currentEvent["event_type"] = "WriteRowsEventV2"
				} else if strings.Contains(value, "UpdateRowsEventV2") {
					currentEvent["event_type"] = "UpdateRowsEventV2"
				} else if strings.Contains(value, "DeleteRowsEventV2") {
					currentEvent["event_type"] = "DeleteRowsEventV2"
				}

			default:
				if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
					currentEvent[normalizedKey] = intVal
				} else {
					currentEvent[normalizedKey] = value
				}
			}
		}
	}

	if currentEvent != nil {
		currentEvent["binlog_file"] = binlogFileBasename // <<< STORE BASENAME
		jsonOutput, err := json.Marshal(currentEvent)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error marshalling event to JSON: %v\n", err)
		} else {
			fmt.Println(string(jsonOutput))
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading stdin: %v\n", err)
	}
}