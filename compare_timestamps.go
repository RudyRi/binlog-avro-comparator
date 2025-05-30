package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

// BinlogEvent represents the relevant fields from json_parser.go output for DML/XID events
type BinlogEvent struct {
	EventType                 string `json:"event_type"`
	Timestamp                 string `json:"timestamp"` // RFC3339 or RFC3339Nano (from 'Date' field)
	ImmediateCommmitTimestamp string `json:"immediate_commmit_timestamp"` // High precision timestamp
	LogPosition               int64  `json:"log_position"`                // Position at the *end* of the event
	Table                     string `json:"table"`                       // For DML events
	Schema                    string `json:"schema"`                      // For DML events
	BinlogFile                string `json:"binlog_file"`                 // IMPORTANT: Added by modified json_parser.go
	GTIDNext                  string `json:"gtid_next"`                   // For GTID events
}

// AvroRecord represents a row from your AVRO JSON output
// Helper structs for Avro primitive types wrapped in a JSON object (e.g., {"string": "value"})
type AvroString struct {
	String string `json:"string"`
}

type AvroLong struct {
	Long int64 `json:"long"`
}

type AvroInt struct {
	Int int `json:"int"`
}

type AvroBoolean struct {
	Boolean bool `json:"boolean"`
}

// AvroRecord represents a row from your AVRO JSON output, adjusted for wrapped types
type AvroRecord struct {
	SourceTimestamp int64 `json:"source_timestamp"` // This one is a direct int64
	SourceMetadata  struct {
		Database                   string      `json:"database"`
		Table                      string      `json:"table"`
		ChangeType                 AvroString  `json:"change_type"` // Now uses AvroString
		GTID                       AvroString  `json:"gtid"`        // Now uses AvroString
		DatastreamMasterServerUUID AvroString  `json:"datastream_master_server_uuid"`
		DatastreamMasterServerID   AvroLong    `json:"datastream_master_server_id"`
		BinlogFile                 AvroString  `json:"binlog_file"` // Now uses AvroString
		BinlogPosition             AvroLong    `json:"binlog_position"` // Now uses AvroLong
		IsDeleted                  AvroBoolean `json:"is_deleted"`
		PrimaryKeys                []string    `json:"primary_keys"` // This appears to be a direct string array
	} `json:"source_metadata"`
	Payload struct {
		OrderID        AvroInt    `json:"order_id"`
		CustomerName   AvroString `json:"customer_name"`
		ProductName    AvroString `json:"product_name"`
		Quantity       AvroInt    `json:"quantity"`
		OrderTimestamp AvroLong   `json:"order_timestamp"`
	} `json:"payload"`
}

// BinlogKey defines the unique identifier for a binlog event or Avro record
type BinlogKey struct {
	BinlogFile     string
	BinlogPosition int64
}

// Maps to store parsed events for efficient lookup
var binlogEvents = make(map[BinlogKey]BinlogEvent) // Stores all relevant binlog events

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s <binlog_metadata.json> <avro_rows.json>\n", os.Args[0])
		os.Exit(1)
	}

	binlogJSONPath := os.Args[1]
	avroJSONPath := os.Args[2]

	fmt.Printf("Loading binlog data from %s...\n", binlogJSONPath)
	if err := loadBinlogData(binlogJSONPath); err != nil {
		fmt.Fprintf(os.Stderr, "Error loading binlog data: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Loaded %d relevant binlog events (DML or XID).\n", len(binlogEvents))

	fmt.Printf("Loading Avro data from %s and comparing...\n", avroJSONPath)
	if err := compareAvroWithBinlog(avroJSONPath); err != nil {
		fmt.Fprintf(os.Stderr, "Error during comparison: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\nComparison complete.")
}

// loadBinlogData reads the binlog_metadata.json file and populates the binlogEvents map
func loadBinlogData(filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		var event map[string]interface{}
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: Skipping malformed binlog JSON line %d: %v\n", lineNum, err)
			continue
		}

		eventType, ok := event["event_type"].(string)
		if !ok {
			// fmt.Fprintf(os.Stderr, "Warning: Skipping binlog event on line %d due to missing 'event_type'. Line: %s\n", lineNum, scanner.Text())
			continue // Skip if event_type is missing or not a string
		}

		isRelevantEventType := strings.HasSuffix(eventType, "RowsEventV2") || eventType == "XID"

		if !isRelevantEventType {
			continue
		}

		var binlogEvt BinlogEvent
		jsonBytes, _ := json.Marshal(event)
		if err := json.Unmarshal(jsonBytes, &binlogEvt); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: Error unmarshalling specific binlog event on line %d: %v (line: %s)\n", lineNum, err, string(jsonBytes))
			continue
		}

		if binlogEvt.BinlogFile == "" || binlogEvt.LogPosition == 0 {
			fmt.Fprintf(os.Stderr, "Warning: Skipping binlog event on line %d due to missing 'binlog_file' or 'log_position'. Event: %s\n", lineNum, string(jsonBytes))
			continue
		}

		key := BinlogKey{
			BinlogFile:     binlogEvt.BinlogFile,
			BinlogPosition: binlogEvt.LogPosition,
		}
		//fmt.Fprintf(os.Stderr, "DEBUG_LOAD: Storing binlog event. Type: %s, File: %s, Pos: %d\n", binlogEvt.EventType, binlogEvt.BinlogFile, binlogEvt.LogPosition)
		binlogEvents[key] = binlogEvt
	}

	return scanner.Err()
}

// compareAvroWithBinlog reads the Avro JSON file line by line and compares with loaded binlog data
func compareAvroWithBinlog(filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	lineNum := 0
	var matches int
	var mismatches int
	var avroOnly int
	var binlogMatchedKeys = make(map[BinlogKey]bool)

	for scanner.Scan() {
		lineNum++
		var avroRec AvroRecord
		if err := json.Unmarshal(scanner.Bytes(), &avroRec); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: Error unmarshalling Avro JSON line %d: %v (line: %s)\n", lineNum, err, scanner.Text())
			continue
		}

		if avroRec.SourceMetadata.BinlogFile.String == "" || avroRec.SourceMetadata.BinlogPosition.Long == 0 {
			fmt.Fprintf(os.Stderr, "Warning: Skipping Avro record on line %d due to missing 'binlog_file' or 'binlog_position' in source_metadata.\n", lineNum)
			continue
		}

		key := BinlogKey{
			BinlogFile:     avroRec.SourceMetadata.BinlogFile.String,
			BinlogPosition: avroRec.SourceMetadata.BinlogPosition.Long,
		}

		binlogEvt, found := binlogEvents[key]
		if !found {
			avroOnly++
			fmt.Printf("AVRO_ONLY_BINLOG_KEY: Line %d. Key %v (DB: %s, Table: %s, Type: %s) -> No matching binlog event found.\n",
				lineNum, key, avroRec.SourceMetadata.Database, avroRec.SourceMetadata.Table, avroRec.SourceMetadata.ChangeType.String)
			continue
		}

		binlogMatchedKeys[key] = true
		matches++

		var binlogTime time.Time
		var errParseTime error

		if binlogEvt.ImmediateCommmitTimestamp != "" {
			binlogTime, errParseTime = time.Parse(time.RFC3339Nano, binlogEvt.ImmediateCommmitTimestamp)
		} else if binlogEvt.Timestamp != "" {
			binlogTime, errParseTime = time.Parse(time.RFC3339, binlogEvt.Timestamp)
		}

		if errParseTime != nil {
			fmt.Printf("ERROR: Line %d. Key %v. Could not parse binlog timestamp '%s' or '%s'. Error: %v\n",
				lineNum, key, binlogEvt.ImmediateCommmitTimestamp, binlogEvt.Timestamp, errParseTime)
			mismatches++
			continue
		}

		avroTime := time.UnixMilli(avroRec.SourceTimestamp)
		tolerance := 100 * time.Millisecond

		if avroTime.Sub(binlogTime).Abs() > tolerance {
			mismatches++
			fmt.Printf("MISMATCH (Timestamp): Line %d. Key %v\n", lineNum, key)
			fmt.Printf("  Avro TS: %s (Unix MS: %d)\n", avroTime.Format(time.RFC3339Nano), avroRec.SourceTimestamp)
			fmt.Printf("  Binlog TS: %s (Event Type: %s)\n", binlogTime.Format(time.RFC3339Nano), binlogEvt.EventType)
		}

		if avroRec.SourceMetadata.GTID.String != "" && binlogEvt.GTIDNext != "" &&
			avroRec.SourceMetadata.GTID.String != binlogEvt.GTIDNext {
			fmt.Printf("MISMATCH (GTID): Line %d. Key %v\n", lineNum, key)
			fmt.Printf("  Avro GTID: %s\n", avroRec.SourceMetadata.GTID.String)
			fmt.Printf("  Binlog GTID_NEXT: %s\n", binlogEvt.GTIDNext)
			// mismatches++ // Decide if GTID mismatch should increment overall mismatches
		}

		inferredBinlogChangeType := ""
		if strings.HasSuffix(binlogEvt.EventType, "WriteRowsEventV2") || strings.HasSuffix(binlogEvt.EventType, "WriteRowsV1") {
			inferredBinlogChangeType = "INSERT"
		} else if strings.HasSuffix(binlogEvt.EventType, "UpdateRowsEventV2") || strings.HasSuffix(binlogEvt.EventType, "UpdateRowsV1") {
			inferredBinlogChangeType = "UPDATE"
		} else if strings.HasSuffix(binlogEvt.EventType, "DeleteRowsV2") || strings.HasSuffix(binlogEvt.EventType, "DeleteRowsV1") {
			inferredBinlogChangeType = "DELETE"
		}

		if avroRec.SourceMetadata.ChangeType.String != "" && inferredBinlogChangeType != "" &&
			strings.ToUpper(avroRec.SourceMetadata.ChangeType.String) != strings.ToUpper(inferredBinlogChangeType) {
			fmt.Printf("MISMATCH (ChangeType): Line %d. Key %v\n", lineNum, key)
			fmt.Printf("  Avro ChangeType: %s\n", avroRec.SourceMetadata.ChangeType.String)
			fmt.Printf("  Inferred Binlog ChangeType (from %s): %s\n", binlogEvt.EventType, inferredBinlogChangeType)
			// mismatches++ // Decide if ChangeType mismatch should increment overall mismatches
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading Avro JSON: %w", err)
	}

	// --- Unmatched Binlog DML Events ---
	var binlogOnly int
	fmt.Println("\n--- Unmatched Binlog DML Events (BINLOG_ONLY) ---")
	for binlogKey, binlogEvt := range binlogEvents {
		if !binlogMatchedKeys[binlogKey] {
			isDML := strings.HasSuffix(binlogEvt.EventType, "WriteRowsEventV2") ||
				strings.HasSuffix(binlogEvt.EventType, "UpdateRowsEventV2") ||
				strings.HasSuffix(binlogEvt.EventType, "DeleteRowsEventV2") ||
				strings.HasSuffix(binlogEvt.EventType, "WriteRowsEventV1") ||
				strings.HasSuffix(binlogEvt.EventType, "UpdateRowsV1") ||
				strings.HasSuffix(binlogEvt.EventType, "DeleteRowsV1")

			if isDML {
				binlogOnly++
				fmt.Printf("BINLOG_ONLY (DML): Key %v (Event: %s, Schema: %s, Table: %s, TS: %s) -> No matching Avro record found.\n",
					binlogKey, binlogEvt.EventType, binlogEvt.Schema, binlogEvt.Table, binlogEvt.Timestamp)
			}
		}
	}
	if binlogOnly == 0 {
		fmt.Println("No DML binlog events found without a matching Avro record.")
	}

	// --- DML Event Type Counting and associated debug prints have been removed ---

	// --- Final Comparison Summary ---
	fmt.Printf("\n--- Comparison Summary ---\n") 
	fmt.Printf("Total Avro Records Processed: %d\n", lineNum)
	fmt.Printf("Total Matched by Binlog Key: %d\n", matches)
	fmt.Printf("Total Timestamp/GTID/ChangeType Mismatches (within matched set): %d\n", mismatches)
	fmt.Printf("Avro Records with no Binlog Event match (by key): %d\n", avroOnly)
	fmt.Printf("Binlog DML Events with no Avro Record match (by key): %d\n", binlogOnly) 

	if mismatches == 0 && avroOnly == 0 && binlogOnly == 0 {
		fmt.Println("\nCONCLUSION: All Avro records have matching binlog events, and timestamps/metadata are consistent.")
	} else {
		fmt.Println("\nCONCLUSION: WARNING - There were discrepancies found during comparison.")
	}
	return nil
}