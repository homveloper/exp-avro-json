package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/linkedin/goavro/v2"
)

// AvroContainer represents Avro JSON format with embedded schema
type AvroContainer struct {
	Schema string      `json:"schema"`
	Data   interface{} `json:"data"`
}

// AvroFileFormat represents complete Avro file with metadata
type AvroFileFormat struct {
	Metadata struct {
		Timestamp   int64  `json:"timestamp"`
		Version     string `json:"version"`
		Compression string `json:"compression"`
		Writer      string `json:"writer"`
	} `json:"metadata"`
	Schema string      `json:"schema"`
	Data   interface{} `json:"data"`
}

func TestAvroJSONWithSchema(t *testing.T) {
	fmt.Println("\n🗂️  === Avro JSON + Schema Storage Test ===")

	// Create test data
	testData := map[string]interface{}{
		"id":     int64(12345),
		"name":   "김철수",
		"email":  "kim@example.com",
		"active": true,
		"tags":   []string{"vip", "korea"},
		"score":  95.5,
	}

	// Define schema
	schema := `{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "id", "type": "long"},
			{"name": "name", "type": "string"},
			{"name": "email", "type": "string"},
			{"name": "active", "type": "boolean"},
			{"name": "tags", "type": {"type": "array", "items": "string"}},
			{"name": "score", "type": "double"}
		]
	}`

	fmt.Printf("📊 Test Data: %+v\n", testData)
	fmt.Printf("📋 Schema: %s\n", schema)

	// Create logs directory
	logsDir := "avro-schema-logs"
	os.MkdirAll(logsDir, 0755)
	timestamp := time.Now().Format("20060102_150405")

	// Method 1: Simple JSON + Schema container
	fmt.Printf("\n🔄 Method 1: Simple JSON + Schema Container\n")

	container := AvroContainer{
		Schema: schema,
		Data:   testData,
	}

	containerJSON, _ := json.MarshalIndent(container, "", "  ")
	containerFile := filepath.Join(logsDir, fmt.Sprintf("simple_container_%s.json", timestamp))
	os.WriteFile(containerFile, containerJSON, 0644)

	fmt.Printf("Container size: %d bytes\n", len(containerJSON))
	fmt.Printf("Saved to: %s\n", containerFile)

	// Method 2: Complete Avro file format with metadata
	fmt.Printf("\n🔄 Method 2: Complete Avro File Format\n")

	avroFile := AvroFileFormat{
		Schema: schema,
		Data:   testData,
	}
	avroFile.Metadata.Timestamp = time.Now().Unix()
	avroFile.Metadata.Version = "1.0"
	avroFile.Metadata.Compression = "none"
	avroFile.Metadata.Writer = "go-avro-experiment"

	avroFileJSON, _ := json.MarshalIndent(avroFile, "", "  ")
	avroFileFile := filepath.Join(logsDir, fmt.Sprintf("complete_avro_%s.json", timestamp))
	os.WriteFile(avroFileFile, avroFileJSON, 0644)

	fmt.Printf("Complete file size: %d bytes\n", len(avroFileJSON))
	fmt.Printf("Saved to: %s\n", avroFileFile)

	// Method 3: Avro JSON (TextualFromNative) + separate schema
	fmt.Printf("\n🔄 Method 3: Avro JSON + Separate Schema\n")

	codec, err := goavro.NewCodec(schema)
	if err != nil {
		t.Fatalf("Failed to create codec: %v", err)
	}

	// Convert to Avro JSON
	avroJSON, err := codec.TextualFromNative(nil, testData)
	if err != nil {
		t.Fatalf("Failed to convert to Avro JSON: %v", err)
	}

	// Save Avro JSON data
	avroJSONFile := filepath.Join(logsDir, fmt.Sprintf("avro_data_%s.json", timestamp))
	os.WriteFile(avroJSONFile, avroJSON, 0644)

	// Save schema separately
	schemaFile := filepath.Join(logsDir, fmt.Sprintf("schema_%s.json", timestamp))
	os.WriteFile(schemaFile, []byte(schema), 0644)

	fmt.Printf("Avro JSON size: %d bytes\n", len(avroJSON))
	fmt.Printf("Schema size: %d bytes\n", len(schema))
	fmt.Printf("Total size: %d bytes\n", len(avroJSON)+len(schema))
	fmt.Printf("Data saved to: %s\n", avroJSONFile)
	fmt.Printf("Schema saved to: %s\n", schemaFile)

	// Method 4: Avro Binary + embedded schema info
	fmt.Printf("\n🔄 Method 4: Avro Binary + Schema Info\n")

	binary, err := codec.BinaryFromNative(nil, testData)
	if err != nil {
		t.Fatalf("Failed to convert to binary: %v", err)
	}

	binaryWithSchema := struct {
		Schema string `json:"schema"`
		Binary []byte `json:"binary"`
	}{
		Schema: schema,
		Binary: binary,
	}

	binaryJSON, _ := json.MarshalIndent(binaryWithSchema, "", "  ")
	binaryFile := filepath.Join(logsDir, fmt.Sprintf("binary_with_schema_%s.json", timestamp))
	os.WriteFile(binaryFile, binaryJSON, 0644)

	fmt.Printf("Binary size: %d bytes\n", len(binary))
	fmt.Printf("Binary + Schema JSON: %d bytes\n", len(binaryJSON))
	fmt.Printf("Saved to: %s\n", binaryFile)

	// Comparison
	fmt.Printf("\n📊 Size Comparison:\n")
	originalJSON, _ := json.Marshal(testData)
	fmt.Printf("Original JSON:           %d bytes (100.0%%)\n", len(originalJSON))
	fmt.Printf("Simple Container:        %d bytes (%.1f%%)\n", len(containerJSON),
		float64(len(containerJSON))/float64(len(originalJSON))*100)
	fmt.Printf("Complete Avro File:      %d bytes (%.1f%%)\n", len(avroFileJSON),
		float64(len(avroFileJSON))/float64(len(originalJSON))*100)
	fmt.Printf("Avro JSON + Schema:      %d bytes (%.1f%%)\n", len(avroJSON)+len(schema),
		float64(len(avroJSON)+len(schema))/float64(len(originalJSON))*100)
	fmt.Printf("Binary + Schema JSON:    %d bytes (%.1f%%)\n", len(binaryJSON),
		float64(len(binaryJSON))/float64(len(originalJSON))*100)
	fmt.Printf("Pure Avro Binary:        %d bytes (%.1f%%)\n", len(binary),
		float64(len(binary))/float64(len(originalJSON))*100)

	// Test reading back
	fmt.Printf("\n✅ Reading Back Test:\n")

	// Read simple container
	containerData, _ := os.ReadFile(containerFile)
	var readContainer AvroContainer
	json.Unmarshal(containerData, &readContainer)

	// Verify schema and recreate codec
	_, err = goavro.NewCodec(readContainer.Schema)
	if err != nil {
		t.Fatalf("Failed to create codec from read schema: %v", err)
	}

	// Verify data integrity
	readJSON, _ := json.Marshal(readContainer.Data)
	fmt.Printf("Original data: %s\n", string(originalJSON))
	fmt.Printf("Read data:     %s\n", string(readJSON))
	fmt.Printf("Data integrity: %t\n", string(originalJSON) == string(readJSON))

	// Show schema is preserved
	fmt.Printf("Schema preserved: %t\n", readContainer.Schema == schema)

	fmt.Printf("\n🎯 === JSON + Schema Storage Test Complete ===\n")
	fmt.Printf("📁 All files saved to: %s/\n", logsDir)
}

// Test reading an Avro file with embedded schema
func TestReadAvroWithSchema(t *testing.T) {
	fmt.Println("\n📖 === Reading Avro File with Schema ===")

	// Simulate reading a file that was saved with schema
	sampleData := `{
		"metadata": {
			"timestamp": 1673456789,
			"version": "1.0", 
			"compression": "none",
			"writer": "go-avro-experiment"
		},
		"schema": "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"name\",\"type\":\"string\"}]}",
		"data": {
			"id": 12345,
			"name": "테스트 사용자"
		}
	}`

	var avroFile AvroFileFormat
	err := json.Unmarshal([]byte(sampleData), &avroFile)
	if err != nil {
		t.Fatalf("Failed to parse Avro file: %v", err)
	}

	fmt.Printf("📋 Metadata: %+v\n", avroFile.Metadata)
	fmt.Printf("🗂️  Schema: %s\n", avroFile.Schema)
	fmt.Printf("📊 Data: %+v\n", avroFile.Data)

	// Create codec from embedded schema
	codec, err := goavro.NewCodec(avroFile.Schema)
	if err != nil {
		t.Fatalf("Failed to create codec: %v", err)
	}

	// Validate data against schema
	binary, err := codec.BinaryFromNative(nil, avroFile.Data)
	if err != nil {
		t.Fatalf("Data doesn't match schema: %v", err)
	}

	fmt.Printf("✅ Schema validation: PASSED\n")
	fmt.Printf("🗜️  Binary size: %d bytes\n", len(binary))

	// Verify round-trip
	native, _, _ := codec.NativeFromBinary(binary)
	fmt.Printf("🔄 Round-trip test: %+v\n", native)
}

// Run with: go test -run TestAvroJSONWithSchema -v
// Run with: go test -run TestReadAvroWithSchema -v
