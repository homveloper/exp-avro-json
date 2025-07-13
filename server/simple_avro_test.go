package main

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/linkedin/goavro/v2"
)

// Simple test structure
type SimpleUser struct {
	ID       int64             `json:"id"`
	Name     string            `json:"name"`
	Email    string            `json:"email"`
	Active   bool              `json:"active"`
	Tags     []string          `json:"tags"`
	Settings map[string]string `json:"settings"`
}

func TestAvroVisualization(t *testing.T) {
	fmt.Println("\n🔍 === Avro Serialization Visualization ===")

	// 1. Create simple test data
	testUser := SimpleUser{
		ID:     12345,
		Name:   "홍길동",
		Email:  "hong@example.com",
		Active: true,
		Tags:   []string{"admin", "premium"},
		Settings: map[string]string{
			"theme":    "dark",
			"language": "ko",
		},
	}

	fmt.Printf("\n📊 Original Go Struct:\n")
	fmt.Printf("%+v\n", testUser)

	// 2. Convert to JSON (baseline)
	jsonBytes, _ := json.Marshal(testUser)
	fmt.Printf("\n📄 Standard JSON (%d bytes):\n", len(jsonBytes))
	fmt.Printf("%s\n", string(jsonBytes))

	// 3. Define simple Avro schema
	avroSchema := `{
		"type": "record",
		"name": "SimpleUser",
		"fields": [
			{"name": "id", "type": "long"},
			{"name": "name", "type": "string"},
			{"name": "email", "type": "string"},
			{"name": "active", "type": "boolean"},
			{"name": "tags", "type": {"type": "array", "items": "string"}},
			{"name": "settings", "type": {"type": "map", "values": "string"}}
		]
	}`

	fmt.Printf("\n🏗️  Avro Schema:\n%s\n", avroSchema)

	// 4. Create Avro codec
	codec, err := goavro.NewCodec(avroSchema)
	if err != nil {
		t.Fatalf("Failed to create codec: %v", err)
	}

	// 5. Convert struct to map (Native format for Avro)
	nativeData := map[string]interface{}{
		"id":       testUser.ID,
		"name":     testUser.Name,
		"email":    testUser.Email,
		"active":   testUser.Active,
		"tags":     testUser.Tags,
		"settings": testUser.Settings,
	}

	fmt.Printf("\n🗺️  Native Data (map[string]interface{}):\n")
	for k, v := range nativeData {
		fmt.Printf("  %s: %v (type: %T)\n", k, v, v)
	}

	// 6. BinaryFromNative - Serialize to Avro binary
	fmt.Printf("\n⚙️  Step 1: BinaryFromNative\n")
	binaryData, err := codec.BinaryFromNative(nil, nativeData)
	if err != nil {
		t.Fatalf("BinaryFromNative failed: %v", err)
	}
	fmt.Printf("Binary size: %d bytes\n", len(binaryData))
	fmt.Printf("Binary data (hex): %x\n", binaryData[:min(50, len(binaryData))])
	if len(binaryData) > 50 {
		fmt.Printf("... (truncated, total %d bytes)\n", len(binaryData))
	}

	// 7. NativeFromBinary - Deserialize from Avro binary
	fmt.Printf("\n⚙️  Step 2: NativeFromBinary\n")
	decodedNative, _, err := codec.NativeFromBinary(binaryData)
	if err != nil {
		t.Fatalf("NativeFromBinary failed: %v", err)
	}
	fmt.Printf("Decoded native data:\n")
	for k, v := range decodedNative.(map[string]interface{}) {
		fmt.Printf("  %s: %v (type: %T)\n", k, v, v)
	}

	// 8. TextualFromNative - Convert to Avro JSON
	fmt.Printf("\n⚙️  Step 3: TextualFromNative\n")
	avroJSONBytes, err := codec.TextualFromNative(nil, decodedNative)
	if err != nil {
		t.Fatalf("TextualFromNative failed: %v", err)
	}
	fmt.Printf("Avro JSON size: %d bytes\n", len(avroJSONBytes))
	fmt.Printf("Avro JSON:\n%s\n", string(avroJSONBytes))

	// 9. NativeFromTextual - Convert back from Avro JSON
	fmt.Printf("\n⚙️  Step 4: NativeFromTextual (reverse)\n")
	nativeFromJSON, _, err := codec.NativeFromTextual(avroJSONBytes)
	if err != nil {
		t.Fatalf("NativeFromTextual failed: %v", err)
	}
	fmt.Printf("Native from JSON:\n")
	for k, v := range nativeFromJSON.(map[string]interface{}) {
		fmt.Printf("  %s: %v (type: %T)\n", k, v, v)
	}

	// 10. Compression comparison
	fmt.Printf("\n📊 Size Comparison:\n")
	fmt.Printf("Original JSON:     %d bytes (100.0%%)\n", len(jsonBytes))
	fmt.Printf("Avro Binary:       %d bytes (%.1f%%)\n", len(binaryData),
		float64(len(binaryData))/float64(len(jsonBytes))*100)
	fmt.Printf("Avro JSON:         %d bytes (%.1f%%)\n", len(avroJSONBytes),
		float64(len(avroJSONBytes))/float64(len(jsonBytes))*100)

	if len(binaryData) < len(jsonBytes) {
		savings := len(jsonBytes) - len(binaryData)
		fmt.Printf("🎉 Binary compression: %d bytes saved (%.1f%% reduction)\n",
			savings, (1.0-float64(len(binaryData))/float64(len(jsonBytes)))*100)
	} else {
		fmt.Printf("⚠️  Binary overhead: %d bytes larger\n", len(binaryData)-len(jsonBytes))
	}

	// 11. Data integrity check
	fmt.Printf("\n✅ Data Integrity Check:\n")
	originalName := testUser.Name
	decodedName := decodedNative.(map[string]interface{})["name"].(string)
	fmt.Printf("Original name: %s\n", originalName)
	fmt.Printf("Decoded name:  %s\n", decodedName)
	fmt.Printf("Names match:   %t\n", originalName == decodedName)

	fmt.Printf("\n🎯 === Avro Visualization Complete ===\n")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Run with: go test -run TestAvroVisualization -v
