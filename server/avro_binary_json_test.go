package main

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/linkedin/goavro/v2"
)

// Different ways to store Avro binary in JSON
type AvroBinaryContainer struct {
	Schema    string `json:"schema"`
	BinaryHex string `json:"binary_hex"`
	BinaryB64 string `json:"binary_base64"`
	BinaryArr []byte `json:"binary_array"`
	Metadata  Meta   `json:"metadata"`
}

type Meta struct {
	Timestamp int64  `json:"timestamp"`
	Size      int    `json:"size"`
	Format    string `json:"format"`
}

func TestAvroBinaryInJSON(t *testing.T) {
	fmt.Println("\n💾 === Avro Binary in JSON Storage Test ===")

	// Test data
	testData := map[string]interface{}{
		"user_id":  int64(98765),
		"username": "avro_user",
		"email":    "user@avro.test",
		"active":   true,
		"score":    87.5,
		"tags":     []string{"binary", "test", "json"},
		"metadata": map[string]string{
			"region": "seoul",
			"tier":   "premium",
		},
	}

	schema := `{
		"type": "record",
		"name": "UserRecord",
		"fields": [
			{"name": "user_id", "type": "long"},
			{"name": "username", "type": "string"},
			{"name": "email", "type": "string"},
			{"name": "active", "type": "boolean"},
			{"name": "score", "type": "double"},
			{"name": "tags", "type": {"type": "array", "items": "string"}},
			{"name": "metadata", "type": {"type": "map", "values": "string"}}
		]
	}`

	fmt.Printf("📊 Original data: %+v\n", testData)

	// Create codec and convert to binary
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		t.Fatalf("Failed to create codec: %v", err)
	}

	binary, err := codec.BinaryFromNative(nil, testData)
	if err != nil {
		t.Fatalf("Failed to convert to binary: %v", err)
	}

	fmt.Printf("🗜️  Avro binary size: %d bytes\n", len(binary))
	fmt.Printf("🔍 Binary (first 20 bytes): %x\n", binary[:min(20, len(binary))])

	// Method 1: Store as Hex string
	fmt.Printf("\n🔄 Method 1: Binary as Hex String\n")
	hexString := hex.EncodeToString(binary)
	hexContainer := map[string]interface{}{
		"schema":     schema,
		"binary_hex": hexString,
		"metadata": map[string]interface{}{
			"encoding": "hex",
			"size":     len(binary),
		},
	}
	hexJSON, _ := json.MarshalIndent(hexContainer, "", "  ")
	fmt.Printf("Hex JSON size: %d bytes\n", len(hexJSON))
	fmt.Printf("Hex string length: %d chars\n", len(hexString))

	// Method 2: Store as Base64 string
	fmt.Printf("\n🔄 Method 2: Binary as Base64 String\n")
	base64String := base64.StdEncoding.EncodeToString(binary)
	base64Container := map[string]interface{}{
		"schema":        schema,
		"binary_base64": base64String,
		"metadata": map[string]interface{}{
			"encoding": "base64",
			"size":     len(binary),
		},
	}
	base64JSON, _ := json.MarshalIndent(base64Container, "", "  ")
	fmt.Printf("Base64 JSON size: %d bytes\n", len(base64JSON))
	fmt.Printf("Base64 string length: %d chars\n", len(base64String))

	// Method 3: Store as byte array
	fmt.Printf("\n🔄 Method 3: Binary as Byte Array\n")
	arrayContainer := map[string]interface{}{
		"schema":       schema,
		"binary_array": binary,
		"metadata": map[string]interface{}{
			"encoding": "byte_array",
			"size":     len(binary),
		},
	}
	arrayJSON, _ := json.MarshalIndent(arrayContainer, "", "  ")
	fmt.Printf("Array JSON size: %d bytes\n", len(arrayJSON))

	// Method 4: Complete container with all formats
	fmt.Printf("\n🔄 Method 4: Complete Container (All Formats)\n")
	completeContainer := AvroBinaryContainer{
		Schema:    schema,
		BinaryHex: hexString,
		BinaryB64: base64String,
		BinaryArr: binary,
		Metadata: Meta{
			Timestamp: time.Now().Unix(),
			Size:      len(binary),
			Format:    "avro_binary",
		},
	}
	completeJSON, _ := json.MarshalIndent(completeContainer, "", "  ")
	fmt.Printf("Complete JSON size: %d bytes\n", len(completeJSON))

	// Save all formats
	logsDir := "avro-binary-json"
	os.MkdirAll(logsDir, 0755)
	timestamp := time.Now().Format("20060102_150405")

	hexFile := filepath.Join(logsDir, fmt.Sprintf("hex_%s.json", timestamp))
	base64File := filepath.Join(logsDir, fmt.Sprintf("base64_%s.json", timestamp))
	arrayFile := filepath.Join(logsDir, fmt.Sprintf("array_%s.json", timestamp))
	completeFile := filepath.Join(logsDir, fmt.Sprintf("complete_%s.json", timestamp))

	os.WriteFile(hexFile, hexJSON, 0644)
	os.WriteFile(base64File, base64JSON, 0644)
	os.WriteFile(arrayFile, arrayJSON, 0644)
	os.WriteFile(completeFile, completeJSON, 0644)

	// Size comparison
	fmt.Printf("\n📊 Size Comparison:\n")
	originalJSON, _ := json.Marshal(testData)
	fmt.Printf("Original JSON:        %d bytes (100.0%%)\n", len(originalJSON))
	fmt.Printf("Pure Avro Binary:     %d bytes (%.1f%%)\n", len(binary),
		float64(len(binary))/float64(len(originalJSON))*100)
	fmt.Printf("Hex JSON:             %d bytes (%.1f%%)\n", len(hexJSON),
		float64(len(hexJSON))/float64(len(originalJSON))*100)
	fmt.Printf("Base64 JSON:          %d bytes (%.1f%%)\n", len(base64JSON),
		float64(len(base64JSON))/float64(len(originalJSON))*100)
	fmt.Printf("Array JSON:           %d bytes (%.1f%%)\n", len(arrayJSON),
		float64(len(arrayJSON))/float64(len(originalJSON))*100)
	fmt.Printf("Complete JSON:        %d bytes (%.1f%%)\n", len(completeJSON),
		float64(len(completeJSON))/float64(len(originalJSON))*100)

	// Encoding efficiency
	fmt.Printf("\n📈 Encoding Efficiency:\n")
	fmt.Printf("Binary size:          %d bytes\n", len(binary))
	fmt.Printf("Hex overhead:         %d bytes (%.1fx)\n", len(hexString)-len(binary),
		float64(len(hexString))/float64(len(binary)))
	fmt.Printf("Base64 overhead:      %d bytes (%.1fx)\n", len(base64String)-len(binary),
		float64(len(base64String))/float64(len(binary)))

	// Test reading back and decoding
	fmt.Printf("\n✅ Reading Back Test:\n")

	// Test Hex decoding
	var hexRead map[string]interface{}
	hexData, _ := os.ReadFile(hexFile)
	json.Unmarshal(hexData, &hexRead)

	decodedHex, err := hex.DecodeString(hexRead["binary_hex"].(string))
	if err != nil {
		t.Fatalf("Failed to decode hex: %v", err)
	}

	// Test Base64 decoding
	var base64Read map[string]interface{}
	base64Data, _ := os.ReadFile(base64File)
	json.Unmarshal(base64Data, &base64Read)

	decodedBase64, err := base64.StdEncoding.DecodeString(base64Read["binary_base64"].(string))
	if err != nil {
		t.Fatalf("Failed to decode base64: %v", err)
	}

	// Verify integrity
	fmt.Printf("Hex decode integrity:     %t\n", string(decodedHex) == string(binary))
	fmt.Printf("Base64 decode integrity:  %t\n", string(decodedBase64) == string(binary))

	// Test Avro decoding from restored binary
	restoredCodec, _ := goavro.NewCodec(hexRead["schema"].(string))
	restoredNative, _, err := restoredCodec.NativeFromBinary(decodedHex)
	if err != nil {
		t.Fatalf("Failed to decode Avro from restored binary: %v", err)
	}

	fmt.Printf("Avro decode success:      %t\n", restoredNative != nil)
	fmt.Printf("Restored data:            %+v\n", restoredNative)

	// Show JSON structure examples
	fmt.Printf("\n🔍 JSON Structure Examples:\n")

	fmt.Printf("\nHex format (first 200 chars):\n%s...\n",
		string(hexJSON[:min(200, len(hexJSON))]))

	fmt.Printf("\nBase64 format (first 200 chars):\n%s...\n",
		string(base64JSON[:min(200, len(base64JSON))]))

	fmt.Printf("\n💾 All files saved to: %s/\n", logsDir)
	fmt.Printf("🎯 === Avro Binary in JSON Test Complete ===\n")
}

// Test performance of different encoding methods
func TestEncodingPerformance(t *testing.T) {
	fmt.Println("\n⚡ === Encoding Performance Test ===")

	// Create larger test data
	largeData := make(map[string]interface{})
	largeData["id"] = int64(12345)
	largeData["name"] = "Performance Test User"

	// Create large array
	tags := make([]string, 100)
	for i := 0; i < 100; i++ {
		tags[i] = fmt.Sprintf("tag_%d", i)
	}
	largeData["tags"] = tags

	schema := `{
		"type": "record",
		"name": "PerfTest",
		"fields": [
			{"name": "id", "type": "long"},
			{"name": "name", "type": "string"},
			{"name": "tags", "type": {"type": "array", "items": "string"}}
		]
	}`

	codec, _ := goavro.NewCodec(schema)
	binary, _ := codec.BinaryFromNative(nil, largeData)

	fmt.Printf("📊 Test data binary size: %d bytes\n", len(binary))

	// Test encoding times (simplified)
	start := time.Now()
	hexEncoded := hex.EncodeToString(binary)
	hexTime := time.Since(start)

	start = time.Now()
	base64Encoded := base64.StdEncoding.EncodeToString(binary)
	base64Time := time.Since(start)

	fmt.Printf("⏱️  Hex encoding time:    %v\n", hexTime)
	fmt.Printf("⏱️  Base64 encoding time: %v\n", base64Time)
	fmt.Printf("📏 Hex result size:      %d bytes\n", len(hexEncoded))
	fmt.Printf("📏 Base64 result size:   %d bytes\n", len(base64Encoded))
}

// Run with: go test -run TestAvroBinaryInJSON -v
// Run with: go test -run TestEncodingPerformance -v
