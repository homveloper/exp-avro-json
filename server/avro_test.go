package main

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/linkedin/goavro/v2"
)

// Test structures for demonstration.gitignore
type UserProfile struct {
	UserID      int64             `json:"user_id"`
	Username    string            `json:"username"`
	Email       string            `json:"email"`
	Age         int               `json:"age"`
	IsActive    bool              `json:"is_active"`
	Balance     float64           `json:"balance"`
	Tags        []string          `json:"tags"`
	Preferences map[string]string `json:"preferences"`
	Address     Address           `json:"address"`
}

type Address struct {
	Street   string      `json:"street"`
	City     string      `json:"city"`
	Country  string      `json:"country"`
	PostCode string      `json:"post_code"`
	Coords   Coordinates `json:"coordinates"`
}

type Coordinates struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type SystemMetrics struct {
	CPUUsage    float64           `json:"cpu_usage"`
	MemoryUsage float64           `json:"memory_usage"`
	DiskIO      int64             `json:"disk_io"`
	NetworkIO   int64             `json:"network_io"`
	ProcessList []string          `json:"process_list"`
	Environment map[string]string `json:"environment"`
}

func TestAvroSerialization(t *testing.T) {
	fmt.Println("=== Avro Serialization Test ===")

	// Create test data structures
	userProfile := UserProfile{
		UserID:   12345,
		Username: "johndoe",
		Email:    "john.doe@example.com",
		Age:      28,
		IsActive: true,
		Balance:  1543.67,
		Tags:     []string{"premium", "verified", "beta_tester"},
		Preferences: map[string]string{
			"theme":      "dark",
			"language":   "en",
			"timezone":   "UTC",
			"newsletter": "true",
		},
		Address: Address{
			Street:   "123 Main Street",
			City:     "Seoul",
			Country:  "South Korea",
			PostCode: "12345",
			Coords: Coordinates{
				Latitude:  37.5665,
				Longitude: 126.9780,
			},
		},
	}

	systemMetrics := SystemMetrics{
		CPUUsage:    87.5,
		MemoryUsage: 4096.0,
		DiskIO:      125467,
		NetworkIO:   789123,
		ProcessList: []string{"nginx", "postgres", "redis", "app_server"},
		Environment: map[string]string{
			"NODE_ENV": "production",
			"PORT":     "8080",
			"DEBUG":    "false",
		},
	}

	// Create test log request
	testLogRequest := LogRequest{
		ProjectName:    "avro_test_project",
		ProjectVersion: "2.0.0",
		LogLevel:       "INFO",
		LogType:        "USER_ACTION",
		LogSource:      "test_suite",
		LogBody: LogData{
			Timestamp:  time.Now().UnixMilli(),
			Logtype:    "user_profile_update",
			Version:    "1.0",
			Issuer:     "test_system",
			Metadata:   systemMetrics,
			DomainData: userProfile,
		},
	}

	fmt.Printf("📊 Original Test Data:\n")
	fmt.Printf("UserProfile: %+v\n", userProfile)
	fmt.Printf("SystemMetrics: %+v\n", systemMetrics)

	// 1. Serialize to JSON for comparison
	originalJSON, err := json.Marshal(testLogRequest)
	if err != nil {
		t.Fatalf("Failed to marshal to JSON: %v", err)
	}
	fmt.Printf("\n📄 Original JSON size: %d bytes\n", len(originalJSON))

	// 2. Convert metadata and domainData to Avro format
	metadataForAvro := convertToAvroMap(testLogRequest.LogBody.Metadata)
	domainDataForAvro := convertToAvroMap(testLogRequest.LogBody.DomainData)

	fmt.Printf("\n🔄 Converted to Avro map format:\n")
	fmt.Printf("Metadata keys: %v\n", getMapKeys(metadataForAvro))
	fmt.Printf("DomainData keys: %v\n", getMapKeys(domainDataForAvro))

	// 3. Create Avro LogData
	avroLogData := AvroLogData{
		Timestamp:  testLogRequest.LogBody.Timestamp,
		Logtype:    testLogRequest.LogBody.Logtype,
		Version:    testLogRequest.LogBody.Version,
		Issuer:     testLogRequest.LogBody.Issuer,
		Metadata:   metadataForAvro,
		DomainData: domainDataForAvro,
	}

	// 4. Serialize LogData to Avro binary
	logDataCodec, err := goavro.NewCodec(logDataSchema)
	if err != nil {
		t.Fatalf("Failed to create LogData codec: %v", err)
	}

	logDataRecord := structToMap(avroLogData)
	logDataBinary, err := logDataCodec.BinaryFromNative(nil, logDataRecord)
	if err != nil {
		t.Fatalf("Failed to encode LogData to Avro: %v", err)
	}

	// 5. Convert back to JSON to see Avro JSON format
	logDataNative, _, err := logDataCodec.NativeFromBinary(logDataBinary)
	if err != nil {
		t.Fatalf("Failed to decode LogData from Avro: %v", err)
	}

	logDataJSON, err := logDataCodec.TextualFromNative(nil, logDataNative)
	if err != nil {
		t.Fatalf("Failed to convert LogData to JSON: %v", err)
	}

	// 6. Create wrapper with LogData JSON as body
	avroWrapper := AvroLogWrapper{
		ProjectName:    testLogRequest.ProjectName,
		ProjectVersion: testLogRequest.ProjectVersion,
		Body:           string(logDataJSON),
		LogLevel:       testLogRequest.LogLevel,
		LogType:        testLogRequest.LogType,
		LogSource:      testLogRequest.LogSource,
	}

	// 7. Serialize wrapper to Avro binary
	wrapperCodec, err := goavro.NewCodec(wrapperSchema)
	if err != nil {
		t.Fatalf("Failed to create wrapper codec: %v", err)
	}

	wrapperRecord := structToMap(avroWrapper)
	wrapperBinary, err := wrapperCodec.BinaryFromNative(nil, wrapperRecord)
	if err != nil {
		t.Fatalf("Failed to encode wrapper to Avro: %v", err)
	}

	// 8. Display results
	fmt.Printf("\n📊 Compression Results:\n")
	fmt.Printf("Original JSON:     %d bytes\n", len(originalJSON))
	fmt.Printf("LogData Avro:      %d bytes (%.2f%% of original)\n",
		len(logDataBinary), float64(len(logDataBinary))/float64(len(originalJSON))*100)
	fmt.Printf("Wrapper Avro:      %d bytes (%.2f%% of original)\n",
		len(wrapperBinary), float64(len(wrapperBinary))/float64(len(originalJSON))*100)

	if len(wrapperBinary) < len(originalJSON) {
		savings := len(originalJSON) - len(wrapperBinary)
		compressionPercent := (1.0 - float64(len(wrapperBinary))/float64(len(originalJSON))) * 100
		fmt.Printf("✅ Compression achieved: %d bytes saved (%.2f%% reduction)\n", savings, compressionPercent)
	} else {
		overhead := len(wrapperBinary) - len(originalJSON)
		fmt.Printf("❌ No compression: %d bytes overhead\n", overhead)
	}

	// 9. Show Avro JSON structure
	fmt.Printf("\n🔍 Avro JSON Structure (first 300 chars):\n")
	if len(logDataJSON) > 300 {
		fmt.Printf("%s...\n", string(logDataJSON[:300]))
	} else {
		fmt.Printf("%s\n", string(logDataJSON))
	}

	// 10. Verify data integrity by decoding
	fmt.Printf("\n✅ Data Integrity Check:\n")

	// Decode wrapper
	wrapperNative, _, err := wrapperCodec.NativeFromBinary(wrapperBinary)
	if err != nil {
		t.Fatalf("Failed to decode wrapper: %v", err)
	}

	decodedWrapper := make(map[string]interface{})
	wrapperJSON, _ := wrapperCodec.TextualFromNative(nil, wrapperNative)
	json.Unmarshal(wrapperJSON, &decodedWrapper)

	fmt.Printf("Project Name: %v\n", decodedWrapper["projectName"])
	fmt.Printf("Log Type: %v\n", decodedWrapper["logType"])
	fmt.Printf("Body contains LogData: %t\n", len(decodedWrapper["body"].(string)) > 0)

	fmt.Printf("\n🎉 Avro serialization test completed successfully!\n")
}

// Helper function to get map keys
func getMapKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// Run this test with: go test -run TestAvroSerialization -v
