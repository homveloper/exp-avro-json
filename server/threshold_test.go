package main

import (
	"encoding/json"
	"fmt"
	"testing"
)

// Simple test structure
type SimpleRecord struct {
	ID       int64             `json:"id"`
	Name     string            `json:"name"`
	Email    string            `json:"email"`
	Active   bool              `json:"active"`
	Score    float64           `json:"score"`
	Tags     []string          `json:"tags"`
	Settings map[string]string `json:"settings"`
}

func TestOptimizationThreshold(t *testing.T) {
	fmt.Println("\n📊 === Optimization Threshold Analysis ===")

	// Test different array sizes to find the break-even point
	testSizes := []int{1, 2, 3, 5, 10, 20, 50, 100, 200, 500, 1000}

	schema := `{
		"type": "record",
		"name": "SimpleRecord", 
		"fields": [
			{"name": "id", "type": "long"},
			{"name": "name", "type": "string"},
			{"name": "email", "type": "string"},
			{"name": "active", "type": "boolean"},
			{"name": "score", "type": "double"},
			{"name": "tags", "type": {"type": "array", "items": "string"}},
			{"name": "settings", "type": {"type": "map", "values": "string"}}
		]
	}`

	fmt.Printf("Schema size: %d bytes\n", len(schema))

	// Calculate schema overhead once
	schemaOverhead := len(schema) + 50 // schema + JSON structure overhead

	fmt.Printf("\n📈 Array Size vs Compression Efficiency:\n")
	fmt.Printf("%-8s %-12s %-12s %-12s %-12s %-12s\n",
		"Size", "Original", "Optimized", "Ratio", "Savings", "Efficient?")
	fmt.Printf("%-8s %-12s %-12s %-12s %-12s %-12s\n",
		"----", "--------", "---------", "-----", "-------", "----------")

	var breakEvenPoint int = -1

	for _, size := range testSizes {
		// Generate test data
		records := generateTestRecords(size)

		// Original JSON
		originalJSON, _ := json.Marshal(records)
		originalSize := len(originalJSON)

		// Optimized format (field-order arrays)
		optimizedData := make([]interface{}, len(records))
		for i, record := range records {
			optimizedData[i] = []interface{}{
				record.ID, record.Name, record.Email, record.Active,
				record.Score, record.Tags, record.Settings,
			}
		}

		optimizedContainer := map[string]interface{}{
			"schema":      schema,
			"field_order": []string{"id", "name", "email", "active", "score", "tags", "settings"},
			"data":        optimizedData,
		}

		optimizedJSON, _ := json.Marshal(optimizedContainer)
		optimizedSize := len(optimizedJSON)

		// Calculate metrics
		ratio := float64(optimizedSize) / float64(originalSize) * 100
		savings := originalSize - optimizedSize
		isEfficient := optimizedSize < originalSize

		// Mark break-even point
		if breakEvenPoint == -1 && isEfficient {
			breakEvenPoint = size
		}

		efficiencyMark := "❌"
		if isEfficient {
			efficiencyMark = "✅"
		}

		fmt.Printf("%-8d %-12d %-12d %-12.1f %-12d %-12s\n",
			size, originalSize, optimizedSize, ratio, savings, efficiencyMark)
	}

	if breakEvenPoint != -1 {
		fmt.Printf("\n🎯 Break-even point: %d records\n", breakEvenPoint)
	} else {
		fmt.Printf("\n⚠️  Break-even point not reached in tested range\n")
	}

	// Detailed analysis for break-even point
	if breakEvenPoint != -1 {
		fmt.Printf("\n🔍 Detailed Analysis at Break-even Point (%d records):\n", breakEvenPoint)

		records := generateTestRecords(breakEvenPoint)
		_, _ = json.Marshal(records)

		// Calculate field name repetition savings
		singleRecord, _ := json.Marshal(records[0])
		fieldNamesSize := len(singleRecord) - getDataOnlySize(records[0])
		totalFieldNameWaste := fieldNamesSize * breakEvenPoint

		fmt.Printf("Single record size:         %d bytes\n", len(singleRecord))
		fmt.Printf("Field names per record:     %d bytes\n", fieldNamesSize)
		fmt.Printf("Total field name waste:     %d bytes\n", totalFieldNameWaste)
		fmt.Printf("Schema overhead:            %d bytes\n", schemaOverhead)
		fmt.Printf("Net savings at break-even:  %d bytes\n", totalFieldNameWaste-schemaOverhead)
	}

	// Test with different record complexity
	fmt.Printf("\n🔄 Record Complexity Impact:\n")
	testRecordComplexity(10)  // Small array
	testRecordComplexity(50)  // Medium array
	testRecordComplexity(100) // Large array
}

func generateTestRecords(count int) []SimpleRecord {
	records := make([]SimpleRecord, count)
	for i := 0; i < count; i++ {
		records[i] = SimpleRecord{
			ID:     int64(i + 1000),
			Name:   fmt.Sprintf("User_%d", i),
			Email:  fmt.Sprintf("user%d@example.com", i),
			Active: i%2 == 0,
			Score:  float64(60 + (i % 40)),
			Tags:   []string{fmt.Sprintf("tag_%d", i%5), "common"},
			Settings: map[string]string{
				"theme": []string{"light", "dark"}[i%2],
				"lang":  []string{"ko", "en"}[i%2],
			},
		}
	}
	return records
}

func getDataOnlySize(record SimpleRecord) int {
	// Estimate data-only size (without field names)
	dataSize := 8 + // id (int64)
		len(record.Name) +
		len(record.Email) +
		1 + // active (bool)
		8 + // score (float64)
		estimateArraySize(record.Tags) +
		estimateMapSize(record.Settings)
	return dataSize
}

func estimateArraySize(arr []string) int {
	size := 0
	for _, s := range arr {
		size += len(s)
	}
	return size + len(arr)*2 // rough JSON array overhead
}

func estimateMapSize(m map[string]string) int {
	size := 0
	for k, v := range m {
		size += len(k) + len(v)
	}
	return size + len(m)*4 // rough JSON object overhead
}

func testRecordComplexity(arraySize int) {
	// Simple record
	simpleRecords := make([]map[string]interface{}, arraySize)
	for i := 0; i < arraySize; i++ {
		simpleRecords[i] = map[string]interface{}{
			"id":   i,
			"name": fmt.Sprintf("User_%d", i),
		}
	}

	// Complex record
	complexRecords := make([]map[string]interface{}, arraySize)
	for i := 0; i < arraySize; i++ {
		complexRecords[i] = map[string]interface{}{
			"id":     i,
			"name":   fmt.Sprintf("User_%d", i),
			"email":  fmt.Sprintf("user%d@example.com", i),
			"active": i%2 == 0,
			"score":  float64(60 + (i % 40)),
			"tags":   []string{fmt.Sprintf("tag_%d", i%5), "common", "test"},
			"settings": map[string]interface{}{
				"theme":         []string{"light", "dark"}[i%2],
				"lang":          []string{"ko", "en"}[i%2],
				"notifications": i%3 == 0,
				"privacy":       "public",
			},
			"metadata": map[string]interface{}{
				"created_at": "2024-01-01",
				"updated_at": "2024-01-02",
				"version":    "1.0",
			},
		}
	}

	// Calculate sizes
	simpleJSON, _ := json.Marshal(simpleRecords)
	complexJSON, _ := json.Marshal(complexRecords)

	// Create optimized versions
	simpleSchema := `{"type":"record","name":"Simple","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}`
	complexSchema := `{"type":"record","name":"Complex","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"},{"name":"email","type":"string"},{"name":"active","type":"boolean"},{"name":"score","type":"double"},{"name":"tags","type":{"type":"array","items":"string"}},{"name":"settings","type":{"type":"map","values":"string"}},{"name":"metadata","type":{"type":"map","values":"string"}}]}`

	simpleOptimized := map[string]interface{}{
		"schema": simpleSchema,
		"data":   extractSimpleData(simpleRecords),
	}

	complexOptimized := map[string]interface{}{
		"schema": complexSchema,
		"data":   extractComplexData(complexRecords),
	}

	simpleOptJSON, _ := json.Marshal(simpleOptimized)
	complexOptJSON, _ := json.Marshal(complexOptimized)

	fmt.Printf("Array size %d - Simple records:  %d → %d bytes (%.1f%%)\n",
		arraySize, len(simpleJSON), len(simpleOptJSON),
		float64(len(simpleOptJSON))/float64(len(simpleJSON))*100)

	fmt.Printf("Array size %d - Complex records: %d → %d bytes (%.1f%%)\n",
		arraySize, len(complexJSON), len(complexOptJSON),
		float64(len(complexOptJSON))/float64(len(complexJSON))*100)
}

func extractSimpleData(records []map[string]interface{}) [][]interface{} {
	data := make([][]interface{}, len(records))
	for i, record := range records {
		data[i] = []interface{}{record["id"], record["name"]}
	}
	return data
}

func extractComplexData(records []map[string]interface{}) [][]interface{} {
	data := make([][]interface{}, len(records))
	for i, record := range records {
		data[i] = []interface{}{
			record["id"], record["name"], record["email"], record["active"],
			record["score"], record["tags"], record["settings"], record["metadata"],
		}
	}
	return data
}

// Mathematical threshold calculation
func TestTheoreticalThreshold(t *testing.T) {
	fmt.Println("\n🧮 === Theoretical Threshold Calculation ===")

	// Assumptions for typical record
	avgFieldNames := 50 // bytes per record for field names
	schemaSize := 200   // bytes for schema definition
	jsonOverhead := 30  // bytes for JSON structure overhead

	fmt.Printf("Assumptions:\n")
	fmt.Printf("- Average field names per record: %d bytes\n", avgFieldNames)
	fmt.Printf("- Schema size: %d bytes\n", schemaSize)
	fmt.Printf("- JSON structure overhead: %d bytes\n", jsonOverhead)

	// Calculate theoretical break-even point
	// Field name waste * N > Schema overhead + JSON overhead
	// N > (Schema + JSON overhead) / Field name waste per record
	theoreticalThreshold := float64(schemaSize+jsonOverhead) / float64(avgFieldNames)

	fmt.Printf("\nTheoretical break-even point: %.1f records\n", theoreticalThreshold)
	fmt.Printf("Practical break-even point: %d records\n", int(theoreticalThreshold)+1)

	// Show efficiency at different sizes
	fmt.Printf("\nEfficiency at different sizes:\n")
	sizes := []int{1, 2, 5, 10, 20, 50, 100}
	for _, size := range sizes {
		savings := (avgFieldNames * size) - (schemaSize + jsonOverhead)
		efficiency := float64(savings) / float64(avgFieldNames*size) * 100
		status := "❌"
		if savings > 0 {
			status = "✅"
		}
		fmt.Printf("Size %3d: %+4d bytes saved (%.1f%% efficiency) %s\n",
			size, savings, efficiency, status)
	}
}

// Run with: go test -run TestOptimizationThreshold -v
// Run with: go test -run TestTheoreticalThreshold -v
