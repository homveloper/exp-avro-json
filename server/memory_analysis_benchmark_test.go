package main

import (
	"encoding/json"
	"runtime"
	"testing"

	"github.com/linkedin/goavro/v2"
)

func BenchmarkMemoryStandardJSON(b *testing.B) {
	data := generateDummyCharacters(20)

	b.ResetTimer()

	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	for i := 0; i < b.N; i++ {
		jsonData, _ := json.Marshal(data)
		_ = jsonData
	}

	runtime.GC()
	runtime.ReadMemStats(&m2)

	b.Logf("Memory: Alloc=%d KB, TotalAlloc=%d KB, Sys=%d KB, NumGC=%d",
		(m2.Alloc-m1.Alloc)/1024,
		(m2.TotalAlloc-m1.TotalAlloc)/1024,
		(m2.Sys-m1.Sys)/1024,
		m2.NumGC-m1.NumGC)
}

func BenchmarkMemoryAvroBinary(b *testing.B) {
	data := generateDummyCharacters(20)
	codec, _ := goavro.NewCodec(userCharacterSchema)

	// Convert to map for Avro
	dataMap := convertToAvroMap(data)

	b.ResetTimer()

	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	for i := 0; i < b.N; i++ {
		binaryData, _ := codec.BinaryFromNative(nil, dataMap)
		_ = binaryData
	}

	runtime.GC()
	runtime.ReadMemStats(&m2)

	b.Logf("Memory: Alloc=%d KB, TotalAlloc=%d KB, Sys=%d KB, NumGC=%d",
		(m2.Alloc-m1.Alloc)/1024,
		(m2.TotalAlloc-m1.TotalAlloc)/1024,
		(m2.Sys-m1.Sys)/1024,
		m2.NumGC-m1.NumGC)
}

func BenchmarkMemoryAvroJSON(b *testing.B) {
	data := generateDummyCharacters(20)
	codec, _ := goavro.NewCodec(userCharacterSchema)

	// Convert to map for Avro
	dataMap := convertToAvroMap(data)

	b.ResetTimer()

	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	for i := 0; i < b.N; i++ {
		jsonData, _ := codec.TextualFromNative(nil, dataMap)
		_ = jsonData
	}

	runtime.GC()
	runtime.ReadMemStats(&m2)

	b.Logf("Memory: Alloc=%d KB, TotalAlloc=%d KB, Sys=%d KB, NumGC=%d",
		(m2.Alloc-m1.Alloc)/1024,
		(m2.TotalAlloc-m1.TotalAlloc)/1024,
		(m2.Sys-m1.Sys)/1024,
		m2.NumGC-m1.NumGC)
}

func TestMemoryComparison(t *testing.T) {
	data := generateDummyCharacters(20)
	codec, _ := goavro.NewCodec(userCharacterSchema)
	dataMap := convertToAvroMap(data)

	t.Log("=== Memory Usage Comparison ===")

	// Standard JSON
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	jsonData, _ := json.Marshal(data)

	runtime.ReadMemStats(&m2)
	t.Logf("Standard JSON: %d bytes, Memory: %d KB allocated",
		len(jsonData), (m2.TotalAlloc-m1.TotalAlloc)/1024)

	// Avro Binary
	runtime.GC()
	runtime.ReadMemStats(&m1)

	binaryData, _ := codec.BinaryFromNative(nil, dataMap)

	runtime.ReadMemStats(&m2)
	t.Logf("Avro Binary: %d bytes, Memory: %d KB allocated",
		len(binaryData), (m2.TotalAlloc-m1.TotalAlloc)/1024)

	// Avro JSON
	runtime.GC()
	runtime.ReadMemStats(&m1)

	avroJsonData, _ := codec.TextualFromNative(nil, dataMap)

	runtime.ReadMemStats(&m2)
	t.Logf("Avro JSON: %d bytes, Memory: %d KB allocated",
		len(avroJsonData), (m2.TotalAlloc-m1.TotalAlloc)/1024)

	t.Logf("Size ratio - Binary/JSON: %.2f", float64(len(binaryData))/float64(len(jsonData)))
}

func TestDetailedMemoryAnalysis(t *testing.T) {
	sizes := []int{5, 10, 20, 50}

	for _, size := range sizes {
		t.Logf("\n=== Memory Analysis for %d Characters ===", size)

		data := generateDummyCharacters(size)
		codec, _ := goavro.NewCodec(userCharacterSchema)
		dataMap := convertToAvroMap(data)

		// Measure each method
		methods := map[string]func() ([]byte, error){
			"Standard JSON": func() ([]byte, error) {
				return json.Marshal(data)
			},
			"Avro Binary": func() ([]byte, error) {
				return codec.BinaryFromNative(nil, dataMap)
			},
			"Avro JSON": func() ([]byte, error) {
				return codec.TextualFromNative(nil, dataMap)
			},
		}

		for name, method := range methods {
			runtime.GC()
			var m1, m2 runtime.MemStats
			runtime.ReadMemStats(&m1)

			result, _ := method()

			runtime.ReadMemStats(&m2)

			t.Logf("%s: Size=%d bytes, Memory=%d KB, Allocs=%d",
				name, len(result),
				(m2.TotalAlloc-m1.TotalAlloc)/1024,
				m2.Mallocs-m1.Mallocs)
		}
	}
}
