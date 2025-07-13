package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"time"
)

type PingRequest struct {
	Data interface{} `json:"data"`
}

type PingResponse struct {
	Status    string      `json:"status"`
	Timestamp int64       `json:"timestamp"`
	Message   string      `json:"message"`
	Echo      interface{} `json:"echo"`
}

type LogRequest struct {
	ProjectName    string  `json:"projectName"`
	ProjectVersion string  `json:"projectVersion"`
	LogLevel       string  `json:"logLevel"`
	LogType        string  `json:"logType"`
	LogSource      string  `json:"logSource"`
	Body           LogData `json:"body"`
}

type LogData struct {
	Timestamp  int64       `json:"timestamp"`
	Logtype    string      `json:"logtype"`
	Version    string      `json:"version"`
	Issuer     string      `json:"issuer"`
	Metadata   interface{} `json:"metadata,omitempty"`
	DomainData interface{} `json:"domainData,omitempty"`
}

type LogResponse struct {
	Status           string                 `json:"status"`
	CompressionStats map[string]interface{} `json:"compression_stats"`
	WrapperAvroJSON  string                 `json:"wrapper_avro_json"`
	LogdataAvroJSON  string                 `json:"logdata_avro_json"`
}

const serverURL = "http://localhost:8080"

func main() {
	if len(os.Args) < 2 {
		printUsage()
		return
	}

	command := os.Args[1]
	switch command {
	case "ping":
		testPing()
	case "log":
		if len(os.Args) < 3 {
			fmt.Println("Please specify log size: small, medium, large, or random")
			return
		}
		size := os.Args[2]
		testLog(size)
	default:
		fmt.Printf("Unknown command: %s\n", command)
		printUsage()
	}
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  go run . ping                  - Send ping request")
	fmt.Println("  go run . log small             - Send small log data")
	fmt.Println("  go run . log medium            - Send medium log data")
	fmt.Println("  go run . log large             - Send large log data")
	fmt.Println("  go run . log random            - Send random size log data")
}

func testPing() {
	fmt.Println("🏓 Testing /ping endpoint with Avro JSON data...")

	// Create test data that simulates Avro JSON format
	avroJSONData := map[string]interface{}{
		"projectName":    "72356c50401b8e20_testproject",
		"projectVersion": "1.0.0",
		"body":           `{"timestamp":1673456789000,"logtype":"리스트 조회","version":"1.0","issuer":"user123","metadata":{"string":"{\"key\":\"value\"}"}}`,
		"logLevel":       "DEBUG",
		"logType":        "WEB",
		"logSource":      "avro",
	}

	pingReq := PingRequest{
		Data: avroJSONData,
	}

	reqBody, err := json.Marshal(pingReq)
	if err != nil {
		fmt.Printf("❌ Failed to marshal request: %v\n", err)
		return
	}

	fmt.Printf("📤 Sending request (%d bytes)...\n", len(reqBody))

	resp, err := http.Post(serverURL+"/ping", "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		fmt.Printf("❌ Failed to send request: %v\n", err)
		return
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("❌ Failed to read response: %v\n", err)
		return
	}

	fmt.Printf("📥 Response status: %s\n", resp.Status)

	var pingResp PingResponse
	if err := json.Unmarshal(respBody, &pingResp); err != nil {
		fmt.Printf("❌ Failed to parse response: %v\n", err)
		return
	}

	fmt.Printf("\n=== 🏓 Ping Test Results ===\n")
	fmt.Printf("Server Status: %s\n", pingResp.Status)
	fmt.Printf("Server Message: %s\n", pingResp.Message)
	
	// Verify echo
	sentDataJSON, _ := json.Marshal(avroJSONData)
	echoDataJSON, _ := json.Marshal(pingResp.Echo)
	
	if string(sentDataJSON) == string(echoDataJSON) {
		fmt.Printf("Echo Test: ✅ PASSED - Data echoed correctly\n")
	} else {
		fmt.Printf("Echo Test: ❌ FAILED - Echo data doesn't match\n")
	}

	fmt.Printf("Timestamp: %s\n", time.Unix(pingResp.Timestamp, 0).Format("2006-01-02 15:04:05"))
}

func testLog(size string) {
	var logReq LogRequest
	
	switch size {
	case "small":
		logReq = createSmallLogData()
	case "medium":
		logReq = createMediumLogData()
	case "large":
		logReq = createLargeLogData()
	case "random":
		sizes := []string{"small", "medium", "large"}
		rand.Seed(time.Now().UnixNano())
		randomSize := sizes[rand.Intn(len(sizes))]
		fmt.Printf("🎲 Randomly selected size: %s\n", randomSize)
		testLog(randomSize)
		return
	default:
		fmt.Printf("❌ Unknown size: %s\n", size)
		return
	}

	fmt.Printf("📝 Testing /log endpoint with %s log data...\n", size)

	reqBody, err := json.Marshal(logReq)
	if err != nil {
		fmt.Printf("❌ Failed to marshal request: %v\n", err)
		return
	}

	fmt.Printf("📤 Request size: %d bytes\n", len(reqBody))
	fmt.Printf("📤 Sending log request...\n")

	resp, err := http.Post(serverURL+"/log", "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		fmt.Printf("❌ Failed to send request: %v\n", err)
		return
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("❌ Failed to read response: %v\n", err)
		return
	}

	fmt.Printf("📥 Response status: %s\n", resp.Status)

	var logResp LogResponse
	if err := json.Unmarshal(respBody, &logResp); err != nil {
		fmt.Printf("❌ Failed to parse response: %v\n", err)
		fmt.Printf("Raw response: %s\n", string(respBody))
		return
	}

	fmt.Printf("\n=== 📊 Compression Results ===\n")
	fmt.Printf("Status: %s\n", logResp.Status)
	fmt.Printf("Compression Stats:\n")
	
	originalSize := getIntValue(logResp.CompressionStats, "original_json_size")
	wrapperSize := getIntValue(logResp.CompressionStats, "wrapper_avro_size")
	logdataSize := getIntValue(logResp.CompressionStats, "logdata_avro_size")
	
	fmt.Printf("  📄 Original JSON size: %d bytes\n", originalSize)
	fmt.Printf("  🗜️  Wrapper Avro size: %d bytes\n", wrapperSize)
	fmt.Printf("  🗜️  LogData Avro size: %d bytes\n", logdataSize)
	
	if originalSize > 0 {
		wrapperRatio := float64(wrapperSize) / float64(originalSize) * 100
		logdataRatio := float64(logdataSize) / float64(originalSize) * 100
		fmt.Printf("  📈 Wrapper compression: %.2f%% of original\n", wrapperRatio)
		fmt.Printf("  📈 LogData compression: %.2f%% of original\n", logdataRatio)
		
		if wrapperSize < originalSize {
			savings := originalSize - wrapperSize
			fmt.Printf("  💾 Space saved: %d bytes (%.2f%%)\n", savings, (1.0-wrapperRatio/100)*100)
		}
	}

	fmt.Printf("\n=== 🔍 Sample Avro JSON Output ===\n")
	fmt.Printf("Wrapper Avro JSON (first 200 chars):\n%s...\n", truncateString(logResp.WrapperAvroJSON, 200))
	fmt.Printf("LogData Avro JSON (first 200 chars):\n%s...\n", truncateString(logResp.LogdataAvroJSON, 200))
}

func createSmallLogData() LogRequest {
	return LogRequest{
		ProjectName:    "72356c50401b8e20_testproject",
		ProjectVersion: "1.0.0",
		LogLevel:       "INFO",
		LogType:        "USER_ACTION",
		LogSource:      "web_client",
		Body: LogData{
			Timestamp: time.Now().UnixMilli(),
			Logtype:   "user_login",
			Version:   "1.0",
			Issuer:    "user123",
			Metadata: map[string]interface{}{
				"ip":         "192.168.1.100",
				"user_agent": "Mozilla/5.0 Chrome/91.0",
				"session_id": "sess_abc123",
			},
			DomainData: map[string]interface{}{
				"login_method": "password",
				"success":      true,
				"duration_ms":  150,
				"redirect_url": "/dashboard",
			},
		},
	}
}

func createMediumLogData() LogRequest {
	return LogRequest{
		ProjectName:    "72356c50401b8e20_testproject",
		ProjectVersion: "1.0.0",
		LogLevel:       "DEBUG",
		LogType:        "API_CALL",
		LogSource:      "backend_service",
		Body: LogData{
			Timestamp: time.Now().UnixMilli(),
			Logtype:   "database_query",
			Version:   "2.1",
			Issuer:    "service_worker_456",
			Metadata: map[string]interface{}{
				"request_id":        "req_abc123def456",
				"correlation_id":    "corr_xyz789",
				"trace_id":          "trace_qwe456rty789",
				"span_id":           "span_123abc456def",
				"environment":       "production",
				"region":            "us-west-2",
				"availability_zone": "us-west-2a",
				"service_version":   "v2.1.3",
			},
			DomainData: map[string]interface{}{
				"query_type":     "SELECT",
				"table_name":     "user_profiles",
				"execution_time": 245.67,
				"rows_affected":  1250,
				"query_plan": map[string]interface{}{
					"node_type":    "Hash Join",
					"startup_cost": 123.45,
					"total_cost":   567.89,
					"plan_rows":    1000,
					"plan_width":   48,
					"joins": []map[string]interface{}{
						{
							"join_type":      "INNER",
							"table":          "users",
							"condition":      "users.id = user_profiles.user_id",
							"estimated_rows": 800,
						},
						{
							"join_type":      "LEFT",
							"table":          "user_settings",
							"condition":      "users.id = user_settings.user_id",
							"estimated_rows": 600,
						},
					},
				},
				"cache_hit":        false,
				"connection_pool":  "pool_1",
				"query_hash":       "hash_abc123def456",
				"slow_query_threshold": 200.0,
				"parameters": map[string]interface{}{
					"user_id":    12345,
					"start_date": "2024-01-01",
					"end_date":   "2024-12-31",
					"limit":      100,
					"offset":     0,
					"filters": []string{
						"status = 'active'",
						"created_at > '2024-01-01'",
						"region IN ('us-west-2', 'us-east-1')",
					},
				},
			},
		},
	}
}

func createLargeLogData() LogRequest {
	// Create large arrays and complex nested structures
	largeUserArray := make([]map[string]interface{}, 150)
	for i := 0; i < 150; i++ {
		largeUserArray[i] = map[string]interface{}{
			"user_id":    i + 1000,
			"username":   fmt.Sprintf("user_%d", i),
			"email":      fmt.Sprintf("user%d@example.com", i),
			"full_name":  fmt.Sprintf("User Number %d", i),
			"bio":        fmt.Sprintf("This is a very detailed biography for user number %d. Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.", i),
			"age":        20 + (i % 50),
			"location": map[string]interface{}{
				"country":    []string{"USA", "Canada", "UK", "Germany", "France"}[i%5],
				"city":       fmt.Sprintf("City_%d", i%20),
				"latitude":   40.7128 + float64(i%100)/100,
				"longitude":  -74.0060 + float64(i%100)/100,
				"timezone":   "UTC-5",
				"postal_code": fmt.Sprintf("%05d", 10000+i),
			},
			"preferences": map[string]interface{}{
				"theme":           []string{"light", "dark", "auto"}[i%3],
				"language":        []string{"en", "es", "fr", "de", "ja"}[i%5],
				"notifications":   i%2 == 0,
				"privacy_level":   []string{"public", "friends", "private"}[i%3],
				"newsletter":      i%3 == 0,
				"two_factor_auth": i%4 == 0,
			},
			"activity": map[string]interface{}{
				"last_login":     time.Now().Add(-time.Duration(i) * time.Hour).Unix(),
				"login_count":    i * 10,
				"posts_count":    i * 5,
				"followers_count": i * 15,
				"following_count": i * 12,
				"likes_given":    i * 100,
				"comments_made":  i * 25,
			},
			"tags": []string{
				fmt.Sprintf("tag_%d", i),
				fmt.Sprintf("category_%d", i%10),
				"premium",
				"verified",
				"active_user",
			},
		}
	}

	performanceMetrics := make([]map[string]interface{}, 50)
	for i := 0; i < 50; i++ {
		performanceMetrics[i] = map[string]interface{}{
			"timestamp":     time.Now().Add(-time.Duration(i) * time.Minute).Unix(),
			"cpu_usage":     50.0 + float64(i%50),
			"memory_usage":  30.0 + float64(i%70),
			"disk_io":       float64(i * 10),
			"network_io":    float64(i * 15),
			"response_time": 100.0 + float64(i%200),
			"error_rate":    float64(i%10) / 100.0,
			"throughput":    1000.0 + float64(i*50),
		}
	}

	return LogRequest{
		ProjectName:    "72356c50401b8e20_testproject",
		ProjectVersion: "1.0.0",
		LogLevel:       "ERROR",
		LogType:        "SYSTEM_EVENT",
		LogSource:      "analytics_engine",
		Body: LogData{
			Timestamp: time.Now().UnixMilli(),
			Logtype:   "batch_processing_complete",
			Version:   "3.2.1",
			Issuer:    "batch_processor_789",
			Metadata: map[string]interface{}{
				"job_id":                "job_12345_batch_analytics_large_dataset",
				"batch_id":              "batch_67890_data_processing_full_pipeline",
				"pipeline_id":           "pipeline_analytics_main_production",
				"cluster_id":            "cluster_prod_west_01_high_performance",
				"node_id":               "node_worker_256_specialized",
				"container_id":          "container_abc123def456ghi789jkl012mno345",
				"kubernetes_pod":        "analytics-worker-7d4b8c9f2-xm4n8-large",
				"namespace":             "production-analytics-high-throughput",
				"hostname":              "analytics-node-256.cluster.internal.local",
				"ip_address":            "10.244.1.156",
				"load_balancer":         "lb-prod-analytics-01.amazonaws.com",
				"region":                "us-west-2",
				"availability_zone":     "us-west-2c",
				"instance_type":         "c5.24xlarge",
				"memory_usage_mb":       122880,
				"cpu_usage_percent":     89.5,
				"disk_usage_gb":         2047.8,
				"network_io_mb":         1245.6,
				"gpu_count":             4,
				"gpu_memory_gb":         64,
				"storage_type":          "nvme_ssd",
				"network_bandwidth_gbps": 100,
			},
			DomainData: map[string]interface{}{
				"processing_result":   "SUCCESS_WITH_WARNINGS",
				"total_records":       12500000,
				"processed_records":   12450000,
				"failed_records":      50000,
				"skipped_records":     0,
				"processing_time_ms":  2456789,
				"retry_count":         2,
				"max_retries":         5,
				"batch_size":          100000,
				"total_batches":       125,
				"completed_batches":   123,
				"failed_batches":      2,
				"warning_batches":     15,
				"data_sources": []string{
					"s3://analytics-data/raw/user_events/2024/01/01/",
					"s3://analytics-data/raw/user_events/2024/01/02/",
					"s3://analytics-data/raw/transaction_logs/2024/01/",
					"s3://analytics-data/raw/system_metrics/2024/01/",
					"s3://analytics-data/raw/application_logs/2024/01/",
					"s3://analytics-data/raw/security_logs/2024/01/",
				},
				"output_destinations": []string{
					"s3://analytics-results/processed/daily_reports/2024/01/",
					"redshift://prod-cluster/analytics_db.daily_metrics",
					"elasticsearch://prod-cluster:9200/analytics-index-2024-01/",
					"snowflake://prod-account/analytics_warehouse/daily_summaries",
				},
				"performance_metrics": performanceMetrics,
				"processed_users":     largeUserArray,
				"data_quality_checks": map[string]interface{}{
					"duplicate_check":     map[string]interface{}{"passed": true, "duplicates_found": 0},
					"null_check":          map[string]interface{}{"passed": false, "null_count": 156},
					"format_check":        map[string]interface{}{"passed": true, "format_errors": 0},
					"range_check":         map[string]interface{}{"passed": true, "out_of_range": 0},
					"consistency_check":   map[string]interface{}{"passed": false, "inconsistencies": 23},
					"completeness_check":  map[string]interface{}{"passed": true, "completeness_score": 0.998},
				},
				"transformations_applied": []string{
					"data_cleansing",
					"format_standardization",
					"duplicate_removal",
					"data_enrichment",
					"privacy_masking",
					"data_validation",
					"schema_evolution",
				},
				"resource_utilization": map[string]interface{}{
					"peak_memory_gb":       89.2,
					"peak_cpu_percent":     94.8,
					"peak_disk_io_mbps":    1567.3,
					"peak_network_mbps":    2345.7,
					"avg_memory_gb":        67.4,
					"avg_cpu_percent":      78.9,
					"total_io_operations":  9876543,
					"cache_hit_ratio":      0.891,
					"compression_ratio":    0.234,
				},
				"configuration": map[string]interface{}{
					"jvm_heap_size":         "64g",
					"gc_algorithm":          "G1GC",
					"parallel_threads":      32,
					"connection_pool_size":  100,
					"timeout_seconds":       1800,
					"buffer_size_mb":        1024,
					"compression_enabled":   true,
					"encryption_enabled":    true,
					"debug_mode":            false,
					"log_level":             "INFO",
					"monitoring_enabled":    true,
					"metrics_collection":    true,
					"distributed_computing": true,
					"auto_scaling":          true,
				},
				"error_details": []map[string]interface{}{
					{
						"error_code":    "DATA_001",
						"error_message": "Invalid timestamp format in batch 45",
						"affected_records": 1234,
						"resolution":    "Applied default timestamp",
					},
					{
						"error_code":    "NET_002",
						"error_message": "Temporary network timeout to data source",
						"affected_records": 567,
						"resolution":    "Retry successful after 30 seconds",
					},
				},
				"warnings": []string{
					"High memory usage detected during processing",
					"Some data sources had minor schema changes",
					"Network latency higher than expected",
					"Disk I/O approaching capacity limits",
				},
			},
		},
	}
}

func getIntValue(m map[string]interface{}, key string) int {
	if val, exists := m[key]; exists {
		if intVal, ok := val.(float64); ok {
			return int(intVal)
		}
	}
	return 0
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen]
}