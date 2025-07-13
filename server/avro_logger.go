package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
)

func logAvroData(wrapperBinary []byte, logDataBinary []byte, originalSize int, req LogRequest) {
	// Create avro-logs directory if it doesn't exist
	avroLogsDir := "avro-logs"
	if err := os.MkdirAll(avroLogsDir, 0755); err != nil {
		logger.Error("Failed to create avro-logs directory", zap.Error(err))
		return
	}

	// Generate timestamp-based filename
	timestamp := time.Now().Format("20060102_150405")

	// Save wrapper Avro binary
	wrapperFile := filepath.Join(avroLogsDir, fmt.Sprintf("wrapper_%s.avro", timestamp))
	if err := os.WriteFile(wrapperFile, wrapperBinary, 0644); err != nil {
		logger.Error("Failed to write wrapper Avro file", zap.Error(err))
	} else {
		logger.Info("Wrapper Avro binary saved",
			zap.String("file", wrapperFile),
			zap.Int("size_bytes", len(wrapperBinary)))
	}

	// Save logdata Avro binary
	logDataFile := filepath.Join(avroLogsDir, fmt.Sprintf("logdata_%s.avro", timestamp))
	if err := os.WriteFile(logDataFile, logDataBinary, 0644); err != nil {
		logger.Error("Failed to write logdata Avro file", zap.Error(err))
	} else {
		logger.Info("LogData Avro binary saved",
			zap.String("file", logDataFile),
			zap.Int("size_bytes", len(logDataBinary)))
	}

	// For comparison, save original JSON as well
	jsonFile := filepath.Join(avroLogsDir, fmt.Sprintf("original_%s.json", timestamp))
	originalJSON, err := json.Marshal(req)
	if err != nil {
		logger.Error("Failed to marshal original JSON", zap.Error(err))
	} else {
		if err := os.WriteFile(jsonFile, originalJSON, 0644); err != nil {
			logger.Error("Failed to write original JSON file", zap.Error(err))
		} else {
			logger.Info("Original JSON saved",
				zap.String("file", jsonFile),
				zap.Int("size_bytes", len(originalJSON)))
		}
	}

	// Calculate and log compression statistics
	wrapperCompressionRatio := float64(len(wrapperBinary)) / float64(originalSize) * 100
	logDataCompressionRatio := float64(len(logDataBinary)) / float64(originalSize) * 100

	wrapperSavings := originalSize - len(wrapperBinary)
	logDataSavings := originalSize - len(logDataBinary)

	logger.Info("Avro compression analysis",
		zap.String("timestamp", timestamp),
		zap.Int("original_json_bytes", originalSize),
		zap.Int("wrapper_avro_bytes", len(wrapperBinary)),
		zap.Int("logdata_avro_bytes", len(logDataBinary)),
		zap.Float64("wrapper_compression_ratio", wrapperCompressionRatio),
		zap.Float64("logdata_compression_ratio", logDataCompressionRatio),
		zap.Int("wrapper_space_saved", wrapperSavings),
		zap.Int("logdata_space_saved", logDataSavings),
		zap.String("log_type", req.LogType),
		zap.String("log_level", req.LogLevel))

	// Log summary with better compression
	if len(wrapperBinary) < originalSize {
		logger.Info("Compression achieved!",
			zap.String("best_compression", "wrapper"),
			zap.Float64("compression_percent", 100.0-wrapperCompressionRatio),
			zap.Int("bytes_saved", wrapperSavings))
	} else {
		logger.Warn("No compression achieved - Avro overhead exceeded savings",
			zap.Int("overhead_bytes", len(wrapperBinary)-originalSize))
	}
}
