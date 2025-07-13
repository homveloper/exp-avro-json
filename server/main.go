package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/linkedin/goavro/v2"
	"go.uber.org/zap"
)

// Avro schema structures
type AvroLogWrapper struct {
	ProjectName    string `avro:"projectName"`
	ProjectVersion string `avro:"projectVersion"`
	Body           string `avro:"body"`
	LogLevel       string `avro:"logLevel"`
	LogType        string `avro:"logType"`
	LogSource      string `avro:"logSource"`
}

type AvroLogData struct {
	Timestamp  int64       `avro:"timestamp"`
	Logtype    string      `avro:"logtype"`
	Version    string      `avro:"version"`
	Issuer     string      `avro:"issuer"`
	Metadata   interface{} `avro:"metadata"`
	DomainData interface{} `avro:"domainData"`
}

var wrapperSchema = `{
	"type": "record",
	"name": "LogWrapper",
	"fields": [
		{"name": "projectName", "type": "string"},
		{"name": "projectVersion", "type": "string"},
		{"name": "body", "type": "string"},
		{"name": "logLevel", "type": "string"},
		{"name": "logType", "type": "string"},
		{"name": "logSource", "type": "string"}
	]
}`

var logDataSchema = `{
	"type": "record",
	"name": "LogData",
	"fields": [
		{"name": "timestamp", "type": "long"},
		{"name": "logtype", "type": "string"},
		{"name": "version", "type": "string"},
		{"name": "issuer", "type": "string"},
		{"name": "metadata", "type": ["null", {"type": "map", "values": "string"}], "default": null},
		{"name": "domainData", "type": ["null", {"type": "map", "values": "string"}], "default": null}
	]
}`

type LogRequest struct {
	ProjectName    string  `json:"projectName" binding:"required"`
	ProjectVersion string  `json:"projectVersion" binding:"required"`
	LogLevel       string  `json:"logLevel" binding:"required"`
	LogType        string  `json:"logType" binding:"required"`
	LogSource      string  `json:"logSource" binding:"required"`
	LogBody        LogData `json:"body" binding:"required"`
}

type LogData struct {
	Timestamp  int64       `json:"timestamp" binding:"required"`
	Logtype    string      `json:"logtype" binding:"required"`
	Version    string      `json:"version" binding:"required"`
	Issuer     string      `json:"issuer" binding:"required"`
	Metadata   interface{} `json:"metadata,omitempty"`
	DomainData interface{} `json:"domainData,omitempty"`
}

type PingRequest struct {
	Data interface{} `json:"data"`
}

type PingResponse struct {
	Status    string      `json:"status"`
	Timestamp int64       `json:"timestamp"`
	Message   string      `json:"message"`
	Echo      interface{} `json:"echo"`
}

var logger *zap.Logger

func main() {
	var err error
	logger, err = setupLogger()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	r := gin.Default()

	r.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	})

	r.POST("/ping", pingHandler)
	r.POST("/log", logHandler)

	fmt.Println("Server starting on :8080")
	r.Run(":8080")
}

func pingHandler(c *gin.Context) {
	start := time.Now()

	var req PingRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		logger.Error("Failed to bind ping request",
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
			zap.String("client_ip", c.ClientIP()),
			zap.String("error", err.Error()),
			zap.Duration("duration", time.Since(start)))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	response := PingResponse{
		Status:    "ok",
		Timestamp: time.Now().Unix(),
		Message:   "Server is running - ready for Unreal Engine communication",
		Echo:      req.Data,
	}

	logger.Info("Ping request processed",
		zap.String("method", c.Request.Method),
		zap.String("path", c.Request.URL.Path),
		zap.String("client_ip", c.ClientIP()),
		zap.Int("status", http.StatusOK),
		zap.Duration("duration", time.Since(start)),
		zap.Any("request_data", req.Data))

	c.JSON(http.StatusOK, response)
}

func logHandler(c *gin.Context) {
	var req LogRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		logger.Error("Failed to bind log request", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	wrapperCodec, err := goavro.NewCodec(wrapperSchema)
	if err != nil {
		logger.Error("Failed to create wrapper Avro codec", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create wrapper Avro codec"})
		return
	}

	logDataCodec, err := goavro.NewCodec(logDataSchema)
	if err != nil {
		logger.Error("Failed to create log data Avro codec", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create log data Avro codec"})
		return
	}

	// Convert metadata and domainData to Avro-compatible format
	var metadataForAvro interface{}
	if req.LogBody.Metadata != nil {
		metadataForAvro = convertToAvroMap(req.LogBody.Metadata)
	}

	var domainDataForAvro interface{}
	if req.LogBody.DomainData != nil {
		domainDataForAvro = convertToAvroMap(req.LogBody.DomainData)
	}

	// Create Avro LogData struct
	avroLogData := AvroLogData{
		Timestamp:  req.LogBody.Timestamp,
		Logtype:    req.LogBody.Logtype,
		Version:    req.LogBody.Version,
		Issuer:     req.LogBody.Issuer,
		Metadata:   metadataForAvro,
		DomainData: domainDataForAvro,
	}

	// Convert struct to map for goavro
	logDataRecord := structToMap(avroLogData)

	logDataBinary, err := logDataCodec.BinaryFromNative(nil, logDataRecord)
	if err != nil {
		logger.Error("Failed to encode log data to Avro binary", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to encode log data to Avro"})
		return
	}

	logDataNative, _, err := logDataCodec.NativeFromBinary(logDataBinary)
	if err != nil {
		logger.Error("Failed to decode log data from Avro binary", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to decode log data from Avro"})
		return
	}

	logDataJSON, err := logDataCodec.TextualFromNative(nil, logDataNative)
	if err != nil {
		logger.Error("Failed to convert log data to JSON", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to convert log data to JSON"})
		return
	}

	// Create Avro LogWrapper struct
	avroWrapper := AvroLogWrapper{
		ProjectName:    req.ProjectName,
		ProjectVersion: req.ProjectVersion,
		Body:           string(logDataJSON),
		LogLevel:       req.LogLevel,
		LogType:        req.LogType,
		LogSource:      req.LogSource,
	}

	// Convert struct to map for goavro
	wrapperRecord := structToMap(avroWrapper)

	wrapperBinary, err := wrapperCodec.BinaryFromNative(nil, wrapperRecord)
	if err != nil {
		logger.Error("Failed to encode wrapper to Avro binary", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to encode wrapper to Avro"})
		return
	}

	wrapperNative, _, err := wrapperCodec.NativeFromBinary(wrapperBinary)
	if err != nil {
		logger.Error("Failed to decode wrapper from Avro binary", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to decode wrapper from Avro"})
		return
	}

	wrapperJSON, err := wrapperCodec.TextualFromNative(nil, wrapperNative)
	if err != nil {
		logger.Error("Failed to convert wrapper to JSON", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to convert wrapper to JSON"})
		return
	}

	originalJSON, _ := json.Marshal(req)
	originalSize := len(originalJSON)
	wrapperAvroSize := len(wrapperBinary)
	logDataAvroSize := len(logDataBinary)
	wrapperJSONSize := len(wrapperJSON)

	logger.Info("Log processed",
		zap.Int("original_json_size", originalSize),
		zap.Int("wrapper_avro_size", wrapperAvroSize),
		zap.Int("logdata_avro_size", logDataAvroSize),
		zap.Int("wrapper_json_size", wrapperJSONSize))
	logger.Debug("Avro JSON output",
		zap.String("wrapper_avro_json", string(wrapperJSON)),
		zap.String("logdata_avro_json", string(logDataJSON)))

	c.JSON(http.StatusOK, gin.H{
		"status": "logged",
		"compression_stats": gin.H{
			"original_json_size":  originalSize,
			"wrapper_avro_size":   wrapperAvroSize,
			"logdata_avro_size":   logDataAvroSize,
			"wrapper_json_size":   wrapperJSONSize,
			"wrapper_compression": fmt.Sprintf("%.2f%%", float64(wrapperAvroSize)/float64(originalSize)*100),
			"logdata_compression": fmt.Sprintf("%.2f%%", float64(logDataAvroSize)/float64(originalSize)*100),
		},
		"wrapper_avro_json": string(wrapperJSON),
		"logdata_avro_json": string(logDataJSON),
	})
}
