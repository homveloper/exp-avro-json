package main

import (
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func setupLogger() (*zap.Logger, error) {
	// Create logs directory if it doesn't exist
	logsDir := "logs"
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		return nil, err
	}

	// Create log files
	logFile := filepath.Join(logsDir, "app.log")
	errorFile := filepath.Join(logsDir, "error.log")

	// Configure log levels
	infoLevel := zap.LevelEnablerFunc(func(level zapcore.Level) bool {
		return level >= zapcore.InfoLevel
	})

	errorLevel := zap.LevelEnablerFunc(func(level zapcore.Level) bool {
		return level >= zapcore.ErrorLevel
	})

	// Configure encoders
	consoleEncoderConfig := zap.NewDevelopmentEncoderConfig()
	consoleEncoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout("15:04:05")
	consoleEncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder

	fileEncoderConfig := zap.NewProductionEncoderConfig()
	fileEncoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout("2006-01-02 15:04:05")

	// Create encoders
	consoleEncoder := zapcore.NewConsoleEncoder(consoleEncoderConfig)
	fileEncoder := zapcore.NewJSONEncoder(fileEncoderConfig)

	// Create writers
	consoleWriter := zapcore.AddSync(os.Stdout)

	infoFile, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	infoWriter := zapcore.AddSync(infoFile)

	errorFileHandle, err := os.OpenFile(errorFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	errorWriter := zapcore.AddSync(errorFileHandle)

	// Create cores
	core := zapcore.NewTee(
		// Console output (colored, readable)
		zapcore.NewCore(consoleEncoder, consoleWriter, zap.DebugLevel),
		// Info file output (JSON format, all logs)
		zapcore.NewCore(fileEncoder, infoWriter, infoLevel),
		// Error file output (JSON format, errors only)
		zapcore.NewCore(fileEncoder, errorWriter, errorLevel),
	)

	// Create logger with caller info
	logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))

	// Add initial log
	logger.Info("Logger initialized",
		zap.String("app", "exp-avro-json-server"),
		zap.String("version", "1.0.0"),
		zap.Time("started_at", time.Now()),
		zap.String("log_file", logFile),
		zap.String("error_file", errorFile))

	return logger, nil
}
