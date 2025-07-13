# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an experimental project for optimizing JSON data serialization using Apache Avro. The goal is to measure compression efficiency and performance for log storage optimization and real-time data transmission between a Go server and Unreal Engine client.

## Architecture

- **Server** (`server/`): Go HTTP server using Gin framework with Avro serialization
  - Exposes `/ping` endpoint for health checks  
  - Exposes `/log` endpoint for processing log data with Avro compression
  - Uses `linkedin/goavro/v2` library for Avro operations
  - Provides compression statistics comparing original JSON vs Avro binary vs Avro JSON formats

- **Client** (`client/`): Unreal Engine implementation (currently empty directory)
  - Intended for communicating with Go server using Avro JSON format

## Development Commands

### Server Development
```bash
cd server
go mod tidy          # Install/update dependencies
go run main.go       # Run development server on :8080
go build            # Build binary
```

### Key Dependencies
- `github.com/gin-gonic/gin` - HTTP web framework
- `github.com/linkedin/goavro/v2` - Avro serialization library

## Avro Schema

The server uses a predefined log schema located in `server/main.go:14-24` with fields:
- `timestamp` (long)
- `level` (string) 
- `message` (string)
- `source` (string)
- `data` (optional string)

## Server Endpoints

- `GET /ping` - Health check endpoint
- `POST /log` - Accepts JSON log data, converts to Avro, returns compression stats and Avro JSON

## Testing the Server

```bash
# Health check
curl http://localhost:8080/ping

# Send log data
curl -X POST http://localhost:8080/log \
  -H "Content-Type: application/json" \
  -d '{"level":"info","message":"test message","source":"test","data":{"key":"value"}}'
```