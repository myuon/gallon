# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## About Gallon

Gallon is a Go-based data migration tool that supports various input and output plugins for moving data between different databases and storage systems.

## Common Development Commands

### Building and Running
- Build: `go build .`
- Run with config: `go run main.go run /path/to/config.yml`
- Run with template parsing: `go run main.go run --template /path/to/config.yml`
- Run with environment variables: `go run main.go run --template-with-env /path/to/config.yml`

### Testing
- Run all tests: `go test ./...`
- Run specific test directory: `go test ./test/random/`
- Run with verbose output: `go test -v ./...`

### Docker Environment
- Start services: `make up` (starts docker-compose services)
- Stop services: `make down`
- Migrate test data to MySQL: `make migrate-mysql`

### Documentation
- Start local documentation server: `make doc`

## Architecture Overview

### Core Components

1. **Gallon Engine** (`gallon/gallon.go`): The main engine that orchestrates data migration
   - Uses goroutines for concurrent extract and load operations
   - Implements error handling with configurable error limits
   - Uses ordered maps to preserve field order in records

2. **Plugin System**: Input and output plugins are dynamically selected based on config `type` field
   - **Input Plugins**: DynamoDB, SQL (MySQL/PostgreSQL), Random data generator
   - **Output Plugins**: BigQuery, File (CSV/JSONL), Stdout

3. **Configuration**: YAML-based configuration with optional Go template support
   - Supports environment variable injection with `--template-with-env`
   - Schema definitions for type mapping and field transformations

### Key Data Flow

1. Configuration is parsed and plugins are instantiated
2. Extract goroutine reads from input source and sends batches to channel
3. Load goroutine receives batches and writes to output destination
4. Error handling goroutine monitors for excessive errors and cancels operation if needed

### Plugin Implementation

Each plugin implements either `InputPlugin` or `OutputPlugin` interfaces:
- Input plugins implement `Extract(ctx, messages chan, errs chan) error`
- Output plugins implement `Load(ctx, messages chan, errs chan) error`
- All plugins implement `BasePlugin` for logging and cleanup

### Testing Strategy

Integration tests are located in `test/` directory, organized by migration type:
- Each test uses `cmd.RunGallon()` with YAML config strings
- Tests cover various plugin combinations (e.g., `mysql_to_bigquery`, `dynamo_to_bigquery`)
- Uses dockertest for database integration testing

### Logging

Uses zap logger with structured JSON output by default. Set `LOGENV=development` for human-readable colored logs during development.