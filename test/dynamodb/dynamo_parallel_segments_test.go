package dynamodb

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/myuon/gallon/cmd"
)

func Test_dynamodb_parallel_segments_2_segments(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping parallel segments test in short mode")
	}

	// Ensure large dataset exists
	if err := MigrateLargeDataset(client); err != nil {
		t.Fatalf("Could not migrate large dataset: %v", err)
	}

	startTime := time.Now()

	configYml := fmt.Sprintf(`
in:
  type: dynamodb
  table: large_users
  endpoint: %v
  totalSegments: 2
  schema:
    id:
      type: string
    name:
      type: string
    age:
      type: number
    created_at:
      type: number
    address:
      type: object
      properties:
        street:
          type: string
        city:
          type: string
        country:
          type: string
    skills:
      type: array
      items:
        type: object
        properties:
          name:
            type: string
          level:
            type: number
          category:
            type: string
out:
  type: file
  filepath: ./parallel_2_segments_output.jsonl
  format: jsonl
`, endpoint)

	defer func() {
		if err := os.Remove("./parallel_2_segments_output.jsonl"); err != nil {
			t.Logf("Could not remove output file: %s", err)
		}
	}()

	t.Log("Starting parallel scan with 2 segments...")
	if err := cmd.RunGallon([]byte(configYml)); err != nil {
		t.Fatalf("Could not run command: %s", err)
	}

	duration := time.Since(startTime)
	t.Logf("Parallel scan with 2 segments completed in %v", duration)

	// Verify output file
	jsonl, err := os.ReadFile("./parallel_2_segments_output.jsonl")
	if err != nil {
		t.Fatalf("Could not read output file: %s", err)
	}

	lines := strings.Split(string(jsonl), "\n")
	expectedLines := 10001 // 10,000 records + 1 empty line
	if len(lines) != expectedLines {
		t.Errorf("Expected %d lines, got %d", expectedLines, len(lines))
	}

	recordsPerSecond := float64(10000) / duration.Seconds()
	t.Logf("Performance with 2 segments: %.2f records/second", recordsPerSecond)
}

func Test_dynamodb_parallel_segments_4_segments(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping parallel segments test in short mode")
	}

	// Ensure large dataset exists
	if err := MigrateLargeDataset(client); err != nil {
		t.Fatalf("Could not migrate large dataset: %v", err)
	}

	startTime := time.Now()

	configYml := fmt.Sprintf(`
in:
  type: dynamodb
  table: large_users
  endpoint: %v
  totalSegments: 4
  schema:
    id:
      type: string
    name:
      type: string
    age:
      type: number
    created_at:
      type: number
out:
  type: file
  filepath: ./parallel_4_segments_output.jsonl
  format: jsonl
`, endpoint)

	defer func() {
		if err := os.Remove("./parallel_4_segments_output.jsonl"); err != nil {
			t.Logf("Could not remove output file: %s", err)
		}
	}()

	t.Log("Starting parallel scan with 4 segments...")
	if err := cmd.RunGallon([]byte(configYml)); err != nil {
		t.Fatalf("Could not run command: %s", err)
	}

	duration := time.Since(startTime)
	t.Logf("Parallel scan with 4 segments completed in %v", duration)

	// Verify output file
	jsonl, err := os.ReadFile("./parallel_4_segments_output.jsonl")
	if err != nil {
		t.Fatalf("Could not read output file: %s", err)
	}

	lines := strings.Split(string(jsonl), "\n")
	expectedLines := 10001 // 10,000 records + 1 empty line
	if len(lines) != expectedLines {
		t.Errorf("Expected %d lines, got %d", expectedLines, len(lines))
	}

	recordsPerSecond := float64(10000) / duration.Seconds()
	t.Logf("Performance with 4 segments: %.2f records/second", recordsPerSecond)
}

func Test_dynamodb_parallel_segments_performance_comparison(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance comparison test in short mode")
	}

	// Ensure large dataset exists
	if err := MigrateLargeDataset(client); err != nil {
		t.Fatalf("Could not migrate large dataset: %v", err)
	}

	tests := []struct {
		name         string
		totalSegments int32
		outputFile   string
	}{
		{"single_segment", 1, "./perf_1_segment.jsonl"},
		{"two_segments", 2, "./perf_2_segments.jsonl"},
		{"four_segments", 4, "./perf_4_segments.jsonl"},
	}

	results := make([]struct {
		name     string
		duration time.Duration
		rps      float64
	}, len(tests))

	for i, test := range tests {
		t.Logf("Running performance test: %s", test.name)
		
		startTime := time.Now()

		configYml := fmt.Sprintf(`
in:
  type: dynamodb
  table: large_users
  endpoint: %v
  totalSegments: %d
  schema:
    id:
      type: string
    name:
      type: string
    age:
      type: number
out:
  type: file
  filepath: %s
  format: jsonl
`, endpoint, test.totalSegments, test.outputFile)

		defer func(file string) {
			if err := os.Remove(file); err != nil {
				t.Logf("Could not remove output file %s: %s", file, err)
			}
		}(test.outputFile)

		if err := cmd.RunGallon([]byte(configYml)); err != nil {
			t.Fatalf("Could not run command for %s: %s", test.name, err)
		}

		duration := time.Since(startTime)
		recordsPerSecond := float64(10000) / duration.Seconds()

		results[i] = struct {
			name     string
			duration time.Duration
			rps      float64
		}{test.name, duration, recordsPerSecond}

		t.Logf("%s: %v (%.2f records/second)", test.name, duration, recordsPerSecond)

		// Verify record count
		jsonl, err := os.ReadFile(test.outputFile)
		if err != nil {
			t.Fatalf("Could not read output file %s: %s", test.outputFile, err)
		}

		lines := strings.Split(string(jsonl), "\n")
		expectedLines := 10001
		if len(lines) != expectedLines {
			t.Errorf("Expected %d lines in %s, got %d", expectedLines, test.name, len(lines))
		}
	}

	// Print summary
	t.Log("\nPerformance Summary:")
	for _, result := range results {
		t.Logf("  %s: %v (%.2f rps)", result.name, result.duration, result.rps)
	}

	// Check if parallel segments improved performance
	if len(results) >= 2 {
		singleSegmentRPS := results[0].rps
		twoSegmentRPS := results[1].rps
		improvement := (twoSegmentRPS - singleSegmentRPS) / singleSegmentRPS * 100

		t.Logf("Two segments vs single segment improvement: %.1f%%", improvement)
		
		if improvement > 0 {
			t.Logf("Parallel segments showed performance improvement!")
		} else {
			t.Logf("Note: Performance improvement may depend on table size and available resources")
		}
	}
}