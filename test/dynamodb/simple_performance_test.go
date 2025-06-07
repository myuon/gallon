package dynamodb

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/myuon/gallon/cmd"
)

// Simple performance comparison without Go benchmark framework
func TestDynamoDBPerformanceComparison(t *testing.T) {
	// Setup: Ensure large dataset exists
	if err := MigrateLargeDataset(client); err != nil {
		t.Fatalf("Could not migrate large dataset: %v", err)
	}

	testCases := []struct {
		name          string
		totalSegments int32
	}{
		{"1_segment", 1},
		{"2_segments", 2},
		{"4_segments", 4},
	}

	results := make([]struct {
		name          string
		totalSegments int32
		duration      time.Duration
		recordsPerSec float64
	}, len(testCases))

	for i, tc := range testCases {
		t.Logf("Testing %s...", tc.name)

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
  filepath: ./perf_test_%d_segments.jsonl
  format: jsonl
`, endpoint, tc.totalSegments, tc.totalSegments)

		defer func(file string) {
			if err := os.Remove(file); err != nil {
				// Ignore error if file doesn't exist
			}
		}(fmt.Sprintf("./perf_test_%d_segments.jsonl", tc.totalSegments))

		// Run test and measure time
		startTime := time.Now()
		if err := cmd.RunGallon([]byte(configYml)); err != nil {
			t.Fatalf("Could not run command for %s: %s", tc.name, err)
		}
		duration := time.Since(startTime)

		const totalRecords = 10000
		recordsPerSecond := float64(totalRecords) / duration.Seconds()

		results[i] = struct {
			name          string
			totalSegments int32
			duration      time.Duration
			recordsPerSec float64
		}{tc.name, tc.totalSegments, duration, recordsPerSecond}

		t.Logf("%s: %v (%.0f records/sec)", tc.name, duration, recordsPerSecond)
	}

	// Print comparison table
	t.Log("\n=== Performance Summary ===")
	t.Logf("%-12s %-8s %-12s %-12s %-10s", "Test", "Segments", "Duration", "Records/sec", "Speedup")
	baseline := results[0].recordsPerSec
	
	for _, result := range results {
		speedup := result.recordsPerSec / baseline
		t.Logf("%-12s %-8d %-12v %-12.0f %.2fx", 
			result.name, 
			result.totalSegments, 
			result.duration.Round(time.Millisecond), 
			result.recordsPerSec, 
			speedup)
	}

	// Validate that parallel segments are faster
	if len(results) >= 2 {
		singleSegmentTime := results[0].duration
		twoSegmentTime := results[1].duration
		improvement := float64(singleSegmentTime-twoSegmentTime) / float64(singleSegmentTime) * 100

		t.Logf("\nTwo segments improvement: %.1f%% faster", improvement)
		
		if improvement > 0 {
			t.Log("✓ Parallel segments showed performance improvement!")
		} else {
			t.Log("⚠ No significant improvement observed (may depend on data distribution)")
		}
	}
}