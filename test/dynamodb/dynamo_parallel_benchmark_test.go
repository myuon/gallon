package dynamodb

import (
	"fmt"
	"os"
	"testing"

	"github.com/myuon/gallon/cmd"
)

// BenchmarkDynamoDBScan_1Segment benchmarks single segment scan
func BenchmarkDynamoDBScan_1Segment(b *testing.B) {
	benchmarkDynamoDBScan(b, 1)
}

// BenchmarkDynamoDBScan_2Segments benchmarks 2 segments scan
func BenchmarkDynamoDBScan_2Segments(b *testing.B) {
	benchmarkDynamoDBScan(b, 2)
}

// BenchmarkDynamoDBScan_4Segments benchmarks 4 segments scan
func BenchmarkDynamoDBScan_4Segments(b *testing.B) {
	benchmarkDynamoDBScan(b, 4)
}

// BenchmarkDynamoDBScan_8Segments benchmarks 8 segments scan
func BenchmarkDynamoDBScan_8Segments(b *testing.B) {
	benchmarkDynamoDBScan(b, 8)
}

func benchmarkDynamoDBScan(b *testing.B, totalSegments int32) {
	// Setup: Ensure large dataset exists
	if err := MigrateLargeDataset(client); err != nil {
		b.Fatalf("Could not migrate large dataset: %v", err)
	}

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
    created_at:
      type: number
out:
  type: file
  filepath: ./benchmark_%d_segments.jsonl
  format: jsonl
`, endpoint, totalSegments, totalSegments)

	defer func() {
		outputFile := fmt.Sprintf("./benchmark_%d_segments.jsonl", totalSegments)
		if err := os.Remove(outputFile); err != nil {
			b.Logf("Could not remove output file %s: %s", outputFile, err)
		}
	}()

	// Reset timer to exclude setup time
	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		if err := cmd.RunGallon([]byte(configYml)); err != nil {
			b.Fatalf("Could not run command: %s", err)
		}
	}
}

// BenchmarkDynamoDBScanComparison runs a comprehensive comparison
func BenchmarkDynamoDBScanComparison(b *testing.B) {
	// Setup: Ensure large dataset exists
	if err := MigrateLargeDataset(client); err != nil {
		b.Fatalf("Could not migrate large dataset: %v", err)
	}

	testCases := []struct {
		name          string
		totalSegments int32
	}{
		{"1_segment", 1},
		{"2_segments", 2},
		{"4_segments", 4},
		{"8_segments", 8},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
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
    created_at:
      type: number
out:
  type: file
  filepath: ./benchmark_comp_%d_segments.jsonl
  format: jsonl
`, endpoint, tc.totalSegments, tc.totalSegments)

			defer func() {
				outputFile := fmt.Sprintf("./benchmark_comp_%d_segments.jsonl", tc.totalSegments)
				if err := os.Remove(outputFile); err != nil {
					b.Logf("Could not remove output file %s: %s", outputFile, err)
				}
			}()

			// Reset timer to exclude setup time
			b.ResetTimer()

			// Run the benchmark
			for i := 0; i < b.N; i++ {
				if err := cmd.RunGallon([]byte(configYml)); err != nil {
					b.Fatalf("Could not run command: %s", err)
				}
			}
		})
	}
}

// BenchmarkDynamoDBScanScalability tests scalability with different segment counts
func BenchmarkDynamoDBScanScalability(b *testing.B) {
	// Setup: Ensure large dataset exists
	if err := MigrateLargeDataset(client); err != nil {
		b.Fatalf("Could not migrate large dataset: %v", err)
	}

	segments := []int32{1, 2, 4, 6, 8, 10, 12, 16}

	for _, segmentCount := range segments {
		b.Run(fmt.Sprintf("%d_segments", segmentCount), func(b *testing.B) {
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
  filepath: ./benchmark_scale_%d_segments.jsonl
  format: jsonl
`, endpoint, segmentCount, segmentCount)

			defer func() {
				outputFile := fmt.Sprintf("./benchmark_scale_%d_segments.jsonl", segmentCount)
				if err := os.Remove(outputFile); err != nil {
					b.Logf("Could not remove output file %s: %s", outputFile, err)
				}
			}()

			// Reset timer to exclude setup time
			b.ResetTimer()

			// Run the benchmark
			for i := 0; i < b.N; i++ {
				if err := cmd.RunGallon([]byte(configYml)); err != nil {
					b.Fatalf("Could not run command: %s", err)
				}
			}

			// Calculate and report records per second
			recordsPerOp := float64(10000)
			nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N)
			recordsPerSecond := recordsPerOp / (nsPerOp / 1e9)

			b.ReportMetric(recordsPerSecond, "records/sec")
			b.ReportMetric(recordsPerOp, "records/op")
		})
	}
}

// BenchmarkDynamoDBScanWithMetrics includes detailed metrics
func BenchmarkDynamoDBScanWithMetrics(b *testing.B) {
	// Setup: Ensure large dataset exists
	if err := MigrateLargeDataset(client); err != nil {
		b.Fatalf("Could not migrate large dataset: %v", err)
	}

	testCases := []struct {
		name          string
		totalSegments int32
	}{
		{"baseline_1seg", 1},
		{"parallel_2seg", 2},
		{"parallel_4seg", 4},
		{"parallel_8seg", 8},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
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
    created_at:
      type: number
out:
  type: file
  filepath: ./benchmark_metrics_%d_segments.jsonl
  format: jsonl
`, endpoint, tc.totalSegments, tc.totalSegments)

			defer func() {
				outputFile := fmt.Sprintf("./benchmark_metrics_%d_segments.jsonl", tc.totalSegments)
				if err := os.Remove(outputFile); err != nil {
					b.Logf("Could not remove output file %s: %s", outputFile, err)
				}
			}()

			// Reset timer to exclude setup time
			b.ResetTimer()

			// Run the benchmark
			for i := 0; i < b.N; i++ {
				if err := cmd.RunGallon([]byte(configYml)); err != nil {
					b.Fatalf("Could not run command: %s", err)
				}
			}

			// Calculate metrics
			const totalRecords = 10000
			avgNsPerOp := b.Elapsed().Nanoseconds() / int64(b.N)
			recordsPerSecond := float64(totalRecords) / (float64(avgNsPerOp) / 1e9)
			
			// Report custom metrics
			b.ReportMetric(recordsPerSecond, "records/sec")
			b.ReportMetric(float64(totalRecords), "records/op")
			b.ReportMetric(float64(tc.totalSegments), "segments")
			b.ReportMetric(recordsPerSecond/float64(tc.totalSegments), "records/sec/segment")
		})
	}
}