package dynamodb

/*
// BenchmarkDynamoDBScanComparison_Quick runs a quick comparison between different segment counts
func BenchmarkDynamoDBScanComparison_Quick(b *testing.B) {
	// Setup: Ensure large dataset exists (run once)
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
out:
  type: file
  filepath: ./bench_quick_%d_segments.jsonl
  format: jsonl
`, endpoint, tc.totalSegments, tc.totalSegments)

			defer func() {
				outputFile := fmt.Sprintf("./bench_quick_%d_segments.jsonl", tc.totalSegments)
				if err := os.Remove(outputFile); err != nil {
					// Ignore error if file doesn't exist
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

			// Report custom metrics
			const totalRecords = 10000
			nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N)
			recordsPerSecond := float64(totalRecords) / (nsPerOp / 1e9)

			b.ReportMetric(recordsPerSecond, "records/sec")
			b.ReportMetric(float64(tc.totalSegments), "segments")
		})
	}
}

// BenchmarkDynamoDBScan1Segment runs only 1 segment benchmark for baseline
func BenchmarkDynamoDBScan1Segment(b *testing.B) {
	// Setup: Ensure large dataset exists
	if err := MigrateLargeDataset(client); err != nil {
		b.Fatalf("Could not migrate large dataset: %v", err)
	}

	configYml := fmt.Sprintf(`
in:
  type: dynamodb
  table: large_users
  endpoint: %v
  totalSegments: 1
  schema:
    id:
      type: string
    name:
      type: string
    age:
      type: number
out:
  type: file
  filepath: ./bench_1_segment.jsonl
  format: jsonl
`, endpoint)

	defer func() {
		if err := os.Remove("./bench_1_segment.jsonl"); err != nil {
			// Ignore error if file doesn't exist
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

// BenchmarkDynamoDBScan2Segments runs 2 segments benchmark
func BenchmarkDynamoDBScan2Segments(b *testing.B) {
	// Setup: Ensure large dataset exists
	if err := MigrateLargeDataset(client); err != nil {
		b.Fatalf("Could not migrate large dataset: %v", err)
	}

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
out:
  type: file
  filepath: ./bench_2_segments.jsonl
  format: jsonl
`, endpoint)

	defer func() {
		if err := os.Remove("./bench_2_segments.jsonl"); err != nil {
			// Ignore error if file doesn't exist
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

// BenchmarkDynamoDBScan4Segments runs 4 segments benchmark
func BenchmarkDynamoDBScan4Segments(b *testing.B) {
	// Setup: Ensure large dataset exists
	if err := MigrateLargeDataset(client); err != nil {
		b.Fatalf("Could not migrate large dataset: %v", err)
	}

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
out:
  type: file
  filepath: ./bench_4_segments.jsonl
  format: jsonl
`, endpoint)

	defer func() {
		if err := os.Remove("./bench_4_segments.jsonl"); err != nil {
			// Ignore error if file doesn't exist
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
*/
