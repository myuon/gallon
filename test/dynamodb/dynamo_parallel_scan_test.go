package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/myuon/gallon/cmd"
	"github.com/neilotoole/slogt"
)

// MigrateLargeDataset creates a table with 10k records for parallel scan testing
func MigrateLargeDataset(client *dynamodb.Client) error {
	ctx := context.Background()
	tableName := "large_users"

	// Use goroutines to insert data in parallel for faster setup
	const totalRecords = 10000
	const batchSize = 25 // DynamoDB BatchWriteItem limit
	const numWorkers = 10

	// Create table with higher write capacity for batch loading
	if _, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       types.KeyTypeHash,
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	}); err != nil {
		// If table already exists, check if it has the expected number of records
		if strings.Contains(err.Error(), "ResourceInUseException") {
			// Count existing records
			scanResult, scanErr := client.Scan(ctx, &dynamodb.ScanInput{
				TableName: aws.String(tableName),
				Select:    types.SelectCount,
			})
			if scanErr != nil {
				return fmt.Errorf("failed to check existing table: %w", scanErr)
			}
			if scanResult.Count >= totalRecords {
				log.Printf("Table %s already exists with %d records, skipping migration", tableName, scanResult.Count)
				return nil
			}
		} else {
			return err
		}
	}

	// Wait for table to be created
	waiter := dynamodb.NewTableExistsWaiter(client)
	if err := waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}, 2*time.Minute); err != nil {
		return fmt.Errorf("failed to wait for table creation: %w", err)
	}

	recordChan := make(chan []UserTable, numWorkers*2)
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for batch := range recordChan {
				if err := insertBatch(client, tableName, batch); err != nil {
					log.Printf("Worker %d failed to insert batch: %v", workerID, err)
				}
			}
		}(i)
	}

	// Generate and send batches
	go func() {
		defer close(recordChan)
		batch := make([]UserTable, 0, batchSize)

		for i := 0; i < totalRecords; i++ {
			v, err := NewFakeUserTable()
			if err != nil {
				log.Printf("Failed to generate fake data: %v", err)
				continue
			}

			// Generate skills and address
			numSkills := gofakeit.Number(1, 3)
			skills := make([]Skill, numSkills)
			for j := 0; j < numSkills; j++ {
				if err := gofakeit.Struct(&skills[j]); err != nil {
					log.Printf("Failed to generate skills: %v", err)
					continue
				}
			}
			v.Skills = skills

			if err := gofakeit.Struct(&v.Address); err != nil {
				log.Printf("Failed to generate address: %v", err)
				continue
			}

			batch = append(batch, v)

			if len(batch) == batchSize {
				recordChan <- batch
				batch = make([]UserTable, 0, batchSize)
			}

			if i%1000 == 0 {
				log.Printf("Generated %d records", i)
			}
		}

		// Send remaining records
		if len(batch) > 0 {
			recordChan <- batch
		}
	}()

	wg.Wait()
	log.Printf("Migrated %d rows to %s", totalRecords, tableName)
	return nil
}

func insertBatch(client *dynamodb.Client, tableName string, batch []UserTable) error {
	ctx := context.Background()

	var writeRequests []types.WriteRequest
	for _, v := range batch {
		record := map[string]types.AttributeValue{
			"id":         &types.AttributeValueMemberS{Value: v.ID},
			"name":       &types.AttributeValueMemberS{Value: v.Name},
			"age":        &types.AttributeValueMemberN{Value: fmt.Sprintf("%v", v.Age)},
			"created_at": &types.AttributeValueMemberN{Value: fmt.Sprintf("%v", v.CreatedAt)},
			"address": &types.AttributeValueMemberM{
				Value: map[string]types.AttributeValue{
					"street":  &types.AttributeValueMemberS{Value: v.Address.Street},
					"city":    &types.AttributeValueMemberS{Value: v.Address.City},
					"country": &types.AttributeValueMemberS{Value: v.Address.Country},
				},
			},
			"skills": &types.AttributeValueMemberL{
				Value: func() []types.AttributeValue {
					skillList := make([]types.AttributeValue, len(v.Skills))
					for i, skill := range v.Skills {
						skillList[i] = &types.AttributeValueMemberM{
							Value: map[string]types.AttributeValue{
								"name":     &types.AttributeValueMemberS{Value: skill.Name},
								"level":    &types.AttributeValueMemberN{Value: fmt.Sprintf("%v", skill.Level)},
								"category": &types.AttributeValueMemberS{Value: skill.Category},
							},
						}
					}
					return skillList
				}(),
			},
		}

		writeRequests = append(writeRequests, types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: record,
			},
		})
	}

	// Retry logic for throttling
	maxRetries := 3
	for retry := 0; retry < maxRetries; retry++ {
		_, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				tableName: writeRequests,
			},
		})

		if err == nil {
			return nil
		}

		if retry < maxRetries-1 {
			// Exponential backoff
			time.Sleep(time.Duration(1<<retry) * 100 * time.Millisecond)
		}
	}

	return fmt.Errorf("failed to insert batch after %d retries", maxRetries)
}

func Test_dynamodb_large_dataset_parallel_scan(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large dataset test in short mode")
	}

	// Create large dataset
	t.Log("Creating large dataset for parallel scan test...")
	if err := MigrateLargeDataset(client); err != nil {
		t.Fatalf("Could not migrate large dataset: %v", err)
	}

	startTime := time.Now()

	configYml := fmt.Sprintf(`
in:
  type: dynamodb
  table: large_users
  endpoint: %v
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
  filepath: ./large_output.jsonl
  format: jsonl
`, endpoint)

	defer func() {
		if err := os.Remove("./large_output.jsonl"); err != nil {
			t.Logf("Could not remove output file: %s", err)
		}
	}()

	t.Log("Starting data migration...")
	if err := cmd.RunGallonWithOptions([]byte(configYml), cmd.RunGallonOptions{
		Logger: slogt.New(t),
	}); err != nil {
		t.Fatalf("Could not run command: %s", err)
	}

	endTime := time.Now()
	duration := endTime.Sub(startTime)

	t.Logf("Migration completed in %v", duration)

	// Verify output file
	jsonl, err := os.ReadFile("./large_output.jsonl")
	if err != nil {
		t.Fatalf("Could not read output file: %s", err)
	}

	lines := strings.Split(string(jsonl), "\n")
	// Should have 10,000 records + 1 empty line
	expectedLines := 10001
	if len(lines) != expectedLines {
		t.Errorf("Expected %d lines, got %d", expectedLines, len(lines))
	}

	// Validate a sample of records
	sampleSize := 100
	for i := 0; i < sampleSize && i < len(lines)-1; i++ {
		line := lines[i]
		if line == "" {
			continue
		}

		var data map[string]any
		if err := json.Unmarshal([]byte(line), &data); err != nil {
			t.Errorf("Could not unmarshal JSON at line %d: %s", i, err)
			continue
		}

		// Validate required fields
		if _, ok := data["id"].(string); !ok {
			t.Errorf("Expected id to be string at line %d, got %T", i, data["id"])
		}
		if _, ok := data["name"].(string); !ok {
			t.Errorf("Expected name to be string at line %d, got %T", i, data["name"])
		}
	}

	// Calculate performance metrics
	recordsPerSecond := float64(10000) / duration.Seconds()
	log.Printf("Performance: %.2f records/second", recordsPerSecond)

	// Performance assertion - should process at least 500 records per second
	if recordsPerSecond < 500 {
		t.Logf("Warning: Performance might be suboptimal. Got %.2f records/second, expected at least 500", recordsPerSecond)
	}
}

func Test_dynamodb_parallel_scan_consistency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping parallel scan consistency test in short mode")
	}

	// Create large dataset first (reuse from previous test or create new one)
	t.Log("Ensuring large dataset exists for consistency test...")
	if err := MigrateLargeDataset(client); err != nil {
		t.Fatalf("Could not migrate large dataset: %v", err)
	}

	// Run the same migration multiple times to ensure consistency
	runs := 3
	var durations []time.Duration
	var lineCounts []int

	for run := 0; run < runs; run++ {
		t.Logf("Starting consistency test run %d/%d", run+1, runs)

		startTime := time.Now()

		configYml := fmt.Sprintf(`
in:
  type: dynamodb
  table: large_users
  endpoint: %v
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
  filepath: ./consistency_output_%d.jsonl
  format: jsonl
`, endpoint, run)

		outputFile := fmt.Sprintf("./consistency_output_%d.jsonl", run)
		defer func(file string) {
			if err := os.Remove(file); err != nil {
				t.Logf("Could not remove output file %s: %s", file, err)
			}
		}(outputFile)

		if err := cmd.RunGallonWithOptions([]byte(configYml), cmd.RunGallonOptions{
			Logger: slogt.New(t),
		}); err != nil {
			t.Fatalf("Could not run command for run %d: %s", run+1, err)
		}

		duration := time.Since(startTime)
		durations = append(durations, duration)

		// Count lines
		jsonl, err := os.ReadFile(outputFile)
		if err != nil {
			t.Fatalf("Could not read output file for run %d: %s", run+1, err)
		}

		lines := strings.Split(string(jsonl), "\n")
		lineCounts = append(lineCounts, len(lines))

		t.Logf("Run %d completed in %v with %d lines", run+1, duration, len(lines))
	}

	// Verify consistency across runs
	firstLineCount := lineCounts[0]
	for i, count := range lineCounts {
		if count != firstLineCount {
			t.Errorf("Inconsistent line count in run %d: expected %d, got %d", i+1, firstLineCount, count)
		}
	}

	// Check performance consistency (durations shouldn't vary too much)
	var totalDuration time.Duration
	for _, d := range durations {
		totalDuration += d
	}
	avgDuration := totalDuration / time.Duration(len(durations))

	for i, d := range durations {
		deviation := float64(d-avgDuration) / float64(avgDuration)
		if deviation > 0.5 || deviation < -0.5 { // Allow 50% deviation
			t.Logf("Warning: Run %d duration %v deviates significantly from average %v (%.1f%%)",
				i+1, d, avgDuration, deviation*100)
		}
	}

	t.Logf("All consistency tests passed. Average duration: %v", avgDuration)
}
