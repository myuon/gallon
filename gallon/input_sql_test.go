package gallon

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	orderedmap "github.com/wk8/go-ordered-map/v2"
)

// identitySerialize is a minimal serialize function that copies every column
// from the source row into the GallonRecord as-is (no schema/type transform),
// mirroring the raw-query-mode serializer in NewInputPluginSqlFromConfig.
func identitySerialize(item orderedmap.OrderedMap[string, any]) (GallonRecord, error) {
	record := NewGallonRecord()
	for pair := item.Oldest(); pair != nil; pair = pair.Next() {
		record.Set(pair.Key, pair.Value)
	}

	return record, nil
}

func Test_buildLegacyPagedQuery(t *testing.T) {
	tests := []struct {
		name      string
		driver    string
		tableName string
		rawQuery  string
		pageSize  int
		want      string
		wantErr   bool
	}{
		{
			name:      "mysql table mode",
			driver:    "mysql",
			tableName: "users",
			pageSize:  1000,
			want:      "SELECT * FROM users LIMIT 1000 OFFSET ?",
		},
		{
			name:      "postgres table mode",
			driver:    "postgres",
			tableName: "users",
			pageSize:  1000,
			want:      "SELECT * FROM users LIMIT 1000 OFFSET $1",
		},
		{
			name:     "mysql raw query mode",
			driver:   "mysql",
			rawQuery: "SELECT id, name FROM users WHERE age > 50",
			pageSize: 500,
			want:     "SELECT * FROM (SELECT id, name FROM users WHERE age > 50) AS __gallon_raw_query LIMIT 500 OFFSET ?",
		},
		{
			name:     "postgres raw query mode",
			driver:   "postgres",
			rawQuery: "SELECT id, name FROM users WHERE age > 50",
			pageSize: 500,
			want:     "SELECT * FROM (SELECT id, name FROM users WHERE age > 50) AS __gallon_raw_query LIMIT 500 OFFSET $1",
		},
		{
			name:      "unsupported driver",
			driver:    "sqlite3",
			tableName: "users",
			pageSize:  100,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildLegacyPagedQuery(tt.driver, tt.tableName, tt.rawQuery, tt.pageSize)
			if (err != nil) != tt.wantErr {
				t.Fatalf("buildLegacyPagedQuery() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if got != tt.want {
				t.Errorf("buildLegacyPagedQuery() = %q, want %q", got, tt.want)
			}
		})
	}
}

func Test_buildKeysetPagedQueries(t *testing.T) {
	tests := []struct {
		name          string
		driver        string
		tableName     string
		rawQuery      string
		paginationKey string
		wantFirstPage string
		wantNextPage  string
		wantErr       bool
	}{
		{
			name:          "mysql table mode",
			driver:        "mysql",
			tableName:     "users",
			paginationKey: "id",
			wantFirstPage: "SELECT * FROM users ORDER BY id LIMIT ?",
			wantNextPage:  "SELECT * FROM users WHERE id > ? ORDER BY id LIMIT ?",
		},
		{
			name:          "postgres table mode",
			driver:        "postgres",
			tableName:     "users",
			paginationKey: "id",
			wantFirstPage: "SELECT * FROM users ORDER BY id LIMIT $1",
			wantNextPage:  "SELECT * FROM users WHERE id > $1 ORDER BY id LIMIT $2",
		},
		{
			name:          "mysql raw query mode",
			driver:        "mysql",
			rawQuery:      "SELECT id, name FROM users WHERE age > 50",
			paginationKey: "id",
			wantFirstPage: "SELECT * FROM (SELECT id, name FROM users WHERE age > 50) AS __gallon_raw_query ORDER BY id LIMIT ?",
			wantNextPage:  "SELECT * FROM (SELECT id, name FROM users WHERE age > 50) AS __gallon_raw_query WHERE id > ? ORDER BY id LIMIT ?",
		},
		{
			name:          "postgres raw query mode",
			driver:        "postgres",
			rawQuery:      "SELECT id, name FROM users WHERE age > 50",
			paginationKey: "id",
			wantFirstPage: "SELECT * FROM (SELECT id, name FROM users WHERE age > 50) AS __gallon_raw_query ORDER BY id LIMIT $1",
			wantNextPage:  "SELECT * FROM (SELECT id, name FROM users WHERE age > 50) AS __gallon_raw_query WHERE id > $1 ORDER BY id LIMIT $2",
		},
		{
			name:          "unsupported driver",
			driver:        "sqlite3",
			tableName:     "users",
			paginationKey: "id",
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotFirst, gotNext, err := buildKeysetPagedQueries(tt.driver, tt.tableName, tt.rawQuery, tt.paginationKey)
			if (err != nil) != tt.wantErr {
				t.Fatalf("buildKeysetPagedQueries() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if gotFirst != tt.wantFirstPage {
				t.Errorf("buildKeysetPagedQueries() firstPage = %q, want %q", gotFirst, tt.wantFirstPage)
			}
			if gotNext != tt.wantNextPage {
				t.Errorf("buildKeysetPagedQueries() nextPage = %q, want %q", gotNext, tt.wantNextPage)
			}
		})
	}
}

// Test_InputPluginSql_KeysetPagination_NoDuplicatesOnConcurrentInsert reproduces
// the scenario from https://github.com/myuon/gallon/issues/40: rows are
// inserted into the source table while a paginated Extract() is still in
// progress. With keyset pagination (paginationKey configured), every row
// that existed at the time it was queried is extracted exactly once, and
// rows appended after the current watermark are still picked up in a later
// page - unlike the legacy OFFSET-based pagination, which has no
// deterministic ordering and can duplicate or skip rows in this situation.
func Test_InputPluginSql_KeysetPagination_NoDuplicatesOnConcurrentInsert(t *testing.T) {
	columns := []string{"id", "name"}
	initialIDs := []int64{1, 2, 4, 6, 7, 8, 9, 10, 11, 12}
	initialRows := make([][]any, 0, len(initialIDs))
	for _, id := range initialIDs {
		initialRows = append(initialRows, []any{id, fmt.Sprintf("name-%d", id)})
	}

	const pageSize = 4

	onQuery := func(callIndex int, tbl *fakeTable) {
		// Right before the 2nd page is fetched (i.e. once the first page,
		// containing ids 1,2,4,6, has already been read and the watermark
		// is at id=6), simulate two concurrent writes to the source table:
		//   - id=3: a row inserted with a key that sorts *before* the
		//     current watermark. Under OFFSET pagination this is exactly
		//     the kind of write that shifts page boundaries and causes
		//     duplicate/skipped rows; keyset pagination simply will not
		//     revisit it, which is expected/documented behavior.
		//   - id=13: a row appended past the current maximum key, which
		//     must still be picked up in a later page (no data loss for
		//     genuinely new rows).
		if callIndex == 1 {
			tbl.insertLocked([]any{int64(3), "name-3-inserted-during-extraction"})
			tbl.insertLocked([]any{int64(13), "name-13-inserted-during-extraction"})
		}
	}

	db, _, err := newFakeSqlDB(columns, initialRows, onQuery)
	if err != nil {
		t.Fatalf("failed to create fake sql db: %v", err)
	}
	defer db.Close()

	plugin := NewInputPluginSqlWithPaginationKey(db, "users", "", "mysql", pageSize, "id", identitySerialize)
	plugin.ReplaceLogger(logr.Discard())

	messages := make(chan []GallonRecord)
	errs := make(chan error, 10)

	var wg sync.WaitGroup
	var allIDs []int64
	wg.Add(1)
	go func() {
		defer wg.Done()
		for msgs := range messages {
			for _, m := range msgs {
				idVal, ok := m.Get("id")
				if !ok {
					t.Errorf("record is missing id column")
					continue
				}
				allIDs = append(allIDs, idVal.(int64))
			}
		}
	}()

	extractErr := func() error {
		defer close(messages)
		return plugin.Extract(context.Background(), messages, errs)
	}()

	wg.Wait()

	if extractErr != nil {
		t.Fatalf("Extract returned error: %v", extractErr)
	}

	select {
	case err := <-errs:
		t.Fatalf("unexpected error on errs channel: %v", err)
	default:
	}

	seen := map[int64]int{}
	for _, id := range allIDs {
		seen[id]++
	}

	for id, count := range seen {
		if count > 1 {
			t.Errorf("id=%v was extracted %d times, want at most 1 (duplicate row - the exact bug from issue #40)", id, count)
		}
	}

	// Every row that existed throughout the run must have been extracted
	// (no skipped rows), and the row appended mid-run past the watermark
	// (id=13) must have been picked up too.
	wantPresent := append(append([]int64{}, initialIDs...), 13)
	for _, id := range wantPresent {
		if seen[id] == 0 {
			t.Errorf("id=%v was not extracted at all (skipped row)", id)
		}
	}
}

func Test_parseTimezone(t *testing.T) {
	tests := []struct {
		name        string
		tz          string
		wantOffset  int // offset in seconds
		wantErr     bool
		description string
	}{
		{
			name:        "IANA timezone UTC",
			tz:          "UTC",
			wantOffset:  0,
			wantErr:     false,
			description: "Standard UTC timezone",
		},
		{
			name:        "IANA timezone Asia/Tokyo",
			tz:          "Asia/Tokyo",
			wantOffset:  9 * 3600,
			wantErr:     false,
			description: "JST timezone",
		},
		{
			name:        "IANA timezone America/New_York",
			tz:          "America/New_York",
			wantOffset:  -5 * 3600, // EST (winter time)
			wantErr:     false,
			description: "EST timezone",
		},
		{
			name:        "Numeric offset +09:00",
			tz:          "+09:00",
			wantOffset:  9 * 3600,
			wantErr:     false,
			description: "Positive offset with colon",
		},
		{
			name:        "Numeric offset +9",
			tz:          "+9",
			wantOffset:  9 * 3600,
			wantErr:     false,
			description: "Positive offset without leading zero",
		},
		{
			name:        "Numeric offset -05:00",
			tz:          "-05:00",
			wantOffset:  -5 * 3600,
			wantErr:     false,
			description: "Negative offset with colon",
		},
		{
			name:        "Numeric offset -5",
			tz:          "-5",
			wantOffset:  -5 * 3600,
			wantErr:     false,
			description: "Negative offset without leading zero",
		},
		{
			name:        "Numeric offset +05:30",
			tz:          "+05:30",
			wantOffset:  5*3600 + 30*60,
			wantErr:     false,
			description: "Offset with minutes (India Standard Time)",
		},
		{
			name:        "Numeric offset +00:00",
			tz:          "+00:00",
			wantOffset:  0,
			wantErr:     false,
			description: "Zero offset",
		},
		{
			name:        "Numeric offset -08:00",
			tz:          "-08:00",
			wantOffset:  -8 * 3600,
			wantErr:     false,
			description: "PST timezone offset",
		},
		{
			name:        "Invalid timezone",
			tz:          "Invalid/Timezone",
			wantOffset:  0,
			wantErr:     true,
			description: "Should fail on invalid timezone",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loc, err := parseTimezone(tt.tz)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseTimezone() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			// Test the offset by checking what time it returns for a fixed UTC time
			// Use a fixed time: 2024-01-15 12:00:00 UTC
			utcTime := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
			localTime := utcTime.In(loc)

			// For IANA timezones, we need to get the actual offset at this specific time
			// because of daylight saving time
			_, actualOffset := localTime.Zone()

			if actualOffset != tt.wantOffset {
				t.Errorf("parseTimezone() offset = %d seconds (%+d hours), want %d seconds (%+d hours) - %s",
					actualOffset, actualOffset/3600, tt.wantOffset, tt.wantOffset/3600, tt.description)
			}
		})
	}
}

func Test_parseTimezone_conversion(t *testing.T) {
	// Test that converting times between timezones works correctly
	tests := []struct {
		name           string
		sourceTime     string
		sourceTz       string
		targetTz       string
		expectedTime   string
		expectedOffset int
	}{
		{
			name:           "JST to UTC",
			sourceTime:     "2024-01-15 10:30:00",
			sourceTz:       "+09:00",
			targetTz:       "UTC",
			expectedTime:   "2024-01-15 01:30:00",
			expectedOffset: 0,
		},
		{
			name:           "UTC to JST",
			sourceTime:     "2024-01-15 01:30:00",
			sourceTz:       "UTC",
			targetTz:       "+09:00",
			expectedTime:   "2024-01-15 10:30:00",
			expectedOffset: 9 * 3600,
		},
		{
			name:           "PST to EST",
			sourceTime:     "2024-01-15 09:00:00",
			sourceTz:       "-08:00",
			targetTz:       "-05:00",
			expectedTime:   "2024-01-15 12:00:00",
			expectedOffset: -5 * 3600,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sourceLoc, err := parseTimezone(tt.sourceTz)
			if err != nil {
				t.Fatalf("Failed to parse source timezone: %v", err)
			}

			targetLoc, err := parseTimezone(tt.targetTz)
			if err != nil {
				t.Fatalf("Failed to parse target timezone: %v", err)
			}

			// Parse the source time in the source timezone
			sourceTime, err := time.ParseInLocation("2006-01-02 15:04:05", tt.sourceTime, sourceLoc)
			if err != nil {
				t.Fatalf("Failed to parse source time: %v", err)
			}

			// Convert to target timezone
			targetTime := sourceTime.In(targetLoc)

			// Format and compare
			actualTimeStr := targetTime.Format("2006-01-02 15:04:05")
			if actualTimeStr != tt.expectedTime {
				t.Errorf("Time conversion = %s, want %s", actualTimeStr, tt.expectedTime)
			}

			// Check offset
			_, actualOffset := targetTime.Zone()
			if actualOffset != tt.expectedOffset {
				t.Errorf("Offset = %d seconds, want %d seconds", actualOffset, tt.expectedOffset)
			}
		})
	}
}

func extractAllRecords(t *testing.T, serialize func(orderedmap.OrderedMap[string, any]) (GallonRecord, error), columns []string, rows [][]any) []GallonRecord {
	t.Helper()

	db, _, err := newFakeSqlDB(columns, rows, nil)
	if err != nil {
		t.Fatalf("failed to create fake sql db: %v", err)
	}
	defer db.Close()

	plugin := NewInputPluginSqlWithPaginationKey(db, "", "SELECT * FROM t", "mysql", 10, "id", serialize)
	plugin.ReplaceLogger(logr.Discard())

	messages := make(chan []GallonRecord)
	errs := make(chan error, 10)

	var records []GallonRecord
	done := make(chan struct{})
	go func() {
		defer close(done)
		for msgs := range messages {
			records = append(records, msgs...)
		}
	}()

	extractErr := func() error {
		defer close(messages)
		return plugin.Extract(context.Background(), messages, errs)
	}()
	<-done

	if extractErr != nil {
		t.Fatalf("Extract returned error: %v", extractErr)
	}
	select {
	case err := <-errs:
		t.Fatalf("unexpected error on errs channel: %v", err)
	default:
	}

	return records
}

func Test_newSqlSerializer_RawQuery_BytesBecomeString(t *testing.T) {
	columns := []string{"id", "uuid", "n"}
	rows := [][]any{{[]byte("1"), []byte("db806676-aaaa"), int64(7)}}

	t.Run("without schema", func(t *testing.T) {
		var schema orderedmap.OrderedMap[string, InputPluginSqlConfigSchemaColumn]
		records := extractAllRecords(t, newSqlSerializer(schema, true), columns, rows)
		if len(records) != 1 {
			t.Fatalf("got %d records, want 1", len(records))
		}
		if v, _ := records[0].Get("uuid"); v != "db806676-aaaa" {
			t.Errorf("uuid = %#v, want string", v)
		}
		if v, _ := records[0].Get("n"); v != int64(7) {
			t.Errorf("n = %#v, want int64(7)", v)
		}
	})

	t.Run("with schema applies type, rename and drops unlisted columns", func(t *testing.T) {
		var schema orderedmap.OrderedMap[string, InputPluginSqlConfigSchemaColumn]
		rename := "listing_id"
		schema.Set("uuid", InputPluginSqlConfigSchemaColumn{Type: "string", Rename: &rename})
		schema.Set("n", InputPluginSqlConfigSchemaColumn{Type: "int"})

		records := extractAllRecords(t, newSqlSerializer(schema, true), columns, rows)
		if len(records) != 1 {
			t.Fatalf("got %d records, want 1", len(records))
		}
		if v, _ := records[0].Get("listing_id"); v != "db806676-aaaa" {
			t.Errorf("listing_id = %#v, want string", v)
		}
		if v, _ := records[0].Get("n"); v != int64(7) {
			t.Errorf("n = %#v, want int64(7)", v)
		}
		if _, ok := records[0].Get("uuid"); ok {
			t.Errorf("uuid should have been renamed")
		}
		if _, ok := records[0].Get("id"); ok {
			t.Errorf("id is not in schema and should not be emitted")
		}
	})
}
