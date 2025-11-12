package gallon

import (
	"testing"
	"time"
)

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
