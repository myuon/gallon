package gallon

import (
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func writeTempGzip(t *testing.T, payload []byte) *os.File {
	t.Helper()

	f, err := os.CreateTemp(t.TempDir(), "load-*.jsonl.gz")
	if err != nil {
		t.Fatal(err)
	}

	w := gzip.NewWriter(f)
	if _, err := w.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	opened, err := os.Open(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = opened.Close() })
	return opened
}

func Test_gzipJSONLoadReader_sendsGzipAsIs(t *testing.T) {
	payload := []byte("{\"id\":\"1\"}\n")
	file := writeTempGzip(t, payload)

	reader, err := gzipJSONLoadReader(file, false)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}

	zr, err := gzip.NewReader(bytes.NewReader(got))
	if err != nil {
		t.Fatal(err)
	}
	defer zr.Close()
	decoded, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, payload, decoded)
}

func Test_gzipJSONLoadReader_decompressesForEmulator(t *testing.T) {
	payload := []byte("{\"id\":\"1\"}\n")
	file := writeTempGzip(t, payload)

	reader, err := gzipJSONLoadReader(file, true)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, payload, got)
}

func Test_decompressGzipForLoad(t *testing.T) {
	endpoint := "http://localhost:9050"

	tests := []struct {
		name        string
		format      bqFormat
		compression bqCompression
		endpoint    *string
		want        bool
	}{
		{name: "default uncompressed", format: bqFormatJSON, compression: bqCompressionNone, want: false},
		{name: "gzip to production", format: bqFormatJSON, compression: bqCompressionGzip, want: false},
		{name: "gzip to emulator", format: bqFormatJSON, compression: bqCompressionGzip, endpoint: &endpoint, want: true},
		{name: "uncompressed emulator", format: bqFormatJSON, compression: bqCompressionNone, endpoint: &endpoint, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &OutputPluginBigQuery{format: tt.format, compression: tt.compression, endpoint: tt.endpoint}
			assert.Equal(t, tt.want, p.decompressGzipForLoad())
		})
	}
}

func Test_parseBigQueryLoadOptions(t *testing.T) {
	tests := []struct {
		name        string
		format      string
		compression string
		wantFormat  bqFormat
		wantComp    bqCompression
		wantErr     string
	}{
		{name: "defaults", wantFormat: bqFormatJSON, wantComp: bqCompressionNone},
		{name: "json gzip", format: "JSON", compression: "GZIP", wantFormat: bqFormatJSON, wantComp: bqCompressionGzip},
		{name: "explicit none", format: "json", compression: "none", wantFormat: bqFormatJSON, wantComp: bqCompressionNone},
		{name: "parquet reserved", format: "parquet", wantErr: "unsupported bigquery format"},
		{name: "unknown compression", compression: "snappy", wantErr: "unsupported bigquery compression"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			format, compression, err := parseBigQueryLoadOptions(tt.format, tt.compression)
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, tt.wantFormat, format)
			assert.Equal(t, tt.wantComp, compression)
		})
	}
}
