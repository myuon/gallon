package gallon

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"os"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		// The temporary file is always gzip, so every JSON case except an
		// as-is gzip upload has to be decompressed on the way out.
		{name: "default uncompressed", format: bqFormatJSON, compression: bqCompressionNone, want: true},
		{name: "gzip to production", format: bqFormatJSON, compression: bqCompressionGzip, want: false},
		{name: "gzip to emulator", format: bqFormatJSON, compression: bqCompressionGzip, endpoint: &endpoint, want: true},
		{name: "uncompressed emulator", format: bqFormatJSON, compression: bqCompressionNone, endpoint: &endpoint, want: true},
		{name: "parquet", format: bqFormatParquet, compression: bqCompressionNone, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &OutputPluginBigQuery{format: tt.format, compression: tt.compression, endpoint: tt.endpoint}
			assert.Equal(t, tt.want, p.decompressGzipForLoad())
		})
	}
}

func Test_uploadGzipJSON(t *testing.T) {
	endpoint := "http://localhost:9050"

	tests := []struct {
		name        string
		format      bqFormat
		compression bqCompression
		endpoint    *string
		want        bool
	}{
		{name: "default uncompressed", format: bqFormatJSON, compression: bqCompressionNone, want: false},
		{name: "gzip to production", format: bqFormatJSON, compression: bqCompressionGzip, want: true},
		{name: "gzip to emulator", format: bqFormatJSON, compression: bqCompressionGzip, endpoint: &endpoint, want: false},
		{name: "parquet", format: bqFormatParquet, compression: bqCompressionNone, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &OutputPluginBigQuery{format: tt.format, compression: tt.compression, endpoint: tt.endpoint}
			assert.Equal(t, tt.want, p.uploadGzipJSON())
		})
	}
}

// The temporary file has to stay gzipped on disk whatever compression says:
// an uncompressed dump of a large table fills the disk and kills the task.
func Test_writeGzipJSONLoadFile_writesGzipWhateverTheCompression(t *testing.T) {
	for _, compression := range []bqCompression{bqCompressionNone, bqCompressionGzip} {
		t.Run(string(compression), func(t *testing.T) {
			schema := bigquery.Schema{
				{Name: "id", Type: bigquery.StringFieldType},
				{Name: "age", Type: bigquery.IntegerFieldType},
			}
			p := &OutputPluginBigQuery{
				logger:      logr.Discard(),
				schema:      schema,
				format:      bqFormatJSON,
				compression: compression,
				deserialize: func(r GallonRecord) ([]bigquery.Value, error) {
					id, _ := r.Get("id")
					age, _ := r.Get("age")
					return []bigquery.Value{id, age}, nil
				},
			}

			file, err := os.CreateTemp(t.TempDir(), "load-*.jsonl.gz")
			require.NoError(t, err)

			record := NewGallonRecord()
			record.Set("id", "user-1")
			record.Set("age", 30)

			messages := make(chan []GallonRecord, 1)
			messages <- []GallonRecord{record}
			close(messages)
			errs := make(chan error, 1)

			require.NoError(t, p.writeGzipJSONLoadFile(context.Background(), file, messages, errs))
			require.NoError(t, file.Close())
			assert.Empty(t, errs)

			written, err := os.ReadFile(file.Name())
			require.NoError(t, err)
			assert.Equal(t, []byte{0x1f, 0x8b}, written[:2], "temporary file must be gzip")

			zr, err := gzip.NewReader(bytes.NewReader(written))
			require.NoError(t, err)
			defer zr.Close()
			decoded, err := io.ReadAll(zr)
			require.NoError(t, err)
			assert.Equal(t, "{\"id\":\"user-1\",\"age\":30}\n", string(decoded))
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
		{name: "parquet", format: "parquet", wantFormat: bqFormatParquet, wantComp: bqCompressionNone},
		{name: "parquet gzip unsupported", format: "parquet", compression: "gzip", wantErr: "only supported for json format"},
		{name: "unknown format", format: "avro", wantErr: "unsupported bigquery format"},
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
