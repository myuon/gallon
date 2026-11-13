package gallon

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	orderedmap "github.com/wk8/go-ordered-map/v2"
	"gopkg.in/yaml.v3"
)

// parseTimezone parses a timezone string which can be:
// - IANA timezone identifier like "Asia/Tokyo", "UTC"
// - Numeric offset like "+09:00", "+9", "-05:00"
func parseTimezone(tz string) (*time.Location, error) {
	// Try to load as IANA timezone first
	loc, err := time.LoadLocation(tz)
	if err == nil {
		return loc, nil
	}

	// Try to parse as numeric offset
	// Handle formats like "+9", "+09:00", "-05:00"
	var hours, minutes int
	var sign int = 1

	if tz[0] == '+' {
		sign = 1
		tz = tz[1:]
	} else if tz[0] == '-' {
		sign = -1
		tz = tz[1:]
	}

	// Try parsing with colon separator first (e.g., "09:00")
	_, err = fmt.Sscanf(tz, "%d:%d", &hours, &minutes)
	if err != nil {
		// Try parsing as just hours (e.g., "9")
		_, err = fmt.Sscanf(tz, "%d", &hours)
		if err != nil {
			return nil, fmt.Errorf("invalid timezone format: %s", tz)
		}
		minutes = 0
	}

	offset := sign * (hours*3600 + minutes*60)
	return time.FixedZone(fmt.Sprintf("UTC%+d", sign*hours), offset), nil
}

type InputPluginSql struct {
	logger        logr.Logger
	client        *sql.DB
	tableName     string
	rawQuery      string
	driver        string
	pageSize      int
	paginationKey string
	serialize     func(orderedmap.OrderedMap[string, any]) (GallonRecord, error)
}

func NewInputPluginSql(
	client *sql.DB,
	tableName string,
	rawQuery string,
	driver string,
	pageSize int,
	serialize func(orderedmap.OrderedMap[string, any]) (GallonRecord, error),
) *InputPluginSql {
	return NewInputPluginSqlWithPaginationKey(client, tableName, rawQuery, driver, pageSize, "", serialize)
}

// NewInputPluginSqlWithPaginationKey is the same as NewInputPluginSql, but additionally
// accepts a paginationKey. When paginationKey is non-empty, Extract paginates using
// keyset pagination (`WHERE <paginationKey> > ? ORDER BY <paginationKey> LIMIT ?`)
// instead of the default `LIMIT ? OFFSET ?`, which avoids duplicated/skipped rows
// when the source table is concurrently modified during a long-running extraction
// (see: https://github.com/myuon/gallon/issues/40).
func NewInputPluginSqlWithPaginationKey(
	client *sql.DB,
	tableName string,
	rawQuery string,
	driver string,
	pageSize int,
	paginationKey string,
	serialize func(orderedmap.OrderedMap[string, any]) (GallonRecord, error),
) *InputPluginSql {
	return &InputPluginSql{
		client:        client,
		tableName:     tableName,
		rawQuery:      rawQuery,
		driver:        driver,
		pageSize:      pageSize,
		paginationKey: paginationKey,
		serialize:     serialize,
	}
}

var _ InputPlugin = &InputPluginSql{}

func (p *InputPluginSql) ReplaceLogger(logger logr.Logger) {
	if p.tableName != "" {
		p.logger = logger.WithValues("table", p.tableName)
		return
	}

	if p.rawQuery != "" {
		p.logger = logger.WithValues("mode", "raw_query")
		return
	}

	p.logger = logger
}

func (p *InputPluginSql) Cleanup() error {
	return p.client.Close()
}

func (p *InputPluginSql) sourceName() string {
	if p.rawQuery != "" {
		return "raw_query"
	}
	return p.tableName
}

// sqlBindPlaceholder returns the driver-specific bind parameter placeholder
// for the nth (1-indexed) argument of a query.
func sqlBindPlaceholder(driver string, n int) (string, error) {
	switch driver {
	case "mysql":
		return "?", nil
	case "postgres":
		return fmt.Sprintf("$%d", n), nil
	default:
		return "", fmt.Errorf("unsupported driver: %v", driver)
	}
}

// sqlFromClause returns the `FROM` target for the paged query: the table name
// as-is in table mode, or the user's raw query wrapped as a subquery in raw
// query mode (so that both modes can be paginated the same way).
func sqlFromClause(tableName, rawQuery string) string {
	if rawQuery != "" {
		return fmt.Sprintf("(%s) AS __gallon_raw_query", rawQuery)
	}

	return tableName
}

// buildLegacyPagedQuery builds the offset-based paginated query. This is the
// default, backward-compatible pagination strategy used when no
// paginationKey is configured. It has no deterministic ORDER BY, so page
// boundaries can shift if the source table is modified concurrently
// (see: https://github.com/myuon/gallon/issues/40).
func buildLegacyPagedQuery(driver, tableName, rawQuery string, pageSize int) (string, error) {
	offsetPlaceholder, err := sqlBindPlaceholder(driver, 1)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(
		"SELECT * FROM %s LIMIT %d OFFSET %s",
		sqlFromClause(tableName, rawQuery),
		pageSize,
		offsetPlaceholder,
	), nil
}

// buildKeysetPagedQueries builds the two queries used for keyset pagination:
//   - firstPageQuery has no lower bound and is used to fetch the first page.
//   - nextPageQuery filters rows strictly after the last seen paginationKey
//     value, so a single run extracts every row at most once (no duplicates)
//     and does not skip rows that are inserted during the run, regardless of
//     concurrent modifications to the source table.
//
// Both queries order by paginationKey ascending, which is required for
// keyset pagination to make deterministic forward progress. paginationKey
// must therefore be a unique, sortable column (e.g. a primary key).
func buildKeysetPagedQueries(driver, tableName, rawQuery, paginationKey string) (firstPageQuery string, nextPageQuery string, err error) {
	firstLimitPlaceholder, err := sqlBindPlaceholder(driver, 1)
	if err != nil {
		return "", "", err
	}
	keyPlaceholder, err := sqlBindPlaceholder(driver, 1)
	if err != nil {
		return "", "", err
	}
	nextLimitPlaceholder, err := sqlBindPlaceholder(driver, 2)
	if err != nil {
		return "", "", err
	}

	from := sqlFromClause(tableName, rawQuery)

	firstPageQuery = fmt.Sprintf(
		"SELECT * FROM %s ORDER BY %s LIMIT %s",
		from,
		paginationKey,
		firstLimitPlaceholder,
	)
	nextPageQuery = fmt.Sprintf(
		"SELECT * FROM %s WHERE %s > %s ORDER BY %s LIMIT %s",
		from,
		paginationKey,
		keyPlaceholder,
		paginationKey,
		nextLimitPlaceholder,
	)

	return firstPageQuery, nextPageQuery, nil
}

func (p *InputPluginSql) Extract(
	ctx context.Context,
	messages chan []GallonRecord,
	errs chan error,
) error {
	// Keyset pagination is opt-in via paginationKey. It is the recommended
	// setting for tables that receive concurrent writes during extraction,
	// since it does not rely on OFFSET (see: https://github.com/myuon/gallon/issues/40).
	// When paginationKey is not configured, fall back to the legacy
	// OFFSET-based pagination for backward compatibility.
	if p.paginationKey != "" {
		return p.extractWithKeysetPagination(ctx, messages, errs)
	}

	return p.extractWithOffsetPagination(ctx, messages, errs)
}

func (p *InputPluginSql) extractWithOffsetPagination(
	ctx context.Context,
	messages chan []GallonRecord,
	errs chan error,
) error {
	hasNext := true
	page := 0

	extractedTotal := 0

	pagedQueryStatement, err := buildLegacyPagedQuery(p.driver, p.tableName, p.rawQuery, p.pageSize)
	if err != nil {
		return err
	}

	query, err := p.client.Prepare(pagedQueryStatement)
	if err != nil {
		return err
	}
	defer func() {
		if err := query.Close(); err != nil {
			errs <- fmt.Errorf("failed to close sql query: %v (error: %v)", p.sourceName(), err)
		}
	}()

loop:
	for hasNext {
		select {
		case <-ctx.Done():
			break loop
		default:
			rows, err := query.Query(page * p.pageSize)
			if err != nil {
				return err
			}
			if err := rows.Err(); err != nil {
				return err
			}

			cols, err := rows.Columns()
			if err != nil {
				return err
			}

			msgs := []GallonRecord{}
			for rows.Next() {
				columns := make([]any, len(cols))
				columnPointers := make([]any, len(cols))
				for i := range columns {
					columnPointers[i] = &columns[i]
				}

				if err := rows.Scan(columnPointers...); err != nil {
					errs <- fmt.Errorf("failed to scan sql table: %v (error: %v)", p.sourceName(), err)
					continue
				}

				record := *orderedmap.New[string, any]()
				for i, colName := range cols {
					val := columnPointers[i].(*any)
					record.Set(colName, *val)
				}

				r, err := p.serialize(record)
				if err != nil {
					errs <- fmt.Errorf("failed to serialize sql table: %v (error: %v)", p.sourceName(), err)
					continue
				}

				msgs = append(msgs, r)
			}

			if err := rows.Close(); err != nil {
				errs <- fmt.Errorf("failed to close sql rows: %v (error: %v)", p.sourceName(), err)
			}

			if len(msgs) > 0 {
				messages <- msgs
				extractedTotal += len(msgs)

				p.logger.Info(fmt.Sprintf("extracted %v records", extractedTotal))
			} else {
				hasNext = false
			}

			page++
		}
	}
	if extractedTotal == 0 {
		p.logger.Info(fmt.Sprintf("no records found in %v", p.sourceName()))
	}

	return nil
}

// extractWithKeysetPagination extracts rows using keyset pagination
// (`WHERE paginationKey > ? ORDER BY paginationKey LIMIT ?`). Unlike
// OFFSET-based pagination, each page's lower bound is anchored to the last
// row actually read, so rows inserted/deleted elsewhere in the table during
// the run cannot shift a row across a page boundary: every row is extracted
// at most once, and rows present at query time are not skipped.
func (p *InputPluginSql) extractWithKeysetPagination(
	ctx context.Context,
	messages chan []GallonRecord,
	errs chan error,
) error {
	firstPageQueryStatement, nextPageQueryStatement, err := buildKeysetPagedQueries(p.driver, p.tableName, p.rawQuery, p.paginationKey)
	if err != nil {
		return err
	}

	firstPageQuery, err := p.client.Prepare(firstPageQueryStatement)
	if err != nil {
		return err
	}
	defer func() {
		if err := firstPageQuery.Close(); err != nil {
			errs <- fmt.Errorf("failed to close sql query: %v (error: %v)", p.sourceName(), err)
		}
	}()

	nextPageQuery, err := p.client.Prepare(nextPageQueryStatement)
	if err != nil {
		return err
	}
	defer func() {
		if err := nextPageQuery.Close(); err != nil {
			errs <- fmt.Errorf("failed to close sql query: %v (error: %v)", p.sourceName(), err)
		}
	}()

	hasNext := true
	isFirstPage := true
	extractedTotal := 0
	var lastKey any

loop:
	for hasNext {
		select {
		case <-ctx.Done():
			break loop
		default:
			var rows *sql.Rows
			var err error
			if isFirstPage {
				rows, err = firstPageQuery.Query(p.pageSize)
			} else {
				rows, err = nextPageQuery.Query(lastKey, p.pageSize)
			}
			if err != nil {
				return err
			}
			if err := rows.Err(); err != nil {
				return err
			}

			cols, err := rows.Columns()
			if err != nil {
				return err
			}

			keyIdx := -1
			for i, col := range cols {
				if col == p.paginationKey {
					keyIdx = i
					break
				}
			}
			if keyIdx == -1 {
				rows.Close()
				return fmt.Errorf("paginationKey %q was not found in the result columns of %v", p.paginationKey, p.sourceName())
			}

			msgs := []GallonRecord{}
			for rows.Next() {
				columns := make([]any, len(cols))
				columnPointers := make([]any, len(cols))
				for i := range columns {
					columnPointers[i] = &columns[i]
				}

				if err := rows.Scan(columnPointers...); err != nil {
					errs <- fmt.Errorf("failed to scan sql table: %v (error: %v)", p.sourceName(), err)
					continue
				}

				record := *orderedmap.New[string, any]()
				for i, colName := range cols {
					record.Set(colName, columns[i])
				}

				r, err := p.serialize(record)
				if err != nil {
					errs <- fmt.Errorf("failed to serialize sql table: %v (error: %v)", p.sourceName(), err)
					continue
				}

				msgs = append(msgs, r)

				// Track the last seen pagination key (rows are ordered
				// ascending by paginationKey) so the next page can resume
				// strictly after it.
				if keyVal := columns[keyIdx]; keyVal != nil {
					if b, ok := keyVal.([]byte); ok {
						// mysql commonly returns string/varchar columns as
						// []byte; normalize to string for the next bind
						// parameter.
						lastKey = string(b)
					} else {
						lastKey = keyVal
					}
				}
			}

			if err := rows.Close(); err != nil {
				errs <- fmt.Errorf("failed to close sql rows: %v (error: %v)", p.sourceName(), err)
			}

			if len(msgs) > 0 {
				messages <- msgs
				extractedTotal += len(msgs)

				p.logger.Info(fmt.Sprintf("extracted %v records", extractedTotal))
			} else {
				hasNext = false
			}

			isFirstPage = false
		}
	}
	if extractedTotal == 0 {
		p.logger.Info(fmt.Sprintf("no records found in %v", p.sourceName()))
	}

	return nil
}

func (p *InputPluginSql) CloseConnection() error {
	return p.client.Close()
}

type InputPluginSqlConfig struct {
	Table         string                                                          `yaml:"table"`
	Query         string                                                          `yaml:"query"`
	DatabaseUrl   string                                                          `yaml:"database_url"`
	Driver        string                                                          `yaml:"driver"`
	PageSize      int                                                             `yaml:"pageSize"`
	PaginationKey string                                                          `yaml:"paginationKey"`
	Schema        orderedmap.OrderedMap[string, InputPluginSqlConfigSchemaColumn] `yaml:"schema"`
}

type InputPluginSqlConfigSchemaColumn struct {
	Type            string                                      `yaml:"type"`
	DefaultTimezone *string                                     `yaml:"default_timezone"`
	Transforms      []InputPluginSqlConfigSchemaColumnTransform `yaml:"transforms"`
	Rename          *string                                     `yaml:"rename"`
}

type InputPluginSqlConfigSchemaColumnTransform struct {
	// Operation: type conversion
	Type   string  `yaml:"type"`
	Format *string `yaml:"format"`
	As     *string `yaml:"as"`
	Tz     *string `yaml:"tz"`
}

func (c InputPluginSqlConfigSchemaColumnTransform) Transform(sourceType string, value any) (any, error) {
	// If value is nil, return nil immediately without transformation
	if value == nil {
		return nil, nil
	}

	switch sourceType {
	case "time":
		v, ok := value.(time.Time)
		if !ok {
			return nil, fmt.Errorf("value is not time: %v", value)
		}

		// Handle timezone conversion
		if c.Tz != nil {
			loc, err := parseTimezone(*c.Tz)
			if err != nil {
				return nil, fmt.Errorf("failed to load timezone: %v", err)
			}
			v = v.In(loc)
			return v, nil
		}

		if c.Type == "string" {
			if c.Format != nil {
				return v.Format(*c.Format), nil
			}

			return v.Format(time.RFC3339), nil
		}
	case "int":
		v, ok := value.(int64)
		if !ok {
			return nil, fmt.Errorf("value is not int: %v", value)
		}

		if c.Type == "time" {
			if c.As == nil || *c.As == "unix" {
				return time.Unix(v, 0), nil
			}
		}
	}

	return nil, fmt.Errorf("unsupported transform: %v -> %v", sourceType, c.Type)
}

func (c InputPluginSqlConfigSchemaColumn) getValue(value any) (any, error) {
	// if value is nil, returns nil anyway
	if value == nil {
		return nil, nil
	}

	switch c.Type {
	case "string":
		v, ok := value.(string)
		if ok {
			return v, nil
		}

		// Since mysql driver returns []byte for string, we need to convert it to string
		b, ok := value.([]byte)
		if ok {
			return string(b), nil
		}

		return nil, fmt.Errorf("value is not string: %v", value)
	case "int":
		v, ok := value.(int64)
		if !ok {
			return nil, fmt.Errorf("value is not int: %v", value)
		}

		return v, nil
	case "float":
		v, ok := value.(float64)
		if !ok {
			return nil, fmt.Errorf("value is not float: %v", value)
		}

		return v, nil
	case "decimal":
		// MySQLのdecimal型は[]byteとして返されることがあるため、文字列に変換してからfloat64に変換
		switch v := value.(type) {
		case float64:
			return v, nil
		case []byte:
			str := string(v)
			f, err := strconv.ParseFloat(str, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse decimal: %v", err)
			}
			return f, nil
		default:
			return nil, fmt.Errorf("value is not decimal: %v", value)
		}
	case "bool":
		switch v := value.(type) {
		case bool:
			return v, nil
		case int64:
			// tinyint(1)
			return v != 0, nil
		case []byte:
			if len(v) == 1 {
				// bit(1)
				return v[0] != 0, nil
			}
			return nil, fmt.Errorf("value is not bool: %v", value)
		default:
			return nil, fmt.Errorf("value is not bool: %v", value)
		}
	case "date":
		b, ok := value.([]byte)
		if ok {
			v, err := time.Parse("2006-01-02", string(b))
			if err != nil {
				return nil, fmt.Errorf("failed to parse date: %v", err)
			}

			return v.Format(time.DateOnly), nil
		}

		v, ok := value.(time.Time)
		if !ok {
			return nil, fmt.Errorf("value is not date: %v", value)
		}

		return v.Format(time.DateOnly), nil
	case "time":
		// when parseTime not specified, mysql returns []byte
		b, ok := value.([]byte)
		if ok {
			var v time.Time
			var err error

			// Parse with default timezone if specified
			if c.DefaultTimezone != nil {
				loc, err := parseTimezone(*c.DefaultTimezone)
				if err != nil {
					return nil, fmt.Errorf("failed to load default timezone: %v", err)
				}
				v, err = time.ParseInLocation("2006-01-02 15:04:05", string(b), loc)
				if err != nil {
					return nil, fmt.Errorf("failed to parse time: %v", err)
				}
			} else {
				v, err = time.Parse("2006-01-02 15:04:05", string(b))
				if err != nil {
					return nil, fmt.Errorf("failed to parse time: %v", err)
				}
			}

			return v, nil
		}

		v, ok := value.(time.Time)
		if !ok {
			return nil, fmt.Errorf("value is not time: %v", value)
		}

		return v, nil
	case "json":
		b, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("value is not json: %v", value)
		}

		var result any
		if err := json.Unmarshal(b, &result); err != nil {
			return nil, fmt.Errorf("failed to unmarshal json: %v", err)
		}

		return result, nil
	default:
		return nil, fmt.Errorf("unknown column type: %v", c.Type)
	}
}

func NewInputPluginSqlFromConfig(configYml []byte) (*InputPluginSql, error) {
	var inConfig GallonConfig[InputPluginSqlConfig, any]
	if err := yaml.Unmarshal(configYml, &inConfig); err != nil {
		return nil, err
	}

	dbConfig := inConfig.In
	if dbConfig.PageSize == 0 {
		dbConfig.PageSize = 1000
	}

	db, err := sql.Open(dbConfig.Driver, dbConfig.DatabaseUrl)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}

	serialize := newSqlSerializer(dbConfig.Schema, dbConfig.Query != "")

	return NewInputPluginSqlWithPaginationKey(
		db,
		dbConfig.Table,
		dbConfig.Query,
		dbConfig.Driver,
		dbConfig.PageSize,
		dbConfig.PaginationKey,
		serialize,
	), nil
}

// newSqlSerializer builds the row -> GallonRecord conversion.
//
// When schema has columns, only the columns listed in schema are emitted, each
// converted by getValue -> transforms -> rename (both in table mode and raw
// query mode).
//
// In raw query mode without a schema, every column is emitted as-is, except
// that []byte values (which go-sql-driver/mysql returns for string columns)
// are converted to string so they are not base64-encoded when serialized to
// JSON. Table mode without a schema keeps emitting nothing.
func newSqlSerializer(
	schema orderedmap.OrderedMap[string, InputPluginSqlConfigSchemaColumn],
	rawQuery bool,
) func(orderedmap.OrderedMap[string, any]) (GallonRecord, error) {
	if rawQuery && schema.Len() == 0 {
		return func(item orderedmap.OrderedMap[string, any]) (GallonRecord, error) {
			record := NewGallonRecord()
			for pair := item.Oldest(); pair != nil; pair = pair.Next() {
				if b, ok := pair.Value.([]byte); ok {
					record.Set(pair.Key, string(b))
					continue
				}
				record.Set(pair.Key, pair.Value)
			}
			return record, nil
		}
	}

	return func(item orderedmap.OrderedMap[string, any]) (GallonRecord, error) {
		record := NewGallonRecord()

		for pair := schema.Oldest(); pair != nil; pair = pair.Next() {
			value, ok := item.Get(pair.Key)
			if !ok {
				continue
			}

			v, err := pair.Value.getValue(value)
			if err != nil {
				return GallonRecord{}, errors.Join(err, fmt.Errorf("failed to get value for column: %v", pair.Key))
			}

			sourceType := pair.Value.Type

			for _, transform := range pair.Value.Transforms {
				v, err = transform.Transform(sourceType, v)
				if err != nil {
					return GallonRecord{}, errors.Join(err, fmt.Errorf("failed to transform value for column: %v", pair.Key))
				}

				sourceType = transform.Type
			}

			columnName := pair.Key
			if pair.Value.Rename != nil {
				columnName = *pair.Value.Rename
			}

			record.Set(columnName, v)
		}

		return record, nil
	}
}
