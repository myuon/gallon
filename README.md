# gallon

[![Go Reference](https://pkg.go.dev/badge/github.com/myuon/gallon.svg)](https://pkg.go.dev/github.com/myuon/gallon)

A tool to migrate your data from one database to another.

## Installation

```bash
go install github.com/myuon/gallon@latest
```

## How to Use

```bash
gallon run /path/to/config.yml
```

You can also use Go's text/template to generate your config file:

```bash
# Parse the config file as a Go's text/template
gallon run --template /path/to/config.yml

# Parse the config file as a Go's text/template with environment variables injected
gallon run --template-with-env /path/to/config.yml

# Run multiple config files using glob pattern
gallon run --template-with-env "/path/to/*.yml"
```

## Example

```yaml
in:
  type: dynamodb
  region: ap-northeast-1
  table: users
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
  type: bigquery
  location: asia-northeast1
  projectId: default
  datasetId: test
  tableId: users_test
  schema:
    id:
      type: string
    name:
      type: string
    age:
      type: integer
    created_at:
      type: integer
```

See [test](./test) directory for more examples.

## Logging

Gallon uses zap to generate json logs.

```bash
❯ gallon run ~/workspace/gallon/test_config_dynamo_to_bigquery.yml
{"level":"info","ts":1675597490.3000588,"caller":"gallon/gallon.go:52","msg":"start load"}
{"level":"info","ts":1675597490.3001778,"caller":"gallon/gallon.go:40","msg":"start extract"}
{"level":"info","ts":1675597490.7327669,"caller":"gallon/input_dynamodb.go:85","msg":"extracted 100 records"}
{"level":"info","ts":1675597490.757751,"caller":"gallon/input_dynamodb.go:85","msg":"extracted 200 records"}
{"level":"info","ts":1675597490.775156,"caller":"gallon/input_dynamodb.go:85","msg":"extracted 300 records"}
{"level":"info","ts":1675597490.789793,"caller":"gallon/input_dynamodb.go:85","msg":"extracted 400 records"}
{"level":"info","ts":1675597490.804925,"caller":"gallon/input_dynamodb.go:85","msg":"extracted 500 records"}
{"level":"info","ts":1675597490.817198,"caller":"gallon/input_dynamodb.go:85","msg":"extracted 600 records"}
{"level":"info","ts":1675597490.8284988,"caller":"gallon/input_dynamodb.go:85","msg":"extracted 700 records"}
{"level":"info","ts":1675597490.837249,"caller":"gallon/input_dynamodb.go:85","msg":"extracted 800 records"}
{"level":"info","ts":1675597490.846975,"caller":"gallon/input_dynamodb.go:85","msg":"extracted 900 records"}
{"level":"info","ts":1675597490.8592231,"caller":"gallon/input_dynamodb.go:85","msg":"extracted 1000 records"}
{"level":"info","ts":1675597490.865189,"caller":"gallon/output_bigquery.go:116","msg":"loaded 100 records"}
{"level":"info","ts":1675597490.865581,"caller":"gallon/output_bigquery.go:116","msg":"loaded 200 records"}
{"level":"info","ts":1675597490.865809,"caller":"gallon/output_bigquery.go:116","msg":"loaded 300 records"}
{"level":"info","ts":1675597490.884367,"caller":"gallon/output_bigquery.go:116","msg":"loaded 400 records"}
{"level":"info","ts":1675597490.884485,"caller":"gallon/output_bigquery.go:116","msg":"loaded 500 records"}
{"level":"info","ts":1675597490.884664,"caller":"gallon/output_bigquery.go:116","msg":"loaded 600 records"}
{"level":"info","ts":1675597490.884762,"caller":"gallon/output_bigquery.go:116","msg":"loaded 700 records"}
{"level":"info","ts":1675597490.884861,"caller":"gallon/output_bigquery.go:116","msg":"loaded 800 records"}
{"level":"info","ts":1675597490.884956,"caller":"gallon/output_bigquery.go:116","msg":"loaded 900 records"}
{"level":"info","ts":1675597490.885045,"caller":"gallon/output_bigquery.go:116","msg":"loaded 1000 records"}
{"level":"info","ts":1675597490.885066,"caller":"gallon/output_bigquery.go:126","msg":"loading into LOAD_TEMP_users_test_324998a2-6e69-4ea7-81b4-9ec5b9701403"}
{"level":"info","ts":1675597495.681443,"caller":"gallon/output_bigquery.go:149","msg":"loaded into LOAD_TEMP_users_test_324998a2-6e69-4ea7-81b4-9ec5b9701403"}
{"level":"info","ts":1675597497.262851,"caller":"gallon/output_bigquery.go:166","msg":"copied from LOAD_TEMP_users_test_324998a2-6e69-4ea7-81b4-9ec5b9701403 to users_test"}
{"level":"info","ts":1675597497.5881999,"caller":"gallon/output_bigquery.go:64","msg":"temporary table deleted","tableId":"LOAD_TEMP_users_test_324998a2-6e69-4ea7-81b4-9ec5b9701403"}
```

Set `LOGENV` to be `development` for colored, concise logs. (It uses `zap.NewDevelopment`)

```bash
❯ LOGENV=development gallon run ~/workspace/gallon/test_config_dynamo_to_bigquery.yml
2023-02-05T20:49:17.741+0900	INFO	gallon/gallon.go:52	start load
2023-02-05T20:49:17.741+0900	INFO	gallon/gallon.go:40	start extract
2023-02-05T20:49:17.761+0900	INFO	gallon/input_dynamodb.go:85	extracted 100 records
2023-02-05T20:49:17.770+0900	INFO	gallon/input_dynamodb.go:85	extracted 200 records
2023-02-05T20:49:17.780+0900	INFO	gallon/input_dynamodb.go:85	extracted 300 records
2023-02-05T20:49:17.788+0900	INFO	gallon/input_dynamodb.go:85	extracted 400 records
2023-02-05T20:49:17.799+0900	INFO	gallon/input_dynamodb.go:85	extracted 500 records
2023-02-05T20:49:17.809+0900	INFO	gallon/input_dynamodb.go:85	extracted 600 records
2023-02-05T20:49:17.818+0900	INFO	gallon/input_dynamodb.go:85	extracted 700 records
2023-02-05T20:49:17.829+0900	INFO	gallon/input_dynamodb.go:85	extracted 800 records
2023-02-05T20:49:17.842+0900	INFO	gallon/input_dynamodb.go:85	extracted 900 records
2023-02-05T20:49:17.850+0900	INFO	gallon/input_dynamodb.go:85	extracted 1000 records
2023-02-05T20:49:18.330+0900	INFO	gallon/output_bigquery.go:116	loaded 100 records
2023-02-05T20:49:18.330+0900	INFO	gallon/output_bigquery.go:116	loaded 200 records
2023-02-05T20:49:18.330+0900	INFO	gallon/output_bigquery.go:116	loaded 300 records
2023-02-05T20:49:18.330+0900	INFO	gallon/output_bigquery.go:116	loaded 400 records
2023-02-05T20:49:18.330+0900	INFO	gallon/output_bigquery.go:116	loaded 500 records
2023-02-05T20:49:18.331+0900	INFO	gallon/output_bigquery.go:116	loaded 600 records
2023-02-05T20:49:18.331+0900	INFO	gallon/output_bigquery.go:116	loaded 700 records
2023-02-05T20:49:18.331+0900	INFO	gallon/output_bigquery.go:116	loaded 800 records
2023-02-05T20:49:18.331+0900	INFO	gallon/output_bigquery.go:116	loaded 900 records
2023-02-05T20:49:18.331+0900	INFO	gallon/output_bigquery.go:116	loaded 1000 records
2023-02-05T20:49:18.331+0900	INFO	gallon/output_bigquery.go:126	loading into LOAD_TEMP_users_test_584caabe-d2b8-4ec3-bfd5-2ceca1151a70
2023-02-05T20:49:23.152+0900	INFO	gallon/output_bigquery.go:149	loaded into LOAD_TEMP_users_test_584caabe-d2b8-4ec3-bfd5-2ceca1151a70
2023-02-05T20:49:24.282+0900	INFO	gallon/output_bigquery.go:166	copied from LOAD_TEMP_users_test_584caabe-d2b8-4ec3-bfd5-2ceca1151a70 to users_test
2023-02-05T20:49:24.536+0900	INFO	gallon/output_bigquery.go:64	temporary table deleted	{"tableId": "LOAD_TEMP_users_test_584caabe-d2b8-4ec3-bfd5-2ceca1151a70"}
```

## Write a Go program to use Gallon

```go
g := gallon.Gallon{
    Logger: zapr.NewLogger(zap.L()),
    Input:  input, // implement gallon.InputPlugin
    Output: output, // implement gallon.OutputPlugin
}
if err := g.Run(); err != nil {
    return err
}
```

## Plugin Configurations for YAML

### DynamoDB Input Plugin

```yaml
in:
  type: dynamodb
  region: ap-northeast-1
  table: users
  endpoint: "http://localhost:8000"
  schema:
    id:
      type: string
    name:
      type: string
    age:
      type: number
    created_at:
      type: number
    profile:
      type: object
      properties:
        email:
          type: string
        address:
          type: string
    tags:
      type: array
      items:
        type: string
    metadata:
      type: any
```

- region: Your AWS Region
- table: Your DynamoDB Table name
- endpoint: for dynamodb-local (optional)
- schema
  - type: `string`, `number`, `boolean`, `object`, `array`, `any` are supported
  - properties: for `object` type, define nested fields in `properties` properties
  - items: for `array` type, define item type in `items` properties

### SQL(RDB) Input Plugin

```yaml
in:
  type: sql
  driver: mysql
  table: users
  database_url: user:password@tcp(localhost:3306)/dbname
  schema:
    id:
      type: string
    name:
      type: string
    age:
      type: int
    created_at:
      type: int
    birthday:
      type: time
      format: "2006-01-02 15:04:05"
    has_partner:
      type: bool
    balance:
      type: decimal
    preferences:
      type: json
```

- driver: `mysql`, `postgres` are supported
  - For MySQL, [go-sql-driver/mysql](https://github.com/go-sql-driver/mysql) is used.
  - For PostgreSQL, [lib/pq](https://github.com/lib/pq) is used.
- table: Table name
- database_url: Database URL. This will be passed to `sql.Open` with the driver name.
  - For MySQL, it should be `user:password@tcp(host:port)/dbname` (See: [go-sql-driver/mysql](https://github.com/go-sql-driver/mysql#dsn-data-source-name))
- schema
  - type: `string`, `int`, `float`, `decimal`, `time`, `bool`, `json` are supported. NULL are always acceptable.
  - format: for `time` type. Specify time format string in [Go time layout](https://pkg.go.dev/time#Layout). Default is `2006-01-02 15:04:05`. (optional)

### Random Input Plugin

```yaml
in:
  type: random
  pageLimit: 10
  pageSize: 50
  schema:
    id:
      type: uuid
    name:
      type: string
    email:
      type: email
    age:
      type: int
      min: 10
      max: 100
    born_at:
      type: time
      format: rfc3339
    created_at:
      type: unixtime
    profile:
      type: record
      fields:
        bio:
          type: string
        website:
          type: url
```

- pageLimit: Number of pages (optional, default: 10)
- pageSize: Number of records per page (optional, default: 10)
- schema
  - type: `string`, `int`, `float`, `bool`, `name`, `url`, `email`, `uuid`, `time`, `unixtime`, `record` are supported.
  - min, max: for `int` type (optional)
  - format: for `time` type. Specify `rfc3339`, or it returns `YYYY-MM-DD` date string. (optional)
  - fields: for `record` type, define nested fields

### BigQuery Output Plugin

```yaml
out:
  type: bigquery
  location: asia-northeast1
  projectId: default-364617
  datasetId: test
  tableId: users_test
  endpoint: "http://localhost:9050"
  schema:
    id:
      type: string
    name:
      type: string
    age:
      type: integer
    created_at:
      type: integer
    profile:
      type: record
      fields:
        email:
          type: string
        address:
          type: string
    metadata:
      type: any
  deleteTemporaryTable: true
```

- location: Your BigQuery location
- projectId: Your GCP Project ID
- datasetId: Your BigQuery Dataset ID
- tableId: Your BigQuery Table ID
- endpoint: for bigquery-emulator (optional)
- schema
  - type: `string`, `integer`, `float`, `boolean`, `timestamp`, `record`, `any` are supported
    - If non-string value is passed while `string` is specified, the value will be serialized using `json.Marshal`
    - For `record` type, define nested fields in `fields` properties
  - fields: for `record` type, define nested fields
- deleteTemporaryTable: Delete temporary table after copying (optional, default: true)

### File Output Plugin

```yaml
out:
  type: file
  filepath: /tmp/users.csv
  format: csv
```

- filepath: File path
- format: `csv`, `jsonl` are supported
