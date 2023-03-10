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

## Example

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
