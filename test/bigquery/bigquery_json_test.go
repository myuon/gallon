package bigquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myuon/gallon/cmd"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/iterator"
)

type UserWithAddressJSON struct {
	ID        string `json:"id" bigquery:"id"`
	Name      string `json:"name" bigquery:"name"`
	Address   string `json:"address" bigquery:"address"` // JSON string
	CreatedAt int64  `json:"created_at" bigquery:"created_at"`
}

func Test_output_bigquery_with_json_string(t *testing.T) {
	configYml := fmt.Sprintf(`
in:
  type: random
  schema:
    id:
      type: uuid
    name:
      type: name
    address:
      type: record
      fields:
        street:
          type: string
        city:
          type: string
        country:
          type: string
    created_at:
      type: unixtime
out:
  type: bigquery
  endpoint: %v
  projectId: test
  datasetId: dataset1
  tableId: user_with_address_json
  schema:
    id:
      type: string
    name:
      type: string
    address:
      type: string
    created_at:
      type: integer
`, endpoint)

	if err := cmd.RunGallon([]byte(configYml)); err != nil {
		t.Errorf("Could not run command: %s", err)
	}

	// テーブルが作成されるのを待つ
	time.Sleep(2 * time.Second)

	it := client.Dataset("dataset1").Table("user_with_address_json").Read(context.Background())

	count := 0
	recordSamples := []UserWithAddressJSON{}

	for {
		var v UserWithAddressJSON
		err := it.Next(&v)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			t.Errorf("Could not iterate: %s", err)
			break
		}

		count++
		if rand.Float32() < 0.1 {
			recordSamples = append(recordSamples, v)
		}
	}

	assert.Equal(t, 100, count)

	for _, record := range recordSamples {
		_, err := uuid.Parse(record.ID)
		assert.Nil(t, err)

		assert.NotEqual(t, "", record.Name)
		assert.NotEqual(t, "", record.Address)
		assert.NotEqual(t, int64(0), record.CreatedAt)

		// JSON文字列が正しくパースできることを確認
		var address Address
		err = json.Unmarshal([]byte(record.Address), &address)
		assert.Nil(t, err)
		assert.NotEqual(t, "", address.Street)
		assert.NotEqual(t, "", address.City)
		assert.NotEqual(t, "", address.Country)
	}
}
