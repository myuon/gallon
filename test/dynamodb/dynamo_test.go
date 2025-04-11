package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/myuon/gallon/cmd"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"go.uber.org/zap"
)

func init() {
	zapLog := zap.Must(zap.NewDevelopment())
	defer zapLog.Sync()
	zap.ReplaceGlobals(zapLog)
}

type UserTable struct {
	ID        string  `json:"id" fake:"{uuid}"`
	Name      string  `json:"name" fake:"{firstname}"`
	Age       int     `json:"age" fake:"{number:1,100}"`
	CreatedAt int64   `json:"createdAt" fake:"{number:949720320,1896491520}"`
	Address   Address `json:"address"`
	Skills    []Skill `json:"skills"`
}

type Address struct {
	Street  string `json:"street" fake:"{street}"`
	City    string `json:"city" fake:"{city}"`
	Country string `json:"country" fake:"{country}"`
}

type Skill struct {
	Name     string `json:"name" fake:"{word}"`
	Level    int    `json:"level" fake:"{number:1,5}"`
	Category string `json:"category" fake:"{word}"`
}

var client *dynamodb.Client
var endpoint string

func NewFakeUserTable() (UserTable, error) {
	v := UserTable{}
	if err := gofakeit.Struct(&v); err != nil {
		return v, err
	}

	return v, nil
}

func Migrate(client *dynamodb.Client) error {
	ctx := context.Background()
	if _, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("users"),
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
		return err
	}

	for i := 0; i < 1000; i++ {
		v, err := NewFakeUserTable()
		if err != nil {
			return err
		}

		// スキルをランダムに1-3個生成
		numSkills := gofakeit.Number(1, 3)
		skills := make([]Skill, numSkills)
		for j := 0; j < numSkills; j++ {
			if err := gofakeit.Struct(&skills[j]); err != nil {
				return err
			}
		}
		v.Skills = skills

		// アドレスを生成
		if err := gofakeit.Struct(&v.Address); err != nil {
			return err
		}

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

		if _, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String("users"),
			Item:      record,
		}); err != nil {
			return err
		}
	}

	log.Printf("Migrated %v rows", 1000)

	return nil
}

func TestMain(m *testing.M) {
	var exitCode int
	defer func() {
		os.Exit(exitCode)
	}()

	pool, err := dockertest.NewPool("")
	pool.MaxWait = time.Minute * 2
	if err != nil {
		log.Fatalf("Could not construct pool: %s", err)
	}

	err = pool.Client.Ping()
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}

	resource, err := pool.RunWithOptions(
		&dockertest.RunOptions{
			Repository: "amazon/dynamodb-local",
			Tag:        "latest",
			Cmd:        []string{"-jar", "DynamoDBLocal.jar", "-sharedDb", "-inMemory"},
		},
		func(config *docker.HostConfig) {
			config.AutoRemove = true
			config.RestartPolicy = docker.RestartPolicy{
				Name: "no",
			}
		},
	)
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}
	if err := resource.Expire(2 * 60); err != nil {
		log.Fatalf("Could not set expiration: %s", err)
	}

	defer func() {
		if err := pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}

		log.Println("Purged resource")
	}()

	port := resource.GetPort("8000/tcp")
	endpoint = fmt.Sprintf("http://localhost:%v", port)

	cfg := aws.Config{}
	cfg.EndpointResolverWithOptions = aws.EndpointResolverWithOptionsFunc(
		func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{URL: endpoint}, nil
		})
	cfg.Credentials = credentials.StaticCredentialsProvider{
		Value: aws.Credentials{
			AccessKeyID: "dummy", SecretAccessKey: "dummy", SessionToken: "dummy",
			Source: "Hard-coded credentials; values are irrelevant for local DynamoDB",
		},
	}
	client = dynamodb.NewFromConfig(cfg)

	if err := pool.Retry(func() error {
		log.Println("Trying to connect to database...")

		_, err := client.ListTables(context.Background(), &dynamodb.ListTablesInput{})
		return err
	}); err != nil {
		log.Fatalf("Could not connect to docker: %v", err)
	}

	if err := Migrate(client); err != nil {
		log.Fatalf("Could not migrate: %v", err)
	}

	exitCode = m.Run()
}

func Test_dynamodb_to_file(t *testing.T) {
	configYml := fmt.Sprintf(`
in:
  type: dynamodb
  table: users
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
  filepath: ./output.jsonl
  format: jsonl
`, endpoint)
	defer func() {
		if err := os.Remove("./output.jsonl"); err != nil {
			t.Errorf("Could not remove output file: %s", err)
		}
	}()

	if err := cmd.RunGallon([]byte(configYml)); err != nil {
		t.Errorf("Could not run command: %s", err)
	}

	jsonl, err := os.ReadFile("./output.jsonl")
	if err != nil {
		t.Errorf("Could not read output file: %s", err)
	}

	lines := strings.Split(string(jsonl), "\n")
	if len(lines) != 1001 { // 1000 records + empty line at the end
		t.Errorf("Expected 1001 lines, got %d", len(lines))
	}

	// ネストされたデータの検証
	for _, line := range lines {
		if line == "" {
			continue
		}

		var data map[string]interface{}
		if err := json.Unmarshal([]byte(line), &data); err != nil {
			t.Errorf("Could not unmarshal JSON: %s", err)
			continue
		}

		// 基本フィールドの検証
		if _, ok := data["id"].(string); !ok {
			t.Errorf("Expected id to be string, got %T", data["id"])
		}
		if _, ok := data["name"].(string); !ok {
			t.Errorf("Expected name to be string, got %T", data["name"])
		}
		if _, ok := data["age"].(string); !ok {
			t.Errorf("Expected age to be string, got %T", data["age"])
		}
		if _, ok := data["created_at"].(string); !ok {
			t.Errorf("Expected created_at to be string, got %T", data["created_at"])
		}

		// アドレスの検証
		address, ok := data["address"].(map[string]interface{})
		if !ok {
			t.Errorf("Expected address to be object, got %T", data["address"])
			continue
		}
		if _, ok := address["street"].(string); !ok {
			t.Errorf("Expected address.street to be string, got %T", address["street"])
		}
		if _, ok := address["city"].(string); !ok {
			t.Errorf("Expected address.city to be string, got %T", address["city"])
		}
		if _, ok := address["country"].(string); !ok {
			t.Errorf("Expected address.country to be string, got %T", address["country"])
		}

		// スキルの検証
		skills, ok := data["skills"].([]interface{})
		if !ok {
			t.Errorf("Expected skills to be array, got %T", data["skills"])
			continue
		}
		if len(skills) < 1 || len(skills) > 3 {
			t.Errorf("Expected skills array length to be between 1 and 3, got %d", len(skills))
		}

		for _, skill := range skills {
			skillMap, ok := skill.(map[string]interface{})
			if !ok {
				t.Errorf("Expected skill to be object, got %T", skill)
				continue
			}
			if _, ok := skillMap["name"].(string); !ok {
				t.Errorf("Expected skill.name to be string, got %T", skillMap["name"])
			}
			if _, ok := skillMap["level"].(string); !ok {
				t.Errorf("Expected skill.level to be string, got %T", skillMap["level"])
			}
			if _, ok := skillMap["category"].(string); !ok {
				t.Errorf("Expected skill.category to be string, got %T", skillMap["category"])
			}
		}
	}
}
