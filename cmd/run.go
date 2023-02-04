package cmd

import (
	"errors"
	"github.com/myuon/gallon/gallon"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
	"log"
	"os"
)

var RunCmd = &cobra.Command{
	Use:   "run",
	Short: "Run a migration",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		configFile := args[0]
		if err := run(configFile); err != nil {
			log.Fatal(err)
		}
	},
}

type GallonConfig struct {
	In  map[string]interface{} `yaml:"in"`
	Out map[string]interface{} `yaml:"out"`
}

type GallonConfigType struct {
	Type string `yaml:"type"`
}

func getType(config map[string]interface{}) (string, error) {
	t, ok := config["type"]
	if !ok {
		return "", errors.New("type not found")
	}

	tStr, ok := t.(string)
	if !ok {
		return "", errors.New("type is not string")
	}

	return tStr, nil
}

func getTypeAndYml(config map[string]interface{}) (string, []byte, error) {
	t, err := getType(config)
	if err != nil {
		return "", nil, err
	}

	yml, err := yaml.Marshal(config)
	if err != nil {
		return "", nil, err
	}

	return t, yml, nil
}

func run(configFile string) error {
	configFileBody, err := os.ReadFile(configFile)
	if err != nil {
		return err
	}

	var config GallonConfig
	if err := yaml.Unmarshal(configFileBody, &config); err != nil {
		return err
	}

	input, err := findInputPlugin(config)
	if err != nil {
		return err
	}

	output, err := findOutputPlugin(config)
	if err != nil {
		return err
	}

	g := gallon.Gallon{
		Input:  input,
		Output: output,
	}
	if err := g.Run(); err != nil {
		return err
	}

	return nil
}

func findInputPlugin(config GallonConfig) (gallon.InputPlugin, error) {
	t, yml, err := getTypeAndYml(config.In)
	if err != nil {
		return nil, err
	}

	if t == "dynamodb" {
		return gallon.NewInputPluginDynamoDbFromConfig(yml)
	}

	return nil, errors.New("plugin not found: " + t)
}

func findOutputPlugin(config GallonConfig) (gallon.OutputPlugin, error) {
	t, yml, err := getTypeAndYml(config.Out)
	if err != nil {
		return nil, err
	}

	if t == "bigquery" {
		return gallon.NewOutputPluginBigQueryFromConfig(yml)
	}

	return nil, errors.New("plugin not found: " + t)
}
