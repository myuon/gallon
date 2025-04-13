// cmd package defines the commands for gallon cli.
package cmd

import (
	"context"
	"errors"
	"os"

	"github.com/go-logr/zapr"
	"github.com/myuon/gallon/gallon"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

// RunCmd defines `gallon run` command.
var RunCmd = &cobra.Command{
	Use:   "run",
	Short: "Run a migration",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		configFile := args[0]

		configFileBody, err := os.ReadFile(configFile)
		if err != nil {
			zap.S().Error(err)
		}

		if err := RunGallon(configFileBody); err != nil {
			zap.S().Error(err)
		}
	},
}

// GallonConfig is the schema of gallon config yaml.
// Both `in` and `out` must contain `type` field. Plugins for input/output will be chosen by `type` field
type GallonConfig struct {
	In  map[string]any `yaml:"in"`
	Out map[string]any `yaml:"out"`
}

type GallonConfigType struct {
	Type string `yaml:"type"`
}

func getType(config map[string]any) (string, error) {
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

func getTypeAndYml(config map[string]any) (string, []byte, error) {
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

// RunGallon runs a migration with the given config yaml. See GallonConfig for the schema of the file.
func RunGallon(configYml []byte) error {
	var config GallonConfig
	if err := yaml.Unmarshal(configYml, &config); err != nil {
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
		Logger: zapr.NewLogger(zap.L()),
		Input:  input,
		Output: output,
	}
	if err := g.Run(context.Background()); err != nil {
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
	} else if t == "sql" {
		return gallon.NewInputPluginSqlFromConfig(yml)
	} else if t == "random" {
		return gallon.NewInputPluginRandomFromConfig(yml)
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
	} else if t == "file" {
		return gallon.NewOutputPluginFileFromConfig(yml)
	} else if t == "stdout" {
		return gallon.NewOutputPluginStdoutFromConfig(yml)
	}

	return nil, errors.New("plugin not found: " + t)
}
