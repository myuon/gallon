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

type WithTypeConfig struct {
	Type string `yaml:"type"`
}

// RunGallon runs a migration with the given config yaml. See GallonConfig for the schema of the file.
func RunGallon(configYml []byte) error {
	var config gallon.GallonConfig[WithTypeConfig, WithTypeConfig]
	if err := yaml.Unmarshal(configYml, &config); err != nil {
		return err
	}

	input, err := findInputPlugin(config.In.Type, configYml)
	if err != nil {
		return err
	}

	output, err := findOutputPlugin(config.Out.Type, configYml)
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

func findInputPlugin(t string, configYml []byte) (gallon.InputPlugin, error) {
	if t == "dynamodb" {
		return gallon.NewInputPluginDynamoDbFromConfig(configYml)
	} else if t == "sql" {
		return gallon.NewInputPluginSqlFromConfig(configYml)
	} else if t == "random" {
		return gallon.NewInputPluginRandomFromConfig(configYml)
	}

	return nil, errors.New("plugin not found: " + t)
}

func findOutputPlugin(t string, configYml []byte) (gallon.OutputPlugin, error) {
	if t == "bigquery" {
		return gallon.NewOutputPluginBigQueryFromConfig(configYml)
	} else if t == "file" {
		return gallon.NewOutputPluginFileFromConfig(configYml)
	} else if t == "stdout" {
		return gallon.NewOutputPluginStdoutFromConfig(configYml)
	}

	return nil, errors.New("plugin not found: " + t)
}
