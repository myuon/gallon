// cmd package defines the commands for gallon cli.
package cmd

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"text/template"

	"github.com/go-logr/zapr"
	"github.com/myuon/gallon/gallon"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

var withTemplate bool
var withTemplateWithEnv bool

func init() {
	RunCmd.Flags().BoolVar(&withTemplate, "template", false, "parse the config file as a Go's text/template")
	RunCmd.Flags().BoolVar(&withTemplateWithEnv, "template-with-env", false, "parse the config file as a Go's text/template with environment variables injected")
}

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

		opts := RunGallonOptions{
			AsTemplate: withTemplate || withTemplateWithEnv,
			WithEnv:    withTemplateWithEnv,
		}

		if err := RunGallonWithOptions(configFileBody, opts); err != nil {
			zap.S().Error(err)
		}
	},
}

type WithTypeConfig struct {
	Type string `yaml:"type"`
}

type RunGallonOptions struct {
	AsTemplate bool
	WithEnv    bool
}

// RunGallon runs a migration with the given config yaml.
// It is a shortcut for RunGallonWithOptions with default options.
func RunGallon(configYml []byte) error {
	return RunGallonWithOptions(configYml, RunGallonOptions{
		AsTemplate: false,
		WithEnv:    false,
	})
}

// RunGallonWithOptions runs a migration with the given config yaml. See GallonConfig for the schema of the file.
func RunGallonWithOptions(configYml []byte, opts RunGallonOptions) error {
	configBytes := configYml
	if opts.AsTemplate {
		tmpl, err := template.New("gallonConfig").Parse(string(configYml))
		if err != nil {
			return err
		}

		dataMap := map[string]string{}
		if opts.WithEnv {
			for _, e := range os.Environ() {
				parts := strings.SplitN(e, "=", 2)
				if len(parts) == 2 {
					dataMap[parts[0]] = parts[1]
				}
			}
		}

		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, dataMap); err != nil {
			return err
		}

		configBytes = buf.Bytes()
	}

	var config gallon.GallonConfig[WithTypeConfig, WithTypeConfig]

	if err := yaml.Unmarshal(configBytes, &config); err != nil {
		return err
	}

	input, err := findInputPlugin(config.In.Type, configBytes)
	if err != nil {
		return err
	}

	output, err := findOutputPlugin(config.Out.Type, configBytes)
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
