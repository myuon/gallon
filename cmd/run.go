// cmd package defines the commands for gallon cli.
package cmd

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
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
		configPath := args[0]

		if err := RunGallonWithPath(configPath, RunGallonOptions{
			AsTemplate: withTemplate || withTemplateWithEnv,
			WithEnv:    withTemplateWithEnv,
		}); err != nil {
			zap.S().Error(err)
			return
		}
	},
}

type WithTypeConfig struct {
	Type string `yaml:"type"`
}

// RunGallonWithPath runs a migration with the given config file path.
// You can use glob pattern to run multiple config files.
func RunGallonWithPath(configPath string, opts RunGallonOptions) error {
	files, err := filepath.Glob(configPath)
	if err != nil {
		return err
	}

	for _, file := range files {
		zap.S().Infow("RunGallon", "path", file)

		configFileBody, err := os.ReadFile(file)
		if err != nil {
			zap.S().Errorw("Failed to read config file", "path", file, "error", err)
			continue
		}

		if err := RunGallonWithOptions(configFileBody, opts); err != nil {
			zap.S().Errorw("Failed to run gallon", "path", file, "error", err)
			continue
		}
	}

	return nil
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
