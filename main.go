package main

import (
	"github.com/myuon/gallon/cmd"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
)

var roomCmd = &cobra.Command{
	Use:   "gallon",
	Short: "Gallon is a tool for data migration",
	Long:  `Gallon is a tool for data migration`,
}

func chooseLogger(env string) (*zap.Logger, error) {
	if env == "development" {
		config := zap.NewDevelopmentConfig()
		config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		return config.Build()
	} else {
		return zap.NewProduction()
	}
}

func main() {
	logenv := os.Getenv("LOGENV")
	zapLog := zap.Must(chooseLogger(logenv))
	defer zapLog.Sync()
	zap.ReplaceGlobals(zapLog)

	roomCmd.AddCommand(cmd.RunCmd)

	if err := roomCmd.Execute(); err != nil {
		zap.S().Error(err)
	}
}
