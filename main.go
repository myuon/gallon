package main

import (
	"github.com/myuon/gallon/cmd"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var roomCmd = &cobra.Command{
	Use:   "gallon",
	Short: "Gallon is a tool for data migration",
	Long:  `Gallon is a tool for data migration`,
}

func main() {
	config := zap.NewDevelopmentConfig()
	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	zapLog := zap.Must(config.Build())
	defer zapLog.Sync()
	zap.ReplaceGlobals(zapLog)

	roomCmd.AddCommand(cmd.RunCmd)

	if err := roomCmd.Execute(); err != nil {
		zap.S().Error(err)
	}
}
