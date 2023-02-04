package main

import (
	"github.com/myuon/gallon/cmd"
	"github.com/spf13/cobra"
	"log"
)

var roomCmd = &cobra.Command{
	Use:   "gallon",
	Short: "Gallon is a tool for data migration",
	Long:  `Gallon is a tool for data migration`,
}

func main() {
	roomCmd.AddCommand(cmd.RunCmd)

	if err := roomCmd.Execute(); err != nil {
		log.Println(err)
	}
}
