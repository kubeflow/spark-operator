package cmd

import (
	_ "fmt"
	_ "os"

	_ "github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
)

var generateCmd = &cobra.Command{
	Use:   "generate",
	Short: "Generate SparkApplication YAML: WIP",
	Long:  `Generate SparkApplication YAML using templates.`,
	Run: func(cmd *cobra.Command, args []string) {
	},
}