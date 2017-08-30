package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
	kafka string
	name  string
)

// serverCmd represents the server command
var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "A brief description of your command",
	Long:  `A longer description`,
	Run: func(cmd *cobra.Command, args []string) {
		if kafka == "" {
			fmt.Println("Missing argument --kafka")
			return
		}

		if name == "" {
			fmt.Println("Missing argument --name")
			return
		}

	},
}

func init() {
	RootCmd.AddCommand(serverCmd)

	serverCmd.Flags().StringVarP(&kafka, "kafka", "k", "", "kafka broker list")
	serverCmd.Flags().StringVarP(&name, "name", "n", "", "conductor cluster name")
}
