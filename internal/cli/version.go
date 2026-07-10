package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newVersionCmd(version, commit string) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version and build information",
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Printf("gentis %s (commit: %s)\n", version, commit)
		},
	}
}
