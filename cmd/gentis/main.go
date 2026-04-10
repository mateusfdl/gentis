package main

import "github.com/mateusfdl/gentis/internal/cli"

// version and commit are set at build time via ldflags:
//
//	-X main.version=$(VERSION) -X main.commit=$(COMMIT)
var (
	version = "dev"
	commit  = "unknown"
)

func main() {
	cli.Execute(version, commit)
}
