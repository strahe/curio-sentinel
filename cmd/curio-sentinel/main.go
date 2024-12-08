package main

import (
	"context"
	"log"
	"os"

	"github.com/urfave/cli/v3"
)

func main() {
	cmd := &cli.Command{
		Name:  "curio-sentinel",
		Usage: "A CLI tool for monitoring database changes in Curio clusters",
		Commands: []*cli.Command{
			runCmd,
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}
