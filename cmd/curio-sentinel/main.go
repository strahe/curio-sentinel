package main

import (
	"context"
	"fmt"
	"os"

	logging "github.com/ipfs/go-log"
	"github.com/urfave/cli/v3"
	"github.com/web3tea/curio-sentinel/config"
)

var log = logging.Logger("cmd")

func main() {
	cmd := &cli.Command{
		Name:  "curio-sentinel",
		Usage: "A CLI tool for monitoring database changes in Curio clusters",
		Commands: []*cli.Command{
			runCmd,
		},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "config",
				Aliases: []string{"c"},
				Usage:   "Path to the configuration file",
				Value:   "config.toml",
			},
		},
		Before: func(ctx context.Context, c *cli.Command) (context.Context, error) {
			cfg, err := config.LoadFromFile(c.String("config"))
			if err != nil {
				return nil, fmt.Errorf("failed to load configuration: %w", err)
			}

			switch cfg.LogLevel {
			case "debug":
				logging.SetAllLoggers(logging.LevelDebug)
			case "warn":
				logging.SetAllLoggers(logging.LevelWarn)
			case "error":
				logging.SetAllLoggers(logging.LevelError)
			default:
				logging.SetAllLoggers(logging.LevelInfo)
			}
			return context.WithValue(ctx, "config", cfg), nil
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
