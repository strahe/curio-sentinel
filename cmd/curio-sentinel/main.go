package main

import (
	"context"
	"fmt"
	"os"

	"github.com/rs/zerolog"
	"github.com/strahe/curio-sentinel/config"
	"github.com/strahe/curio-sentinel/pkg/log"
	"github.com/urfave/cli/v3"
)

func main() {
	cmd := &cli.Command{
		Name:  "curio-sentinel",
		Usage: "A CLI tool for monitoring database changes in Curio clusters",
		Commands: []*cli.Command{
			runCmd,
		},
		Before: func(ctx context.Context, c *cli.Command) (context.Context, error) {
			cfg, err := config.LoadFromFile(c.String("config"))
			if err != nil {
				return nil, fmt.Errorf("failed to load configuration: %w", err)
			}
			switch cfg.LogLevel {
			case "debug":
				log.SetGlobalLevel(zerolog.DebugLevel)
			case "info":
				log.SetGlobalLevel(zerolog.InfoLevel)
			case "warn":
				log.SetGlobalLevel(zerolog.WarnLevel)
			case "error":
				log.SetGlobalLevel(zerolog.ErrorLevel)
			}
			return context.WithValue(ctx, "config", cfg), nil
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
