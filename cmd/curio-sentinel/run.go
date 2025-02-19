package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/strahe/curio-sentinel/capturer"
	"github.com/strahe/curio-sentinel/config"
	"github.com/strahe/curio-sentinel/pkg/log"
	"github.com/strahe/curio-sentinel/processor"
	"github.com/strahe/curio-sentinel/processor/filter"
	"github.com/strahe/curio-sentinel/processor/transformer"
	"github.com/strahe/curio-sentinel/sentinel"
	"github.com/strahe/curio-sentinel/sink"
	"github.com/urfave/cli/v3"
)

var runCmd = &cli.Command{
	Name:  "run",
	Usage: "Run the Curio Sentinel processor",
	Action: func(ctx context.Context, c *cli.Command) error {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		cfg := ctx.Value("config").(*config.Config)

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

		capturer, err := setupCapturer(cfg)
		if err != nil {

		}

		proc, err := setupProcessor(cfg)
		if err != nil {
			log.Fatalf("Failed to setup processor: %v", err)
		}

		s, err := setupSink(cfg)
		if err != nil {
			log.Fatalf("Failed to setup sink: %v", err)
		}

		sentinelSystem := sentinel.NewSentinel(
			capturer,
			proc,
			s,
		)

		if err := sentinelSystem.Start(ctx); err != nil {
			log.Fatalf("Failed to start sentinel: %v", err)
		}

		log.Infof("Sentinel system started successfully")

		sig := <-sigChan
		log.Infof("Received signal: %s", sig.String())

		if err := sentinelSystem.Stop(); err != nil {
			log.Fatalf("Failed to stop sentinel: %v", err)
		}

		log.Infof("Sentinel system stopped successfully")
		return nil
	},
}

func setupCapturer(cfg *config.Config) (capturer.Capturer, error) {
	log.Infof("Setup capturer")
	return capturer.NewYugabyteCapturer(capturer.Config(cfg.Capturer), log.NewLogger("capturer", os.Stdout)), nil
}

func setupProcessor(cfg *config.Config) (processor.Processor, error) {
	log.Infof("Setup processor")

	processor := processor.NewProcessorChain()

	if len(cfg.Processor.Filter.Types) > 0 || len(cfg.Processor.Filter.Schemas) > 0 || len(cfg.Processor.Filter.Tables) > 0 {
		// todo: Add filter
		processor.AddFilter(filter.NewDebugFilter())
	}

	if cfg.Processor.EnableTransformation {
		processor.AddTransformer(transformer.NewDebugTransformer())
	}
	return processor, nil
}

func setupSink(cfg *config.Config) (sink.Sink, error) {
	log.Infof("Setup sink")

	switch cfg.Sink.Type {
	case "console":
		return sink.NewConsoleSink(), nil
	default:
		return nil, fmt.Errorf("unsupported sink type: %s", cfg.Sink.Type)
	}
}
