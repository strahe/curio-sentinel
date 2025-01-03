package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/strahe/curio-sentinel/capture"
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
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "config",
			Aliases: []string{"c"},
			Usage:   "Path to the configuration file",
			Value:   "config.toml",
		},
	},
	Action: func(ctx context.Context, c *cli.Command) error {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		cfg, err := config.LoadFromFile(c.String("config"))
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to load configuration")
		}

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

		// 初始化捕获器，例如YugabyteDB捕获器
		capturer, err := setupCapturer(cfg)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to setup capturer")
		}

		proc, err := setupProcessor(cfg)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to setup processor")
		}

		s, err := setupSink(cfg)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to setup sink")
		}

		// 创建Sentinel实例
		sentinelSystem := sentinel.NewSentinel(
			capturer,
			proc,
			s,
		)

		// 启动Sentinel
		if err := sentinelSystem.Start(ctx); err != nil {
			log.Fatal().Err(err).Msg("Failed to start sentinel")
		}

		log.Info().Msg("Sentinel system started successfully")

		// 等待终止信号
		sig := <-sigChan
		log.Info().Str("signal", sig.String()).Msg("Received signal")

		if err := sentinelSystem.Stop(); err != nil {
			log.Info().Err(err).Msg("Failed to stop sentinel")
			os.Exit(1)
		}

		log.Info().Msg("Sentinel")
		return nil
	},
}

func setupCapturer(cfg *config.Config) (capture.Capturer, error) {
	log.Info().Msgf("Setup capture")
	return capture.NewYugabyteCapture(cfg.Capture), nil
}

func setupProcessor(cfg *config.Config) (processor.Processor, error) {
	log.Info().Msgf("Setup processor")

	processor := processor.NewProcessorChain()

	// 添加过滤处理器
	if len(cfg.Processor.Filter.Types) > 0 || len(cfg.Processor.Filter.Schemas) > 0 || len(cfg.Processor.Filter.Tables) > 0 {
		// todo: 添加过滤处理器
		processor.AddFilter(filter.NewDebugFilter())
	}

	// 添加转换处理器
	if cfg.Processor.EnableTransformation {
		processor.AddTransformer(transformer.NewDebugTransformer())
	}
	return processor, nil
}

func setupSink(cfg *config.Config) (sink.Sink, error) {
	log.Info().Msgf("Setup sink")

	switch cfg.Sink.Type {
	case "stdout":
		return sink.NewStdoutSink(), nil

	default:
		return nil, fmt.Errorf("unsupported sink type: %s", cfg.Sink.Type)
	}
}
