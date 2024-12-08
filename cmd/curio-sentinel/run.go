package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/strahe/curio-sentinel/capture"
	"github.com/strahe/curio-sentinel/config"
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
			log.Fatalf("Failed to load configuration: %v", err)
		}

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

		// 初始化捕获器，例如YugabyteDB捕获器
		capturer, err := setupCapturer(cfg)
		if err != nil {
			log.Fatalf("Failed to setup capturer: %v", err)
		}

		proc, err := setupProcessor(cfg)
		if err != nil {
			log.Fatalf("Failed to setup processor: %v", err)
		}

		s, err := setupSink(cfg)
		if err != nil {
			log.Fatalf("Failed to setup sink: %v", err)
		}

		// 创建Sentinel实例
		sentinelSystem := sentinel.NewSentinel(
			capturer,
			proc,
			s,
		)

		// 启动Sentinel
		if err := sentinelSystem.Start(ctx); err != nil {
			log.Fatalf("Failed to start sentinel: %v", err)
		}

		log.Println("Sentinel system started successfully")

		// 等待终止信号
		<-sigChan
		log.Println("Received termination signal, shutting down...")

		if err := sentinelSystem.Stop(); err != nil {
			log.Printf("Error during shutdown: %v", err)
			os.Exit(1)
		}

		log.Println("Sentinel system shutdown complete")
		return nil
	},
}

func setupCapturer(cfg *config.Config) (capture.Capturer, error) {
	switch cfg.Capture.Type {
	case "yugabyte":
		ybConfig := capture.YugabyteConfig{
			ConnString:      cfg.Capture.Yugabyte.DSN,
			SlotName:        cfg.Capture.Yugabyte.SlotName,
			PublicationName: cfg.Capture.Yugabyte.PublicationName,
			Tables:          cfg.Capture.Yugabyte.Tables,
			DropSlotOnStop:  cfg.Capture.Yugabyte.DropSlotOnStop,
			EventBufferSize: cfg.Capture.BufferSize,
			ProtocolVersion: cfg.Capture.Yugabyte.ProtocolVersion,
			EnableStreaming: cfg.Capture.Yugabyte.EnableStreaming,
		}

		return capture.NewYugabyte(ybConfig), nil
	default:
		return nil, fmt.Errorf("unsupported capture type: %s", cfg.Capture.Type)
	}
}

func setupProcessor(cfg *config.Config) (processor.Processor, error) {
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
	switch cfg.Sink.Type {
	case "debug":
		return sink.NewDebugSink(), nil

	default:
		return nil, fmt.Errorf("unsupported sink type: %s", cfg.Sink.Type)
	}
}
