package capture

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/strahe/curio-sentinel/models"
	"github.com/strahe/curio-sentinel/pkg/log"
	"github.com/strahe/curio-sentinel/yblogrepl"
	"github.com/yugabyte/pgx/v5/pgconn"
)

const (
	defaultLSNType      = "HYBRID_TIME"
	defaultOutputPlugin = "yboutput"
	defaultSlotPrefix   = "curio_sentinel_"
	defaultPublicPrefix = "curio_pub_"
)

type YugabyteConfig struct {
	SlotName              string
	PublicationName       string
	Tables                []string
	DropSlotOnStop        bool
	DropPublicationOnStop bool
	// HeartbeatMs           int64
	// EventBufferSize       int
	ConnString string
	// ProtocolVersion       string // "1" 或 "2"
	// EnableStreaming       bool
	// EnableMessages        bool
	// TemporarySlot         bool // not supported yet
}

type YugabyteCapture struct {
	conn        *pgconn.PgConn
	cfg         YugabyteConfig
	pubCreated  bool
	slotCreated bool

	ctx      context.Context
	cancelFn context.CancelFunc
	wg       sync.WaitGroup
	running  bool
	mu       sync.Mutex
}

func NewYugabyteCapture(cfg YugabyteConfig) Capturer {
	if cfg.SlotName == "" {
		cfg.SlotName = defaultSlotPrefix + time.Now().Format("20060102150405")
	}
	if cfg.PublicationName == "" {
		cfg.PublicationName = defaultPublicPrefix + time.Now().Format("20060102150405")
	}
	yc := &YugabyteCapture{
		cfg: cfg,
	}
	yc.ctx, yc.cancelFn = context.WithCancel(context.Background())

	return yc
}

// Start implements Capturer.
func (y *YugabyteCapture) Start() error {
	if y.IsRunning() {
		return fmt.Errorf("capture already running")
	}

	if err := y.init(y.ctx); err != nil {
		return fmt.Errorf("failed to initialize: %w", err)
	}

	y.wg.Add(1)
	go y.startReplication(y.ctx)

	y.setRunning(true)
	return nil
}

// Stop implements Capturer.
func (y *YugabyteCapture) Stop() error {

	if !y.IsRunning() {
		return fmt.Errorf("capture not running")
	}

	y.cancelFn()

	if err := y.cleanUp(context.Background()); err != nil {
		log.Error().Err(err).Msg("failed to clean up")
	}

	y.wg.Wait()

	y.setRunning(false)
	log.Info().Msg("capture stopped")

	return nil
}

func (y *YugabyteCapture) IsRunning() bool {
	y.mu.Lock()
	defer y.mu.Unlock()
	return y.running
}

func (y *YugabyteCapture) setRunning(running bool) {
	y.mu.Lock()
	defer y.mu.Unlock()
	y.running = running
}

func (y *YugabyteCapture) startReplication(ctx context.Context) {
	defer y.wg.Done()
	log.Info().Str("slot", y.cfg.SlotName).Msg("starting replication")

	err := yblogrepl.StartReplication(ctx, y.conn, y.cfg.SlotName, yblogrepl.LSN(0), yblogrepl.StartReplicationOptions{})

	if err != nil {
		log.Error().Err(err).Msg("failed to start replication")
		return
	}

	for {
		select {
		case <-y.ctx.Done():
			log.Info().Str("reason", y.ctx.Err().Error()).Msg("capture yugabyte exiting")
			return
		default:
			<-time.After(time.Second)
			// todo: do nothing
		}
	}
}

// Checkpoint implements Capturer.
func (y *YugabyteCapture) Checkpoint() (string, error) {
	panic("unimplemented")
}

// Events implements Capturer.
func (y *YugabyteCapture) Events() <-chan models.Event {
	panic("unimplemented")
}

// SetCheckpoint implements Capturer.
func (y *YugabyteCapture) SetCheckpoint(checkpoint string) error {
	panic("unimplemented")
}

func (y *YugabyteCapture) init(ctx context.Context) (err error) {

	defer func() {
		if err != nil {
			log.Error().Err(err).Msg("failed to initialize")
			if err := y.cleanUp(context.Background()); err != nil {
				log.Error().Err(err).Msg("failed to clean up")
			}
		}
	}()

	if err := y.createConn(ctx); err != nil {
		return fmt.Errorf("failed to create connection: %w", err)
	}
	if err := y.createPublication(ctx); err != nil {
		return fmt.Errorf("failed to create publication: %w", err)
	}
	if err := y.createReplicationSlot(ctx); err != nil {
		return fmt.Errorf("failed to create replication slot: %w", err)
	}
	return nil
}

func (y *YugabyteCapture) cleanUp(ctx context.Context) error {
	var errs []error

	if y.pubCreated && y.cfg.DropPublicationOnStop {
		if err := y.dropPublication(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if y.slotCreated && y.cfg.DropSlotOnStop {
		if err := y.dropReplicationSlot(ctx); err != nil {
			errs = append(errs, err)
		}
	}

	if err := y.conn.Close(ctx); err != nil {
		errs = append(errs, err)
	}

	if len(errs) == 0 {
		return nil
	}

	return fmt.Errorf("failed to clean up: %v", errs)
}

func (y *YugabyteCapture) createConn(ctx context.Context) error {
	y.mu.Lock()
	defer y.mu.Unlock()

	if y.conn != nil && !y.conn.IsClosed() {
		log.Warn().Msg("connection already established")
		return nil
	}

	connConfig, err := pgconn.ParseConfig(y.cfg.ConnString)
	if err != nil {
		return fmt.Errorf("failed to parse connection string: %w", err)
	}
	connConfig.RuntimeParams["replication"] = "database"

	pgconn, err := pgconn.ConnectConfig(ctx, connConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to YugabyteDB: %w", err)
	}
	y.conn = pgconn
	return nil
}

func (y *YugabyteCapture) createPublication(ctx context.Context) error {
	slog := log.With().Str("publication", y.cfg.PublicationName).Logger()

	exists, err := yblogrepl.CheckPublicationExists(ctx, y.conn, y.cfg.PublicationName)
	if err != nil {
		return fmt.Errorf("failed to check if publication exists: %w", err)
	}

	if exists {
		slog.Info().Msg("Publication already exists")
		return nil
	}

	pubInsert := true
	pubUpdate := true
	pubDelete := true
	pubTruncate := true
	params := yblogrepl.PublicationParams{
		Name:            y.cfg.PublicationName,
		Tables:          y.cfg.Tables,
		AllTables:       len(y.cfg.Tables) == 0, // empty tables means all tables
		PublishInsert:   &pubInsert,
		PublishUpdate:   &pubUpdate,
		PublishDelete:   &pubDelete,
		PublishTruncate: &pubTruncate,
	}

	slog.Info().Msg("Creating publication")
	if err := yblogrepl.CreatePublication(ctx, y.conn, params); err != nil {
		return fmt.Errorf("failed to create publication: %w", err)
	}

	slog.Info().Msg("Created publication")
	y.pubCreated = true
	return nil
}

func (y *YugabyteCapture) dropPublication(ctx context.Context) error {

	slog := log.With().Str("publication", y.cfg.PublicationName).Logger()
	slog.Info().Msg("Dropping publication")

	if err := yblogrepl.DropPublication(ctx, y.conn, y.cfg.PublicationName); err != nil {
		return fmt.Errorf("failed to drop publication: %w", err)
	}

	slog.Info().Msg("Dropped publication")
	y.pubCreated = false
	return nil
}

func (y *YugabyteCapture) createReplicationSlot(ctx context.Context) error {
	slog := log.With().Str("slot", y.cfg.SlotName).Logger()

	exists, err := yblogrepl.CheckReplicationSlotExists(ctx, y.conn, y.cfg.SlotName)
	if err != nil {
		return fmt.Errorf("failed to check if replication slot exists: %w", err)
	}

	if exists {
		slog.Info().Msg("Replication slot already exists")
		return nil // slot already exists
	}

	// create replication slot
	options := yblogrepl.CreateReplicationSlotOptions{
		Temporary:    false, // yugabyte not support temporary slots
		OutputPlugin: defaultOutputPlugin,
		LSNType:      defaultLSNType,
	}

	slog.Info().Msg("Creating replication slot")
	result, err := yblogrepl.CreateLogicalReplicationSlot(
		ctx,
		y.conn,
		y.cfg.SlotName,
		options,
	)
	if err != nil {
		return fmt.Errorf("failed to create replication slot: %w", err)
	}

	slog.Info().Str("lsn", result.LSN.String()).Msg("Replication slot created")
	y.slotCreated = true
	return nil
}

func (y *YugabyteCapture) dropReplicationSlot(ctx context.Context) error {

	slog := log.With().Str("slot", y.cfg.SlotName).Logger()
	slog.Info().Msg("Dropping replication slot")

	tk := time.NewTicker(time.Second * 2)
loop:
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled")
		case <-tk.C:
			if err := yblogrepl.DropReplicationSlot(ctx, y.conn, y.cfg.SlotName); err != nil {
				slog.Info().Msg("Replication slot is still active")
				continue
			}
			break loop
		}
	}

	slog.Info().Msg("Dropped replication slot")
	y.slotCreated = false
	return nil
}

var _ Capturer = (*YugabyteCapture)(nil)
