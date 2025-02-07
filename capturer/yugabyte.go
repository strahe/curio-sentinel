package capturer

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/strahe/curio-sentinel/yblogrepl"
	"github.com/yugabyte/pgx/v5/pgconn"
	"github.com/yugabyte/pgx/v5/pgproto3"
)

const (
	defaultLSNType      = "HYBRID_TIME"
	defaultOutputPlugin = "yboutput"
	defaultSlotPrefix   = "curio_sentinel_slot_"
	defaultPublicPrefix = "curio_sentinel_pub_"
)

type YugabyteCapture struct {
	conn   *pgconn.PgConn
	cfg    Config
	logger Logger

	pubCreated  bool
	slotCreated bool
	running     bool

	ctx        context.Context
	cancelFn   context.CancelFunc
	wg         sync.WaitGroup
	events     chan *Event
	appliedLSN yblogrepl.LSN
	mu         sync.Mutex
}

func NewYugabyteCapturer(cfg Config, logger Logger) Capturer {
	if cfg.SlotName == "" {
		cfg.SlotName = defaultSlotPrefix + time.Now().Format("20060102150405")
		cfg.DropSlotOnStop = true
	}
	if cfg.PublicationName == "" {
		cfg.PublicationName = defaultPublicPrefix + time.Now().Format("20060102150405")
		cfg.DropPublicationOnStop = true
	}
	if logger == nil {
		logger = &noopLogger{}
	}
	if cfg.EventBufferSize == 0 {
		cfg.EventBufferSize = 32
	}
	yc := &YugabyteCapture{
		cfg:    cfg,
		events: make(chan *Event, cfg.EventBufferSize),
		logger: logger,
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
		y.logger.Errorf("failed to clean up: %v", err)
	}

	y.wg.Wait()

	y.setRunning(false)
	y.logger.Infof("capturer stopped")

	return nil
}

func (y *YugabyteCapture) Events() <-chan *Event {
	return y.events
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

	conn, err := y.getReplicationConn(ctx)
	if err != nil {
		y.logger.Errorf("failed to get replication connection: %v", err)
		return
	}
	defer conn.Close(context.Background())

	startLSN := y.appliedLSN

	y.logger.Infof("starting replication from LSN %s", startLSN.String())

	err = yblogrepl.StartReplication(ctx, conn, y.cfg.SlotName, startLSN, yblogrepl.StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '1'",
			fmt.Sprintf("publication_names '%s'", y.cfg.PublicationName),
		},
	})

	if err != nil {
		y.logger.Errorf("failed to start replication: %v", err)
		return
	}

	standbyMessageTimeout := time.Second * 5
	standbyTicker := time.NewTicker(time.Second)
	defer standbyTicker.Stop()
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)
	clientXLogPos := startLSN

	processor := NewProcessor(y.events, y.logger, conn)

	for {
		select {
		case <-y.ctx.Done():
			y.logger.Infof("capturer yugabyte exiting: %v", y.ctx.Err())
			return
		case <-standbyTicker.C:
			if time.Now().After(nextStandbyMessageDeadline) {
				y.logger.Debugf("sending Standby status message")
				err = yblogrepl.SendStandbyStatusUpdate(ctx, conn, yblogrepl.StandbyStatusUpdate{
					WALWritePosition: clientXLogPos,
					WALFlushPosition: y.appliedLSN,
					WALApplyPosition: y.appliedLSN,
					ReplyRequested:   true,
				})
				if err != nil {
					y.logger.Errorf("failed to send Standby status message: %v", err)
					continue
				}
				nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
			}
		default:
			receiveCtx, cancel := context.WithTimeout(ctx, time.Second*5)
			rawMsg, err := conn.ReceiveMessage(receiveCtx)
			cancel()
			if err != nil {
				if pgconn.Timeout(err) {
					continue
				}
				y.logger.Errorf("failed to receive message: %v", err)
			}

			switch msg := rawMsg.(type) {
			case *pgproto3.ErrorResponse:
				y.logger.Errorf("error response received: %v", msg.Message)
			case *pgproto3.ReadyForQuery:
				y.logger.Debugf("ready for query: %v", msg.TxStatus)
			case *pgproto3.CopyData:
				switch msg.Data[0] {
				case yblogrepl.PrimaryKeepaliveMessageByteID:
					pkm, err := yblogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
					if err != nil {
						y.logger.Errorf("ParsePrimaryKeepaliveMessage failed: %v", err)
						continue
					}
					y.logger.Debugf("Primary Keepalive Message: %v", pkm)
					if pkm.ServerWALEnd > clientXLogPos {
						clientXLogPos = pkm.ServerWALEnd
					}
					if pkm.ReplyRequested {
						nextStandbyMessageDeadline = time.Time{}
					}

				case yblogrepl.XLogDataByteID:
					xld, err := yblogrepl.ParseXLogData(msg.Data[1:])
					if err != nil {
						y.logger.Errorf("ParseXLogData failed: %v", err)
						continue
					}
					processor.Process(ctx, xld.WALData)

					if xld.WALStart > clientXLogPos {
						clientXLogPos = xld.WALStart
					}
				default:
					y.logger.Debugf("recive unknown copy data")
				}
			default:
				y.logger.Debugf("recive unknown msg type: %T", msg)
			}
		}
	}
}

func (y *YugabyteCapture) Checkpoint(ctx context.Context) (string, error) {
	y.mu.Lock()
	defer y.mu.Unlock()

	if !y.running {
		return "", fmt.Errorf("capture not running")
	}

	slots, err := yblogrepl.ListReplicationSlots(ctx, y.conn)
	if err != nil {
		return "", fmt.Errorf("failed to list replication slots: %w", err)
	}
	for _, slot := range slots {
		if slot.SlotName == y.cfg.SlotName {
			return slot.ConfirmedFlushLSN.String(), nil
		}
	}
	return "", fmt.Errorf("slot not found")
}

// ACK implements Capturer.
func (y *YugabyteCapture) ACK(ctx context.Context, position string) error {
	y.mu.Lock()
	defer y.mu.Unlock()

	lsn, err := yblogrepl.ParseLSN(position)
	if err != nil {
		return fmt.Errorf("failed to parse checkpoint: %w", err)
	}
	if lsn != y.appliedLSN {
		y.appliedLSN = lsn
		y.logger.Infof("ACK: %s", lsn.String())
	}

	return nil
}

func (y *YugabyteCapture) init(ctx context.Context) (err error) {

	defer func() {
		if err != nil {
			y.logger.Errorf("failed to initialize: %v", err)
			if err := y.cleanUp(context.Background()); err != nil {
				y.logger.Errorf("failed to clean up: %v", err)
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

func (y *YugabyteCapture) buildConnConfig() (*pgconn.Config, error) {
	if len(y.cfg.Database.Hosts) == 0 {
		return nil, fmt.Errorf("no database hosts provided")
	}

	connString := "host=" + y.cfg.Database.Hosts[0] + " "
	for k, v := range map[string]string{
		"user":     y.cfg.Database.Username,
		"password": y.cfg.Database.Password,
		"dbname":   y.cfg.Database.Database,
		"port":     fmt.Sprintf("%d", y.cfg.Database.Port),
	} {
		if strings.TrimSpace(v) != "" {
			connString += k + "=" + v + " "
		}
	}
	cfg, err := pgconn.ParseConfig(connString)
	if err != nil {
		return nil, err
	}
	for _, host := range y.cfg.Database.Hosts[1:] {
		cfg.Fallbacks = append(cfg.Fallbacks, &pgconn.FallbackConfig{
			Host: host,
			Port: y.cfg.Database.Port,
		})
	}
	cfg.OnNotice = func(_ *pgconn.PgConn, notice *pgconn.Notice) {
		y.logger.Warnf("Databse Notice: %s", notice.Message)
	}

	cfg.AfterConnect = func(ctx context.Context, conn *pgconn.PgConn) error {
		y.logger.Infof("Databse connection established")
		return nil
	}

	return cfg, nil
}

func (y *YugabyteCapture) createConn(ctx context.Context) error {
	y.mu.Lock()
	defer y.mu.Unlock()

	if y.conn != nil && !y.conn.IsClosed() {
		y.logger.Infof("connection already established")
		return nil
	}

	cfg, err := y.buildConnConfig()
	if err != nil {
		return fmt.Errorf("failed to build connection config: %w", err)
	}

	pgconn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("failed to connect to YugabyteDB: %w", err)
	}

	y.conn = pgconn
	return nil
}

func (y *YugabyteCapture) getReplicationConn(ctx context.Context) (*pgconn.PgConn, error) {
	y.mu.Lock()
	defer y.mu.Unlock()

	cfg, err := y.buildConnConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to build connection config: %w", err)
	}
	cfg.RuntimeParams["replication"] = "database"

	pgconn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to YugabyteDB: %w", err)
	}
	return pgconn, nil
}

func (y *YugabyteCapture) createPublication(ctx context.Context) error {

	exists, err := yblogrepl.CheckPublicationExists(ctx, y.conn, y.cfg.PublicationName)
	if err != nil {
		return fmt.Errorf("failed to check if publication exists: %w", err)
	}

	if exists {
		y.logger.Infof("publication already exists: %s", y.cfg.PublicationName)
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

	y.logger.Infof("Creating publication: %s", y.cfg.PublicationName)
	if err := yblogrepl.CreatePublication(ctx, y.conn, params); err != nil {
		return fmt.Errorf("failed to create publication: %w", err)
	}

	y.logger.Infof("Created publication: %s", y.cfg.PublicationName)
	y.pubCreated = true
	return nil
}

func (y *YugabyteCapture) dropPublication(ctx context.Context) error {

	y.logger.Infof("Dropping publication: %s", y.cfg.PublicationName)

	if err := yblogrepl.DropPublication(ctx, y.conn, y.cfg.PublicationName); err != nil {
		return fmt.Errorf("failed to drop publication: %w", err)
	}

	y.logger.Infof("Dropped publication: %s", y.cfg.PublicationName)
	y.pubCreated = false
	return nil
}

func (y *YugabyteCapture) createReplicationSlot(ctx context.Context) error {
	y.mu.Lock()
	defer y.mu.Unlock()

	exists, err := yblogrepl.CheckReplicationSlotExists(ctx, y.conn, y.cfg.SlotName)
	if err != nil {
		return fmt.Errorf("failed to check if replication slot exists: %w", err)
	}

	if exists {
		slot, err := yblogrepl.GetReplicationSlot(ctx, y.conn, y.cfg.SlotName)
		if err != nil {
			return fmt.Errorf("failed to get replication slot: %w", err)
		}
		y.logger.Infof("Replication slot already exists: %s", y.cfg.SlotName)

		y.appliedLSN = slot.ConfirmedFlushLSN
		return nil
	}

	// create replication slot
	options := yblogrepl.CreateReplicationSlotOptions{
		Temporary:    false, // yugabyte not support temporary slots
		OutputPlugin: defaultOutputPlugin,
		LSNType:      defaultLSNType,
	}

	y.logger.Infof("Creating replication slot: %s", y.cfg.SlotName)
	result, err := yblogrepl.CreateLogicalReplicationSlot(
		ctx,
		y.conn,
		y.cfg.SlotName,
		options,
	)
	if err != nil {
		return fmt.Errorf("failed to create replication slot: %w", err)
	}

	y.logger.Infof("Replication slot created: %s", y.cfg.SlotName)

	y.slotCreated = true
	y.appliedLSN = result.LSN

	return nil
}

func (y *YugabyteCapture) dropReplicationSlot(ctx context.Context) error {

	y.logger.Infof("Dropping replication slot: %s", y.cfg.SlotName)

	tk := time.NewTicker(time.Second * 2)
loop:
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled")
		case <-tk.C:
			if err := yblogrepl.DropReplicationSlot(ctx, y.conn, y.cfg.SlotName); err != nil {
				y.logger.Warnf("failed to drop replication slot: %v", err)
				continue
			}
			break loop
		}
	}

	y.logger.Infof("Replication slot dropped: %s", y.cfg.SlotName)
	y.slotCreated = false
	return nil
}

var _ Capturer = (*YugabyteCapture)(nil)
