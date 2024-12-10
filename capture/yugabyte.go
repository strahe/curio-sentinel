package capture

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/strahe/curio-sentinel/models"
	"github.com/strahe/curio-sentinel/pglogrepl"
	"github.com/yugabyte/pgx/v5/pgconn"
	"github.com/yugabyte/pgx/v5/pgproto3"
	"github.com/yugabyte/pgx/v5/pgtype"
)

// 复制状态常量
const (
	statusIdle     = "idle"
	statusStarting = "starting"
	statusRunning  = "running"
	statusStopping = "stopping"
	statusError    = "error"
)

// 默认配置
const (
	defaultOutputPlugin    = "pgoutput"
	defaultSlotPrefix      = "curio_sentinel_"
	defaultPublicPrefix    = "curio_pub_"
	defaultHeartbeat       = 10 * time.Second
	defaultStatusUpdate    = 5 * time.Second
	defaultEventBuffer     = 10000
	defaultErrorWait       = 5 * time.Second
	defaultReconnectPeriod = 10 * time.Second
)

// 复制协议参数
const (
	protoVersion     = "1"
	publicationNames = "publication_names"
	protoDecodeTxn   = "proto_version '1'"
	binaryEncoding   = "binary '1'"
)

// YugabyteConfig 配置参数
type YugabyteConfig struct {
	SlotName        string
	PublicationName string
	Tables          []string
	DropSlotOnStop  bool
	HeartbeatMs     int64
	EventBufferSize int
	ConnString      string
	ProtocolVersion string // "1" 或 "2"
	EnableStreaming bool
	EnableMessages  bool
	TemporarySlot   bool
}

// Checkpoint 表示复制的检查点
type YugabyteCheckpoint struct {
	LSN         pglogrepl.LSN `json:"lsn"`
	YBStreamID  string        `json:"yb_stream_id,omitempty"`
	YBCommitHT  int64         `json:"yb_commit_ht,omitempty"`
	LastEventID string        `json:"last_event_id,omitempty"`
}

// Yugabyte 实现 Capturer 接口，处理 YugabyteDB 逻辑复制
type Yugabyte struct {
	conn        *pgconn.PgConn
	config      YugabyteConfig
	checkpoint  YugabyteCheckpoint
	events      chan models.Event
	status      string
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	lastError   error
	slotCreated bool
	pubCreated  bool
	mu          sync.RWMutex

	// 用于处理消息
	relations   map[uint32]*pglogrepl.RelationMessage
	relationsV2 map[uint32]*pglogrepl.RelationMessageV2
	typeMap     *pgtype.Map
	inStream    bool
}

// NewYugabyte 创建新的 Yugabyte Capturer
func NewYugabyte(config YugabyteConfig) *Yugabyte {
	if config.SlotName == "" {
		config.SlotName = defaultSlotPrefix + time.Now().Format("20060102150405")
	}
	if config.PublicationName == "" {
		config.PublicationName = defaultPublicPrefix + time.Now().Format("20060102150405")
	}
	if config.HeartbeatMs == 0 {
		config.HeartbeatMs = int64(defaultHeartbeat / time.Millisecond)
	}
	if config.EventBufferSize == 0 {
		config.EventBufferSize = defaultEventBuffer
	}
	if config.ProtocolVersion == "" {
		config.ProtocolVersion = "2" // 默认使用 v2 协议
	}

	return &Yugabyte{
		config:      config,
		events:      make(chan models.Event, config.EventBufferSize),
		status:      statusIdle,
		relations:   make(map[uint32]*pglogrepl.RelationMessage),
		relationsV2: make(map[uint32]*pglogrepl.RelationMessageV2),
		typeMap:     pgtype.NewMap(),
	}
}

// Checkpoint 返回当前检查点
func (y *Yugabyte) Checkpoint() (string, error) {
	y.mu.RLock()
	defer y.mu.RUnlock()

	checkpointData, err := json.Marshal(y.checkpoint)
	if err != nil {
		return "", fmt.Errorf("failed to marshal checkpoint: %w", err)
	}

	return string(checkpointData), nil
}

// SetCheckpoint 设置检查点
func (y *Yugabyte) SetCheckpoint(checkpoint string) error {
	if checkpoint == "" {
		return nil // 没有检查点，使用默认值
	}

	var cp YugabyteCheckpoint
	if err := json.Unmarshal([]byte(checkpoint), &cp); err != nil {
		return fmt.Errorf("invalid checkpoint format: %w", err)
	}

	y.mu.Lock()
	defer y.mu.Unlock()
	y.checkpoint = cp
	return nil
}

// Events 返回事件通道
func (y *Yugabyte) Events() <-chan models.Event {
	return y.events
}

// Start 启动捕获过程
func (y *Yugabyte) Start(ctx context.Context) error {
	if err := y.createConn(ctx); err != nil {
		return fmt.Errorf("failed to create connection: %w", err)
	}

	y.mu.Lock()
	if y.status != statusIdle {
		y.mu.Unlock()
		return fmt.Errorf("capturer already started or in invalid state: %s", y.status)
	}
	y.status = statusStarting
	y.ctx, y.cancel = context.WithCancel(ctx)
	y.mu.Unlock()

	// 确保资源清理
	defer func() {
		if y.status == statusStarting {
			y.status = statusIdle
			y.cancel()
		}
	}()

	// 1. 创建发布
	if err := y.createPublication(); err != nil {
		return fmt.Errorf("failed to create publication: %w", err)
	}

	// 2. 创建复制槽
	if err := y.createReplicationSlot(); err != nil {
		return fmt.Errorf("failed to create replication slot: %w", err)
	}

	// 3. 启动复制
	y.wg.Add(1)
	go y.startReplication()

	y.mu.Lock()
	y.status = statusRunning
	y.mu.Unlock()

	return nil
}

// Stop 停止捕获过程
func (y *Yugabyte) Stop() error {
	y.mu.Lock()
	if y.status != statusRunning {
		y.mu.Unlock()
		return fmt.Errorf("capturer not running")
	}
	y.status = statusStopping
	y.mu.Unlock()

	// 取消上下文，停止处理
	if y.cancel != nil {
		y.cancel()
	}

	// 等待处理完成
	y.wg.Wait()

	// 删除复制资源
	err := y.cleanupResources()

	// 关闭事件通道
	close(y.events)

	y.mu.Lock()
	y.status = statusIdle
	y.mu.Unlock()

	return err
}

func (y *Yugabyte) createConn(ctx context.Context) error {
	y.mu.Lock()
	defer y.mu.Unlock()

	if y.conn != nil {
		return nil
	}
	pgconn, err := pgconn.Connect(ctx, y.config.ConnString)
	if err != nil {
		return fmt.Errorf("failed to connect to YugabyteDB: %w", err)
	}
	y.conn = pgconn
	return nil
}

// 创建发布
func (y *Yugabyte) createPublication() error {
	// 检查发布是否已存在
	checkQuery := fmt.Sprintf(
		"SELECT COUNT(*) FROM pg_publication WHERE pubname = '%s'",
		y.config.PublicationName,
	)
	result := y.conn.Exec(y.ctx, checkQuery)
	results, err := result.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to check publication existence: %w", err)
	}

	var exists bool
	if len(results) > 0 && len(results[0].Rows) > 0 {
		count, err := parseInt(string(results[0].Rows[0][0]))
		if err != nil {
			return fmt.Errorf("failed to parse count result: %w", err)
		}
		exists = count > 0
	}

	if exists {
		log.Printf("Publication %s already exists", y.config.PublicationName)
		return nil
	}

	// 创建发布
	pubInsert := true
	pubUpdate := true
	pubDelete := true
	pubTruncate := true
	params := pglogrepl.PublicationParams{
		Name:            y.config.PublicationName,
		Tables:          y.config.Tables,
		AllTables:       len(y.config.Tables) == 0, // 如果没有指定表，则发布所有表
		PublishInsert:   &pubInsert,
		PublishUpdate:   &pubUpdate,
		PublishDelete:   &pubDelete,
		PublishTruncate: &pubTruncate,
	}

	if err := pglogrepl.CreatePublication(y.ctx, y.conn, params); err != nil {
		return fmt.Errorf("failed to create publication: %w", err)
	}

	log.Printf("Created publication: %s", y.config.PublicationName)
	y.pubCreated = true
	return nil
}

// 创建复制槽
func (y *Yugabyte) createReplicationSlot() error {
	// 检查槽是否已存在
	slots, err := pglogrepl.ListReplicationSlots(y.ctx, y.conn)
	if err != nil {
		return fmt.Errorf("failed to list replication slots: %w", err)
	}

	var slotExists bool
	for _, slot := range slots {
		if slot.SlotName == y.config.SlotName {
			slotExists = true
			log.Printf("Replication slot %s already exists", y.config.SlotName)
			break
		}
	}

	if slotExists {
		return nil // 槽已存在，无需创建
	}

	// 创建复制槽
	options := pglogrepl.CreateReplicationSlotOptions{
		Temporary: y.config.TemporarySlot,
	}

	result, err := pglogrepl.CreateReplicationSlot(
		y.ctx,
		y.conn,
		y.config.SlotName,
		defaultOutputPlugin,
		options,
	)
	if err != nil {
		return fmt.Errorf("failed to create replication slot: %w", err)
	}

	log.Printf("Created replication slot: %s, consistent point: %s", result.SlotName, result.ConsistentPoint)
	y.slotCreated = true
	return nil
}

// 启动复制过程
func (y *Yugabyte) startReplication() {
	defer y.wg.Done()

	// 设置起始LSN
	startLSN := y.checkpoint.LSN
	if startLSN == 0 {
		log.Println("Starting replication from the beginning")
	} else {
		log.Printf("Resuming replication from checkpoint: %s", startLSN)
	}

	// 设置插件参数
	pluginArgs := []string{
		fmt.Sprintf("proto_version '%s'", y.config.ProtocolVersion),
		fmt.Sprintf("publication_names '%s'", y.config.PublicationName),
	}

	if y.config.EnableMessages {
		pluginArgs = append(pluginArgs, "messages 'true'")
	}

	if y.config.EnableStreaming && y.config.ProtocolVersion == "2" {
		pluginArgs = append(pluginArgs, "streaming 'true'")
	}

	// 开始复制
	err := pglogrepl.StartReplication(
		y.ctx,
		y.conn,
		y.config.SlotName,
		startLSN,
		pglogrepl.StartReplicationOptions{
			Mode:       pglogrepl.LogicalReplication,
			PluginArgs: pluginArgs,
		},
	)
	if err != nil {
		y.setError(fmt.Errorf("failed to start replication: %w", err))
		return
	}

	log.Printf("Started logical replication on slot %s from %s", y.config.SlotName, startLSN)

	// 设置复制状态
	clientXLogPos := startLSN
	standbyMessageTimeout := defaultHeartbeat
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	for {
		select {
		case <-y.ctx.Done():
			// 上下文取消，停止复制
			log.Println("Replication stopped: context canceled")
			return

		default:
			// 检查是否需要发送状态更新
			if time.Now().After(nextStandbyMessageDeadline) {
				err = pglogrepl.SendStandbyStatusUpdate(y.ctx, y.conn, pglogrepl.StandbyStatusUpdate{
					WALWritePosition: clientXLogPos,
					WALFlushPosition: clientXLogPos,
					WALApplyPosition: clientXLogPos,
					ClientTime:       time.Now(),
					ReplyRequested:   false,
				})
				if err != nil {
					y.setError(fmt.Errorf("failed to send status update: %w", err))
					return
				}
				log.Printf("Sent standby status update at %s", clientXLogPos)
				nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
			}

			// 接收复制消息，带超时
			ctx, cancel := context.WithDeadline(y.ctx, nextStandbyMessageDeadline)
			rawMsg, err := y.conn.ReceiveMessage(ctx)
			cancel()

			if err != nil {
				if pgconn.Timeout(err) {
					// 这是正常的超时，继续循环
					continue
				}
				y.setError(fmt.Errorf("failed to receive message: %w", err))
				return
			}

			// 处理错误响应
			if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
				y.setError(fmt.Errorf("received Postgres error: %s", errMsg.Message))
				return
			}

			// 处理复制消息
			msg, ok := rawMsg.(*pgproto3.CopyData)
			if !ok {
				log.Printf("Received unexpected message: %T", rawMsg)
				continue
			}

			// 根据消息类型处理
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				// 处理主服务器发来的保活消息
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					y.setError(fmt.Errorf("failed to parse keepalive message: %w", err))
					return
				}

				log.Printf("Primary Keepalive: ServerWALEnd: %s, ServerTime: %s, ReplyRequested: %v",
					pkm.ServerWALEnd, pkm.ServerTime, pkm.ReplyRequested)

				// 更新位置
				if pkm.ServerWALEnd > clientXLogPos {
					clientXLogPos = pkm.ServerWALEnd
				}

				// 如果服务器请求回复，立即发送状态更新
				if pkm.ReplyRequested {
					nextStandbyMessageDeadline = time.Time{}
				}

			case pglogrepl.XLogDataByteID:
				// 处理WAL数据
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					y.setError(fmt.Errorf("failed to parse XLogData: %w", err))
					return
				}

				log.Printf("XLogData: WALStart: %s, ServerWALEnd: %s, ServerTime: %s",
					xld.WALStart, xld.ServerWALEnd, xld.ServerTime)

				// 处理WAL数据
				if err := y.processWALData(xld); err != nil {
					y.setError(fmt.Errorf("error processing WAL data: %w", err))
					time.Sleep(defaultErrorWait)
					continue
				}

				// 更新位置
				if xld.WALStart > clientXLogPos {
					clientXLogPos = xld.WALStart
				}

				// 保存检查点
				y.mu.Lock()
				y.checkpoint.LSN = clientXLogPos
				y.mu.Unlock()

			default:
				log.Printf("Received unknown message type: %d", msg.Data[0])
			}
		}
	}
}

// 处理WAL数据
func (y *Yugabyte) processWALData(xld *pglogrepl.XLogData) error {
	// 根据协议版本选择正确的解析方式
	useV2 := y.config.ProtocolVersion == "2"

	// 解析逻辑复制消息
	var err error
	var eventsSent int

	if useV2 {
		eventsSent, err = y.processWALDataV2(xld)
	} else {
		eventsSent, err = y.processWALDataV1(xld)
	}

	if err != nil {
		return fmt.Errorf("error processing WAL data: %w", err)
	}

	log.Printf("Processed WAL data, sent %d events", eventsSent)
	return nil
}

// 处理WAL数据 (协议V1)
func (y *Yugabyte) processWALDataV1(xld *pglogrepl.XLogData) (int, error) {
	// 解析逻辑复制消息
	logicalMsg, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		return 0, fmt.Errorf("failed to parse logical message: %w", err)
	}

	log.Printf("Received logical replication message: %s", logicalMsg.Type())

	eventsSent := 0

	// 处理不同类型的消息
	switch msg := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		// 存储表结构信息供后续使用
		y.relations[msg.RelationID] = msg
		log.Printf("Relation: %s.%s with %d columns", msg.Namespace, msg.RelationName, len(msg.Columns))

	case *pglogrepl.BeginMessage:
		// 事务开始
		log.Printf("Begin: XID %d at LSN %s", msg.Xid, msg.FinalLSN)

	case *pglogrepl.CommitMessage:
		// 事务提交
		log.Printf("commit message")
		// log.Printf("Commit: XID %d at LSN %s, end LSN %s", msg.Xid, msg.CommitLSN, msg.TransactionEndLSN)

		// // 如果是YugabyteDB，可能需要处理混合时间戳
		// if y.checkpoint.YBStreamID != "" && msg.CommitTime > 0 {
		// 	y.checkpoint.YBCommitHT = int64(msg.CommitTime)
		// 	log.Printf("Updated commit hybrid timestamp: %d", y.checkpoint.YBCommitHT)
		// }

	case *pglogrepl.InsertMessage:
		// 处理插入操作
		event, err := y.createInsertEventV1(msg)
		if err != nil {
			return eventsSent, fmt.Errorf("failed to create insert event: %w", err)
		}

		// 发送事件到通道
		select {
		case y.events <- event:
			eventsSent++
		case <-y.ctx.Done():
			return eventsSent, fmt.Errorf("context canceled while sending event")
		}

	case *pglogrepl.UpdateMessage:
		// 处理更新操作
		event, err := y.createUpdateEventV1(msg)
		if err != nil {
			return eventsSent, fmt.Errorf("failed to create update event: %w", err)
		}

		select {
		case y.events <- event:
			eventsSent++
		case <-y.ctx.Done():
			return eventsSent, fmt.Errorf("context canceled while sending event")
		}

	case *pglogrepl.DeleteMessage:
		// 处理删除操作
		event, err := y.createDeleteEventV1(msg)
		if err != nil {
			return eventsSent, fmt.Errorf("failed to create delete event: %w", err)
		}

		select {
		case y.events <- event:
			eventsSent++
		case <-y.ctx.Done():
			return eventsSent, fmt.Errorf("context canceled while sending event")
		}

	case *pglogrepl.TruncateMessage:
		// 处理截断操作
		event, err := y.createTruncateEventV1(msg)
		if err != nil {
			return eventsSent, fmt.Errorf("failed to create truncate event: %w", err)
		}

		select {
		case y.events <- event:
			eventsSent++
		case <-y.ctx.Done():
			return eventsSent, fmt.Errorf("context canceled while sending event")
		}

	case *pglogrepl.LogicalDecodingMessage:
		// 处理逻辑解码消息
		log.Printf("Logical decoding message: prefix=%s, content=%s", msg.Prefix, string(msg.Content))

		// 可以选择性地生成事件
		if msg.Prefix == "curio" || msg.Prefix == "yb_event" {
			event := models.Event{
				ID:        uuid.New().String(),
				Type:      "logical_message",
				Table:     "",
				Schema:    "",
				Data:      map[string]any{"prefix": msg.Prefix, "content": string(msg.Content)},
				Timestamp: time.Now(),
			}

			select {
			case y.events <- event:
				eventsSent++
			case <-y.ctx.Done():
				return eventsSent, fmt.Errorf("context canceled while sending event")
			}
		}

	case *pglogrepl.TypeMessage:
		// 处理类型消息，通常不需要特殊处理
		log.Printf("Type message received for type ID %d", msg.DataType)

	case *pglogrepl.OriginMessage:
		// 处理来源消息
		log.Printf("Origin message from %s at LSN %s", msg.Name, msg.CommitLSN)

	default:
		log.Printf("Unknown message type: %T", msg)
	}

	return eventsSent, nil
}

// 处理WAL数据 (协议V2)
func (y *Yugabyte) processWALDataV2(xld *pglogrepl.XLogData) (int, error) {
	// 解析逻辑复制消息
	logicalMsg, err := pglogrepl.ParseV2(xld.WALData, y.inStream)
	if err != nil {
		return 0, fmt.Errorf("failed to parse V2 logical message: %w", err)
	}

	log.Printf("Received logical replication message V2: %s", logicalMsg.Type())

	eventsSent := 0

	// 处理不同类型的消息
	switch msg := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		// 存储表结构信息供后续使用
		y.relationsV2[msg.RelationID] = msg
		log.Printf("Relation V2: %s.%s with %d columns", msg.Namespace, msg.RelationName, len(msg.Columns))

	case *pglogrepl.BeginMessage:
		// 事务开始
		log.Printf("Begin: XID %d at LSN %s", msg.Xid, msg.FinalLSN)

	case *pglogrepl.CommitMessage:
		// 事务提交
		log.Printf("commit message")
		// log.Printf("Commit: XID %d at LSN %s, end LSN %s", msg.Xid, msg.CommitLSN, msg.TransactionEndLSN)

		// // 如果是YugabyteDB，可能需要处理混合时间戳
		// if y.checkpoint.YBStreamID != "" && msg.CommitTime > 0 {
		// 	y.checkpoint.YBCommitHT = int64(msg.CommitTime)
		// 	log.Printf("Updated commit hybrid timestamp: %d", y.checkpoint.YBCommitHT)
		// }

	case *pglogrepl.InsertMessageV2:
		// 处理插入操作
		event, err := y.createInsertEventV2(msg)
		if err != nil {
			return eventsSent, fmt.Errorf("failed to create insert event V2: %w", err)
		}

		// 发送事件到通道
		select {
		case y.events <- event:
			eventsSent++
			log.Printf("Sent insert event for table %s", event.Table)
		case <-y.ctx.Done():
			return eventsSent, fmt.Errorf("context canceled while sending event")
		}

	case *pglogrepl.UpdateMessageV2:
		// 处理更新操作
		event, err := y.createUpdateEventV2(msg)
		if err != nil {
			return eventsSent, fmt.Errorf("failed to create update event V2: %w", err)
		}

		select {
		case y.events <- event:
			eventsSent++
			log.Printf("Sent update event for table %s", event.Table)
		case <-y.ctx.Done():
			return eventsSent, fmt.Errorf("context canceled while sending event")
		}

	case *pglogrepl.DeleteMessageV2:
		// 处理删除操作
		event, err := y.createDeleteEventV2(msg)
		if err != nil {
			return eventsSent, fmt.Errorf("failed to create delete event V2: %w", err)
		}

		select {
		case y.events <- event:
			eventsSent++
			log.Printf("Sent delete event for table %s", event.Table)
		case <-y.ctx.Done():
			return eventsSent, fmt.Errorf("context canceled while sending event")
		}

	case *pglogrepl.TruncateMessageV2:
		// 处理截断操作
		event, err := y.createTruncateEventV2(msg)
		if err != nil {
			return eventsSent, fmt.Errorf("failed to create truncate event V2: %w", err)
		}

		select {
		case y.events <- event:
			eventsSent++
			log.Printf("Sent truncate event")
		case <-y.ctx.Done():
			return eventsSent, fmt.Errorf("context canceled while sending event")
		}

	case *pglogrepl.LogicalDecodingMessageV2:
		// 处理逻辑解码消息
		log.Printf("Logical decoding message V2: prefix=%s, content=%s, xid=%d",
			msg.Prefix, string(msg.Content), msg.Xid)

		// 可以选择性地生成事件
		if msg.Prefix == "curio" || msg.Prefix == "yb_event" {
			event := models.Event{
				ID:     uuid.New().String(),
				Type:   "logical_message",
				Table:  "",
				Schema: "",
				Data: map[string]any{
					"prefix":  msg.Prefix,
					"content": string(msg.Content),
					"xid":     msg.Xid,
				},
				Timestamp: time.Now(),
			}

			select {
			case y.events <- event:
				eventsSent++
			case <-y.ctx.Done():
				return eventsSent, fmt.Errorf("context canceled while sending event")
			}
		}

	case *pglogrepl.TypeMessageV2:
		// 处理类型消息，通常不需要特殊处理
		log.Printf("Type message V2 received")

	case *pglogrepl.OriginMessage:
		// 处理来源消息
		log.Printf("Origin message from %s at LSN %s", msg.Name, msg.CommitLSN)

	case *pglogrepl.StreamStartMessageV2:
		// 处理流开始消息
		y.inStream = true
		log.Printf("Stream start message: XID %d, first segment: %d", msg.Xid, msg.FirstSegment)

	case *pglogrepl.StreamStopMessageV2:
		// 处理流结束消息
		y.inStream = false
		log.Printf("Stream stop message")

	case *pglogrepl.StreamCommitMessageV2:
		// 处理流提交消息
		log.Printf("Stream commit message: XID %d", msg.Xid)

		// 如果是YugabyteDB，可以处理混合时间戳
		log.Printf("StreamCommitMessageV2")
		// if y.checkpoint.YBStreamID != "" && msg.CommitTimestamp > 0 {
		// 	y.checkpoint.YBCommitHT = int64(msg.CommitTimestamp)
		// 	log.Printf("Updated stream commit hybrid timestamp: %d", y.checkpoint.YBCommitHT)
		// }

	case *pglogrepl.StreamAbortMessageV2:
		// 处理流中止消息
		log.Printf("Stream abort message: XID %d", msg.Xid)

	default:
		log.Printf("Unknown message type V2: %T", msg)
	}

	return eventsSent, nil
}

// 创建插入事件（V1）
func (y *Yugabyte) createInsertEventV1(msg *pglogrepl.InsertMessage) (models.Event, error) {
	rel, ok := y.relations[msg.RelationID]
	if !ok {
		return models.Event{}, fmt.Errorf("unknown relation ID %d", msg.RelationID)
	}

	// 解析元组数据
	values, err := y.decodeTupleData(rel.Columns, msg.Tuple.Columns)
	if err != nil {
		return models.Event{}, fmt.Errorf("failed to decode tuple data: %w", err)
	}

	// 创建事件
	eventID := uuid.New().String()
	event := models.Event{
		ID:        eventID,
		Type:      "insert",
		Table:     rel.RelationName,
		Schema:    rel.Namespace,
		Data:      values,
		Timestamp: time.Now(),
		Source:    "postgres_logical_replication",
	}

	// 更新检查点
	y.mu.Lock()
	y.checkpoint.LastEventID = eventID
	y.mu.Unlock()

	return event, nil
}

// 创建更新事件（V1）
func (y *Yugabyte) createUpdateEventV1(msg *pglogrepl.UpdateMessage) (models.Event, error) {
	rel, ok := y.relations[msg.RelationID]
	if !ok {
		return models.Event{}, fmt.Errorf("unknown relation ID %d", msg.RelationID)
	}

	// 解析旧数据和新数据
	var oldValues map[string]any
	var err error

	if msg.OldTuple != nil {
		oldValues, err = y.decodeTupleData(rel.Columns, msg.OldTuple.Columns)
		if err != nil {
			return models.Event{}, fmt.Errorf("failed to decode old tuple data: %w", err)
		}
	}

	newValues, err := y.decodeTupleData(rel.Columns, msg.NewTuple.Columns)
	if err != nil {
		return models.Event{}, fmt.Errorf("failed to decode new tuple data: %w", err)
	}

	// 合并数据
	data := map[string]any{
		"new": newValues,
	}

	if oldValues != nil {
		data["old"] = oldValues
	}

	// 创建事件
	eventID := uuid.New().String()
	event := models.Event{
		ID:        eventID,
		Type:      "update",
		Table:     rel.RelationName,
		Schema:    rel.Namespace,
		Data:      data,
		Timestamp: time.Now(),
		Source:    "postgres_logical_replication",
	}

	// 更新检查点
	y.mu.Lock()
	y.checkpoint.LastEventID = eventID
	y.mu.Unlock()

	return event, nil
}

// 创建删除事件（V1）
func (y *Yugabyte) createDeleteEventV1(msg *pglogrepl.DeleteMessage) (models.Event, error) {
	rel, ok := y.relations[msg.RelationID]
	if !ok {
		return models.Event{}, fmt.Errorf("unknown relation ID %d", msg.RelationID)
	}

	// 解析被删除的数据
	oldValues, err := y.decodeTupleData(rel.Columns, msg.OldTuple.Columns)
	if err != nil {
		return models.Event{}, fmt.Errorf("failed to decode tuple data: %w", err)
	}

	// 创建事件
	eventID := uuid.New().String()
	event := models.Event{
		ID:        eventID,
		Type:      "delete",
		Table:     rel.RelationName,
		Schema:    rel.Namespace,
		Data:      oldValues,
		Timestamp: time.Now(),
		Source:    "postgres_logical_replication",
	}

	// 更新检查点
	y.mu.Lock()
	y.checkpoint.LastEventID = eventID
	y.mu.Unlock()

	return event, nil
}

// 创建截断事件（V1）
func (y *Yugabyte) createTruncateEventV1(msg *pglogrepl.TruncateMessage) (models.Event, error) {
	// 获取被截断的表名称
	tableNames := make([]string, 0, len(msg.RelationIDs))
	for _, relID := range msg.RelationIDs {
		if rel, ok := y.relations[relID]; ok {
			tableNames = append(tableNames, rel.RelationName)
		}
	}

	// 创建事件
	eventID := uuid.New().String()
	event := models.Event{
		ID:     eventID,
		Type:   "truncate",
		Table:  strings.Join(tableNames, ","),
		Schema: "", // 可能有多个schema
		Data: map[string]any{
			"tables": tableNames,
		},
		Timestamp: time.Now(),
		Source:    "postgres_logical_replication",
	}

	// 更新检查点
	y.mu.Lock()
	y.checkpoint.LastEventID = eventID
	y.mu.Unlock()

	return event, nil
}

// 创建插入事件（V2）
func (y *Yugabyte) createInsertEventV2(msg *pglogrepl.InsertMessageV2) (models.Event, error) {
	rel, ok := y.relationsV2[msg.RelationID]
	if !ok {
		return models.Event{}, fmt.Errorf("unknown relation ID %d", msg.RelationID)
	}

	// 解析元组数据
	values, err := y.decodeTupleData(rel.Columns, msg.Tuple.Columns)
	if err != nil {
		return models.Event{}, fmt.Errorf("failed to decode tuple data: %w", err)
	}

	// 创建事件
	eventID := uuid.New().String()
	event := models.Event{
		ID:        eventID,
		Type:      "insert",
		Table:     rel.RelationName,
		Schema:    rel.Namespace,
		Data:      values,
		Timestamp: time.Now(),
		Source:    "postgres_logical_replication",
		Extra: map[string]any{
			"xid": msg.Xid,
		},
	}

	// 更新检查点
	y.mu.Lock()
	y.checkpoint.LastEventID = eventID
	y.mu.Unlock()

	return event, nil
}

// 创建更新事件（V2）
func (y *Yugabyte) createUpdateEventV2(msg *pglogrepl.UpdateMessageV2) (models.Event, error) {
	rel, ok := y.relationsV2[msg.RelationID]
	if !ok {
		return models.Event{}, fmt.Errorf("unknown relation ID %d", msg.RelationID)
	}

	// 解析旧数据和新数据
	var oldValues map[string]any
	var err error

	if msg.OldTuple != nil {
		oldValues, err = y.decodeTupleData(rel.Columns, msg.OldTuple.Columns)
		if err != nil {
			return models.Event{}, fmt.Errorf("failed to decode old tuple data: %w", err)
		}
	}

	newValues, err := y.decodeTupleData(rel.Columns, msg.NewTuple.Columns)
	if err != nil {
		return models.Event{}, fmt.Errorf("failed to decode new tuple data: %w", err)
	}

	// 合并数据
	data := map[string]any{
		"new": newValues,
	}

	if oldValues != nil {
		data["old"] = oldValues
	}

	// 创建事件
	eventID := uuid.New().String()
	event := models.Event{
		ID:        eventID,
		Type:      "update",
		Table:     rel.RelationName,
		Schema:    rel.Namespace,
		Data:      data,
		Timestamp: time.Now(),
		Source:    "postgres_logical_replication",
		Extra: map[string]any{
			"xid": msg.Xid,
		},
	}

	// 更新检查点
	y.mu.Lock()
	y.checkpoint.LastEventID = eventID
	y.mu.Unlock()

	return event, nil
}

// 创建删除事件（V2）
func (y *Yugabyte) createDeleteEventV2(msg *pglogrepl.DeleteMessageV2) (models.Event, error) {
	rel, ok := y.relationsV2[msg.RelationID]
	if !ok {
		return models.Event{}, fmt.Errorf("unknown relation ID %d", msg.RelationID)
	}

	// 解析被删除的数据
	oldValues, err := y.decodeTupleData(rel.Columns, msg.OldTuple.Columns)
	if err != nil {
		return models.Event{}, fmt.Errorf("failed to decode tuple data: %w", err)
	}

	// 创建事件
	eventID := uuid.New().String()
	event := models.Event{
		ID:        eventID,
		Type:      "delete",
		Table:     rel.RelationName,
		Schema:    rel.Namespace,
		Data:      oldValues,
		Timestamp: time.Now(),
		Source:    "postgres_logical_replication",
		Extra: map[string]any{
			"xid": msg.Xid,
		},
	}

	// 更新检查点
	y.mu.Lock()
	y.checkpoint.LastEventID = eventID
	y.mu.Unlock()

	return event, nil
}

// 创建截断事件（V2）
func (y *Yugabyte) createTruncateEventV2(msg *pglogrepl.TruncateMessageV2) (models.Event, error) {
	// 获取被截断的表名称
	tableNames := make([]string, 0, len(msg.RelationIDs))
	for _, relID := range msg.RelationIDs {
		if rel, ok := y.relationsV2[relID]; ok {
			tableNames = append(tableNames, rel.RelationName)
		}
	}

	// 创建事件
	eventID := uuid.New().String()
	event := models.Event{
		ID:     eventID,
		Type:   "truncate",
		Table:  strings.Join(tableNames, ","),
		Schema: "", // 可能有多个schema
		Data: map[string]any{
			"tables": tableNames,
		},
		Timestamp: time.Now(),
		Source:    "postgres_logical_replication",
		Extra: map[string]any{
			"xid":     msg.Xid,
			"options": msg.TruncateMessage,
		},
	}

	// 更新检查点
	y.mu.Lock()
	y.checkpoint.LastEventID = eventID
	y.mu.Unlock()

	return event, nil
}

func (y *Yugabyte) decodeTupleData(columns []*pglogrepl.RelationMessageColumn, tupleColumns []*pglogrepl.TupleDataColumn) (map[string]any, error) {
	values := make(map[string]any)

	for i, col := range tupleColumns {
		if i >= len(columns) {
			return nil, fmt.Errorf("column index out of bounds: %d >= %d", i, len(columns))
		}

		colName := columns[i].Name

		switch col.DataType {
		case 'n': // null
			values[colName] = nil
		case 'u': // unchanged toast
			// 这个TOAST值没有改变，我们不在元组中存储TOAST值
			values[colName] = "[unchanged toast]"
		case 't': // text format
			val, err := y.decodeTextColumnData(col.Data, columns[i].DataType)
			if err != nil {
				return nil, fmt.Errorf("failed to decode column %s: %w", colName, err)
			}
			values[colName] = val
		case 'b': // binary format (pg_output v2)
			// 目前简单处理为十六进制字符串
			values[colName] = fmt.Sprintf("0x%x", col.Data)
		}
	}

	return values, nil
}

// 解码文本格式的列数据
func (y *Yugabyte) decodeTextColumnData(data []byte, dataType uint32) (any, error) {
	// 尝试使用类型映射解析
	if dt, ok := y.typeMap.TypeForOID(dataType); ok {
		val, err := dt.Codec.DecodeValue(y.typeMap, dataType, pgtype.TextFormatCode, data)
		if err != nil {
			// 如果解析失败，返回原始字符串
			return string(data), nil
		}
		return val, nil
	}

	// 默认以字符串形式返回
	return string(data), nil
}

// 清理资源
func (y *Yugabyte) cleanupResources() error {
	var errs []error

	// 只有当配置了删除槽且槽是由我们创建的时才删除
	if y.config.DropSlotOnStop && y.slotCreated {
		err := y.dropReplicationSlot()
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to drop replication slot: %w", err))
		}
	}

	// 如果需要删除发布
	//if y.config.DropPublicationOnStop && y.pubCreated {
	//	err := y.dropPublication()
	//	if err != nil {
	//		errs = append(errs, fmt.Errorf("failed to drop publication: %w", err))
	//	}
	//}

	if len(errs) > 0 {
		// 合并所有错误
		var errMsgs []string
		for _, err := range errs {
			errMsgs = append(errMsgs, err.Error())
		}
		return fmt.Errorf("cleanup errors: %s", strings.Join(errMsgs, "; "))
	}

	return nil
}

// 删除复制槽
func (y *Yugabyte) dropReplicationSlot() error {
	log.Printf("Dropping replication slot: %s", y.config.SlotName)

	// 检查槽是否存在
	slots, err := pglogrepl.ListReplicationSlots(y.ctx, y.conn)
	if err != nil {
		return fmt.Errorf("failed to list replication slots: %w", err)
	}

	var slotExists bool
	for _, slot := range slots {
		if slot.SlotName == y.config.SlotName {
			slotExists = true
			break
		}
	}

	if !slotExists {
		log.Printf("Replication slot %s does not exist, skipping drop", y.config.SlotName)
		return nil
	}

	// 删除复制槽
	query := fmt.Sprintf("DROP_REPLICATION_SLOT %s", y.config.SlotName)
	result := y.conn.Exec(y.ctx, query)
	_, err = result.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to drop replication slot: %w", err)
	}

	log.Printf("Successfully dropped replication slot: %s", y.config.SlotName)
	return nil
}

// 删除发布
func (y *Yugabyte) dropPublication() error {
	log.Printf("Dropping publication: %s", y.config.PublicationName)

	// 检查发布是否存在
	checkQuery := fmt.Sprintf(
		"SELECT COUNT(*) FROM pg_publication WHERE pubname = '%s'",
		y.config.PublicationName,
	)
	result := y.conn.Exec(y.ctx, checkQuery)
	results, err := result.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to check publication existence: %w", err)
	}

	var exists bool
	if len(results) > 0 && len(results[0].Rows) > 0 {
		count, err := parseInt(string(results[0].Rows[0][0]))
		if err != nil {
			return fmt.Errorf("failed to parse count result: %w", err)
		}
		exists = count > 0
	}

	if !exists {
		log.Printf("Publication %s does not exist, skipping drop", y.config.PublicationName)
		return nil
	}

	// 删除发布
	if err := pglogrepl.DropPublication(y.ctx, y.conn, y.config.PublicationName); err != nil {
		return fmt.Errorf("failed to drop publication: %w", err)
	}

	log.Printf("Successfully dropped publication: %s", y.config.PublicationName)
	return nil
}

// 设置错误状态
func (y *Yugabyte) setError(err error) {
	y.mu.Lock()
	defer y.mu.Unlock()

	y.lastError = err
	y.status = statusError
	log.Printf("ERROR: %v", err)
}

// GetStatus 获取当前状态
func (y *Yugabyte) GetStatus() string {
	y.mu.RLock()
	defer y.mu.RUnlock()
	return y.status
}

// GetLastError 获取最后一个错误
func (y *Yugabyte) GetLastError() error {
	y.mu.RLock()
	defer y.mu.RUnlock()
	return y.lastError
}

// 辅助函数: 将字符串解析为整数
func parseInt(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

var _ Capturer = (*Yugabyte)(nil)
