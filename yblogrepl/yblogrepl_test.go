package yblogrepl_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/strahe/curio-sentinel/yblogrepl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/yugabyte/pgx/v5/pgconn"
	"github.com/yugabyte/pgx/v5/pgproto3"
)

func TestLSNSuite(t *testing.T) {
	suite.Run(t, new(lsnSuite))
}

type lsnSuite struct {
	suite.Suite
}

func (s *lsnSuite) R() *require.Assertions {
	return s.Require()
}

func (s *lsnSuite) Equal(e, a interface{}, args ...interface{}) {
	s.R().Equal(e, a, args...)
}

func (s *lsnSuite) NoError(err error) {
	s.R().NoError(err)
}

func (s *lsnSuite) TestScannerInterface() {
	var lsn yblogrepl.LSN
	lsnText := "16/B374D848"
	lsnUint64 := uint64(97500059720)
	var err error

	err = lsn.Scan(lsnText)
	s.NoError(err)
	s.Equal(lsnText, lsn.String())

	err = lsn.Scan([]byte(lsnText))
	s.NoError(err)
	s.Equal(lsnText, lsn.String())

	lsn = 0
	err = lsn.Scan(lsnUint64)
	s.NoError(err)
	s.Equal(lsnText, lsn.String())

	err = lsn.Scan(int64(lsnUint64))
	s.Error(err)
	s.T().Log(err)
}

func (s *lsnSuite) TestScanToNil() {
	var lsnPtr *yblogrepl.LSN
	err := lsnPtr.Scan("16/B374D848")
	s.NoError(err)
}

func (s *lsnSuite) TestValueInterface() {
	lsn := yblogrepl.LSN(97500059720)
	driverValue, err := lsn.Value()
	s.NoError(err)
	lsnStr, ok := driverValue.(string)
	s.R().True(ok)
	s.Equal("16/B374D848", lsnStr)
}

const slotName = "yblogrepl._test"
const outputPlugin = "test_decoding"

func closeConn(t testing.TB, conn *pgconn.PgConn) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, conn.Close(ctx))
}

func TestIdentifySystem(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	conn, err := pgconn.Connect(ctx, os.Getenv("yblogrepl._TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closeConn(t, conn)

}

func TestGetHistoryFile(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	config, err := pgconn.ParseConfig(os.Getenv("yblogrepl._TEST_CONN_STRING"))
	require.NoError(t, err)
	config.RuntimeParams["replication"] = "on"

	conn, err := pgconn.ConnectConfig(ctx, config)
	require.NoError(t, err)
	defer closeConn(t, conn)

	_, err = yblogrepl.TimelineHistory(ctx, conn, 0)
	require.Error(t, err)

	_, err = yblogrepl.TimelineHistory(ctx, conn, 1)
	require.Error(t, err)

}

func TestCreateReplicationSlot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	conn, err := pgconn.Connect(ctx, os.Getenv("yblogrepl._TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closeConn(t, conn)

	result, err := yblogrepl.CreateReplicationSlot(ctx, conn, slotName, outputPlugin, yblogrepl.CreateReplicationSlotOptions{Temporary: true})
	require.NoError(t, err)

	assert.Equal(t, slotName, result.SlotName)
	assert.Equal(t, outputPlugin, result.OutputPlugin)
}

func TestDropReplicationSlot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	conn, err := pgconn.Connect(ctx, os.Getenv("yblogrepl._TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closeConn(t, conn)

	_, err = yblogrepl.CreateReplicationSlot(ctx, conn, slotName, outputPlugin, yblogrepl.CreateReplicationSlotOptions{Temporary: true})
	require.NoError(t, err)

	err = yblogrepl.DropReplicationSlot(ctx, conn, slotName, yblogrepl.DropReplicationSlotOptions{})
	require.NoError(t, err)

	_, err = yblogrepl.CreateReplicationSlot(ctx, conn, slotName, outputPlugin, yblogrepl.CreateReplicationSlotOptions{Temporary: true})
	require.NoError(t, err)
}

func TestStartReplication(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	conn, err := pgconn.Connect(ctx, os.Getenv("yblogrepl._TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closeConn(t, conn)

	_, err = yblogrepl.CreateReplicationSlot(ctx, conn, slotName, outputPlugin, yblogrepl.CreateReplicationSlotOptions{Temporary: true})
	require.NoError(t, err)

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		config, err := pgconn.ParseConfig(os.Getenv("yblogrepl._TEST_CONN_STRING"))
		require.NoError(t, err)
		delete(config.RuntimeParams, "replication")

		conn, err := pgconn.ConnectConfig(ctx, config)
		require.NoError(t, err)
		defer closeConn(t, conn)

		_, err = conn.Exec(ctx, `
create table t(id int primary key, name text);

insert into t values (1, 'foo');
insert into t values (2, 'bar');
insert into t values (3, 'baz');

update t set name='quz' where id=3;

delete from t where id=2;

drop table t;
`).ReadAll()
		require.NoError(t, err)
	}()

	rxKeepAlive := func() yblogrepl.PrimaryKeepaliveMessage {
		msg, err := conn.ReceiveMessage(ctx)
		require.NoError(t, err)
		cdMsg, ok := msg.(*pgproto3.CopyData)
		require.True(t, ok)

		require.Equal(t, byte(yblogrepl.PrimaryKeepaliveMessageByteID), cdMsg.Data[0])
		pkm, err := yblogrepl.ParsePrimaryKeepaliveMessage(cdMsg.Data[1:])
		require.NoError(t, err)
		return pkm
	}

	rxXLogData := func() *yblogrepl.XLogData {
		var cdMsg *pgproto3.CopyData
		// Discard keepalive messages
		for {
			msg, err := conn.ReceiveMessage(ctx)
			require.NoError(t, err)
			var ok bool
			cdMsg, ok = msg.(*pgproto3.CopyData)
			require.True(t, ok)
			if cdMsg.Data[0] != yblogrepl.PrimaryKeepaliveMessageByteID {
				break
			}
		}
		require.Equal(t, byte(yblogrepl.XLogDataByteID), cdMsg.Data[0])
		xld, err := yblogrepl.ParseXLogData(cdMsg.Data[1:])
		require.NoError(t, err)
		return xld
	}

	rxKeepAlive()
	xld := rxXLogData()
	assert.Equal(t, "BEGIN", string(xld.WALData[:5]))
	xld = rxXLogData()
	assert.Equal(t, "table public.t: INSERT: id[integer]:1 name[text]:'foo'", string(xld.WALData))
	xld = rxXLogData()
	assert.Equal(t, "table public.t: INSERT: id[integer]:2 name[text]:'bar'", string(xld.WALData))
	xld = rxXLogData()
	assert.Equal(t, "table public.t: INSERT: id[integer]:3 name[text]:'baz'", string(xld.WALData))
	xld = rxXLogData()
	assert.Equal(t, "table public.t: UPDATE: id[integer]:3 name[text]:'quz'", string(xld.WALData))
	xld = rxXLogData()
	assert.Equal(t, "table public.t: DELETE: id[integer]:2", string(xld.WALData))
	xld = rxXLogData()
	assert.Equal(t, "COMMIT", string(xld.WALData[:6]))
}

func TestStartReplicationPhysical(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*50)
	defer cancel()

	conn, err := pgconn.Connect(ctx, os.Getenv("yblogrepl._TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closeConn(t, conn)

	_, err = yblogrepl.CreateReplicationSlot(ctx, conn, slotName, "", yblogrepl.CreateReplicationSlotOptions{Temporary: true, Mode: yblogrepl.PhysicalReplication})
	require.NoError(t, err)

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		config, err := pgconn.ParseConfig(os.Getenv("yblogrepl._TEST_CONN_STRING"))
		require.NoError(t, err)
		delete(config.RuntimeParams, "replication")

		conn, err := pgconn.ConnectConfig(ctx, config)
		require.NoError(t, err)
		defer closeConn(t, conn)

		_, err = conn.Exec(ctx, `
create table mytable(id int primary key, name text);
drop table mytable;
`).ReadAll()
		require.NoError(t, err)
	}()

	_ = func() yblogrepl.PrimaryKeepaliveMessage {
		msg, err := conn.ReceiveMessage(ctx)
		require.NoError(t, err)
		cdMsg, ok := msg.(*pgproto3.CopyData)
		require.True(t, ok)

		require.Equal(t, byte(yblogrepl.PrimaryKeepaliveMessageByteID), cdMsg.Data[0])
		pkm, err := yblogrepl.ParsePrimaryKeepaliveMessage(cdMsg.Data[1:])
		require.NoError(t, err)
		return pkm
	}

	rxXLogData := func() *yblogrepl.XLogData {
		msg, err := conn.ReceiveMessage(ctx)
		require.NoError(t, err)
		cdMsg, ok := msg.(*pgproto3.CopyData)
		require.True(t, ok)

		require.Equal(t, byte(yblogrepl.XLogDataByteID), cdMsg.Data[0])
		xld, err := yblogrepl.ParseXLogData(cdMsg.Data[1:])
		require.NoError(t, err)
		return xld
	}

	xld := rxXLogData()
	assert.Contains(t, string(xld.WALData), "mytable")

	copyDoneResult, err := yblogrepl.SendStandbyCopyDone(ctx, conn)
	require.NoError(t, err)
	assert.Nil(t, copyDoneResult)
}

func TestBaseBackup(t *testing.T) {
	// base backup test could take a long time. Therefore it can be disabled.
	envSkipTest := os.Getenv("yblogrepl._SKIP_BASE_BACKUP")
	if envSkipTest != "" {
		skipTest, err := strconv.ParseBool(envSkipTest)
		if err != nil {
			t.Error(err)
		} else if skipTest {
			return
		}
	}

	conn, err := pgconn.Connect(context.Background(), os.Getenv("yblogrepl._TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closeConn(t, conn)

	options := yblogrepl.BaseBackupOptions{
		NoVerifyChecksums: true,
		Progress:          true,
		Label:             "yblogrepl.test",
		Fast:              true,
		WAL:               true,
		NoWait:            true,
		MaxRate:           1024,
		TablespaceMap:     true,
	}
	startRes, err := yblogrepl.StartBaseBackup(context.Background(), conn, options)
	require.NoError(t, err)
	require.GreaterOrEqual(t, startRes.TimelineID, int32(1))

	//Write the tablespaces
	for i := 0; i < len(startRes.Tablespaces)+1; i++ {
		f, err := os.CreateTemp("", fmt.Sprintf("yblogrepl._test_tbs_%d.tar", i))
		require.NoError(t, err)
		err = yblogrepl.NextTableSpace(context.Background(), conn)
		var message pgproto3.BackendMessage
	L:
		for {
			message, err = conn.ReceiveMessage(context.Background())
			require.NoError(t, err)
			switch msg := message.(type) {
			case *pgproto3.CopyData:
				_, err := f.Write(msg.Data)
				require.NoError(t, err)
			case *pgproto3.CopyDone:
				break L
			default:
				t.Errorf("Received unexpected message: %#v\n", msg)
			}
		}
		err = f.Close()
		require.NoError(t, err)
	}

	stopRes, err := yblogrepl.FinishBaseBackup(context.Background(), conn)
	require.NoError(t, err)
	require.Equal(t, startRes.TimelineID, stopRes.TimelineID)
	require.Equal(t, len(stopRes.Tablespaces), 0)
	require.Less(t, uint64(startRes.LSN), uint64(stopRes.LSN))
	_, err = yblogrepl.StartBaseBackup(context.Background(), conn, options)
	require.NoError(t, err)
}

func TestSendStandbyStatusUpdate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	conn, err := pgconn.Connect(ctx, os.Getenv("yblogrepl._TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closeConn(t, conn)
}
