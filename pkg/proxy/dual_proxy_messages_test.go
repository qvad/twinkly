package proxy

import (
	"bytes"
	"testing"

	"github.com/qvad/twinkly/pkg/config"
	"github.com/qvad/twinkly/pkg/protocol"
)

func TestProxyMessage_SyncDrainsUntilReady(t *testing.T) {
	proxy := NewDualExecutionProxy(&config.Config{})
	session := proxy.newSessionState()

	// Simulate backend sending messages via channel
	dbCh := make(chan BackendMessage, 10)
	dbCh <- BackendMessage{Msg: &protocol.PGMessage{Type: protocol.MsgTypeDataRow, Data: []byte{}}}
	dbCh <- BackendMessage{Msg: &protocol.PGMessage{Type: protocol.MsgTypeCommandComplete, Data: []byte{}}}
	dbCh <- BackendMessage{Msg: &protocol.PGMessage{Type: protocol.MsgTypeReadyForQuery, Data: []byte{'I'}}}
	// Close channel to signal end (though loop should break on ReadyForQuery)
	// close(dbCh)

	// Client writer collects forwarded responses
	var clientBuf bytes.Buffer
	clientWriter := protocol.NewPGProtocolWriter(&clientBuf)

	// DB writer just accepts the incoming Sync message
	var dbIncoming bytes.Buffer
	dbWriter := protocol.NewPGProtocolWriter(&dbIncoming)

	// Send a Sync message through the proxy
	syncMsg := &protocol.PGMessage{Type: protocol.MsgTypeSync, Data: []byte{0, 0, 0, 0}}
	session.proxyMessage(syncMsg, clientWriter, dbWriter, dbCh, "DB")

	// Verify backend received the Sync (first byte should be 'S')
	got := dbIncoming.Bytes()
	if len(got) == 0 || got[0] != byte(protocol.MsgTypeSync) {
		t.Fatalf("backend did not receive Sync, got=%v", got)
	}

	// Now parse client buffer and ensure it contains three messages and ends with ReadyForQuery
	reader := protocol.NewPGProtocolReader(&clientBuf)
	var types []byte
	for {
		m, err := reader.ReadMessage()
		if err != nil {
			break
		}
		types = append(types, m.Type)
		if m.Type == protocol.MsgTypeReadyForQuery {
			break
		}
	}
	if len(types) != 3 {
		t.Fatalf("expected 3 messages forwarded to client, got %d (%v)", len(types), types)
	}
	if types[0] != protocol.MsgTypeDataRow || types[1] != protocol.MsgTypeCommandComplete || types[2] != protocol.MsgTypeReadyForQuery {
		t.Errorf("unexpected forwarded sequence: %v", types)
	}
}

func TestProxyMessage_FlushDoesNotRead(t *testing.T) {
	proxy := NewDualExecutionProxy(&config.Config{})
	session := proxy.newSessionState()

	// Channel is empty. If proxyMessage tries to read, it will block/panic (deadlock) if not careful,
	// or return error if closed.
	// Ideally for Flush it should NOT read.
	dbCh := make(chan BackendMessage) // unbuffered, nothing in it

	var clientBuf bytes.Buffer
	clientWriter := protocol.NewPGProtocolWriter(&clientBuf)
	var dbIncoming bytes.Buffer
	dbWriter := protocol.NewPGProtocolWriter(&dbIncoming)

	flush := &protocol.PGMessage{Type: protocol.MsgTypeFlush, Data: []byte{0, 0, 0, 0}}

	// This should return immediately without reading from dbCh
	session.proxyMessage(flush, clientWriter, dbWriter, dbCh, "DB")

	// Ensure client received nothing as a result of Flush handling
	if clientBuf.Len() != 0 {
		t.Fatalf("client should not receive responses on Flush handling, got %d bytes", clientBuf.Len())
	}
	// Ensure backend received the Flush message
	if b := dbIncoming.Bytes(); len(b) == 0 || b[0] != byte(protocol.MsgTypeFlush) {
		t.Fatalf("backend did not receive Flush, got=%v", b)
	}
}
