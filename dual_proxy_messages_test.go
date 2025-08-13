package main

import (
	"bytes"
	"testing"
)

// countingReader implements io.Reader and counts reads
type countingReader struct{ reads int }

func (c *countingReader) Read(p []byte) (int, error) {
	c.reads++
	return 0, bytes.ErrTooLarge // deterministic non-EOF error if used
}

func TestProxyMessage_SyncDrainsUntilReady(t *testing.T) {
	proxy := NewDualExecutionProxy(&Config{})

	// Prepare backend reader that has two messages and then ReadyForQuery
	var backendBuf bytes.Buffer
	backendWriter := NewPGProtocolWriter(&backendBuf)
	// Simulate some backend chatter: a DataRow, then CommandComplete, then ReadyForQuery
	if err := backendWriter.WriteMessage(&PGMessage{Type: msgTypeDataRow, Data: []byte{}}); err != nil {
		t.Fatalf("prep write: %v", err)
	}
	if err := backendWriter.WriteMessage(&PGMessage{Type: msgTypeCommandComplete, Data: []byte{}}); err != nil {
		t.Fatalf("prep write: %v", err)
	}
	if err := backendWriter.WriteMessage(&PGMessage{Type: msgTypeReadyForQuery, Data: []byte{'I'}}); err != nil {
		t.Fatalf("prep write: %v", err)
	}

	// Client writer collects forwarded responses
	var clientBuf bytes.Buffer
	clientWriter := NewPGProtocolWriter(&clientBuf)

	// DB writer just accepts the incoming Sync message
	var dbIncoming bytes.Buffer
	dbWriter := NewPGProtocolWriter(&dbIncoming)

	dbReader := NewPGProtocolReader(&backendBuf)

	// Send a Sync message through the proxy
	syncMsg := &PGMessage{Type: msgTypeSync, Data: []byte{0, 0, 0, 0}}
	proxy.proxyMessage(syncMsg, clientWriter, dbWriter, dbReader)

	// Verify backend received the Sync (first byte should be 'S')
	got := dbIncoming.Bytes()
	if len(got) == 0 || got[0] != byte(msgTypeSync) {
		t.Fatalf("backend did not receive Sync, got=%v", got)
	}

	// Now parse client buffer and ensure it contains three messages and ends with ReadyForQuery
	reader := NewPGProtocolReader(&clientBuf)
	var types []byte
	for {
		m, err := reader.ReadMessage()
		if err != nil {
			break
		}
		types = append(types, m.Type)
		if m.Type == msgTypeReadyForQuery {
			break
		}
	}
	if len(types) != 3 {
		t.Fatalf("expected 3 messages forwarded to client, got %d (%v)", len(types), types)
	}
	if types[0] != msgTypeDataRow || types[1] != msgTypeCommandComplete || types[2] != msgTypeReadyForQuery {
		t.Errorf("unexpected forwarded sequence: %v", types)
	}
}

func TestProxyMessage_FlushDoesNotRead(t *testing.T) {
	proxy := NewDualExecutionProxy(&Config{})

	// Counting reader will error if ReadMessage is attempted
	cr := &countingReader{}
	dbReader := NewPGProtocolReader(cr)

	var clientBuf bytes.Buffer
	clientWriter := NewPGProtocolWriter(&clientBuf)
	var dbIncoming bytes.Buffer
	dbWriter := NewPGProtocolWriter(&dbIncoming)

	flush := &PGMessage{Type: msgTypeFlush, Data: []byte{0, 0, 0, 0}}
	proxy.proxyMessage(flush, clientWriter, dbWriter, dbReader)

	// Ensure no reads happened from dbReader
	if cr.reads != 0 {
		t.Fatalf("Flush should not trigger backend reads, got %d reads", cr.reads)
	}
	// Ensure client received nothing as a result of Flush handling
	if clientBuf.Len() != 0 {
		t.Fatalf("client should not receive responses on Flush handling, got %d bytes", clientBuf.Len())
	}
	// Ensure backend received the Flush message
	if b := dbIncoming.Bytes(); len(b) == 0 || b[0] != byte(msgTypeFlush) {
		t.Fatalf("backend did not receive Flush, got=%v", b)
	}
}
