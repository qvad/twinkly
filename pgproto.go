package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
)

// PostgreSQL protocol message types
const (
	// Frontend (client to server) message types
	msgTypeQuery        = 'Q'
	msgTypeParse        = 'P'
	msgTypeBind         = 'B'
	msgTypeExecute      = 'E'
	msgTypeSync         = 'S'
	msgTypeFlush        = 'H'
	msgTypeTerminate    = 'X'
	msgTypeDescribe     = 'D'
	msgTypeClose        = 'C'
	msgTypeFunctionCall = 'F'
	msgTypeCopyData     = 'd'
	msgTypeCopyDone     = 'c'
	msgTypeCopyFail     = 'f'

	// Backend (server to client) message types
	msgTypeRowDescription   = 'T'
	msgTypeDataRow          = 'D'
	msgTypeCommandComplete  = 'C'
	msgTypeErrorResponse    = 'E'
	msgTypeNoticeResponse   = 'N'
	msgTypeAuthentication   = 'R'
	msgTypeParameterStatus  = 'S'
	msgTypeBackendKeyData   = 'K'
	msgTypeReadyForQuery    = 'Z'
	msgTypeParseComplete    = '1'
	msgTypeBindComplete     = '2'
	msgTypeCloseComplete    = '3'
	msgTypePortalSuspended  = 's'
	msgTypeNoData           = 'n'
	msgTypeEmptyQuery       = 'I'
	msgTypeCopyInResponse   = 'G'
	msgTypeCopyOutResponse  = 'H'
	msgTypeCopyBothResponse = 'W'
)

// PGMessage represents a PostgreSQL protocol message
type PGMessage struct {
	Type byte
	Data []byte
}

// PGProtocolReader reads PostgreSQL protocol messages
type PGProtocolReader struct {
	reader io.Reader
}

// NewPGProtocolReader creates a new protocol reader
func NewPGProtocolReader(r io.Reader) *PGProtocolReader {
	return &PGProtocolReader{reader: r}
}

// ReadMessage reads a single PostgreSQL protocol message
func (r *PGProtocolReader) ReadMessage() (*PGMessage, error) {
	// Read message type (1 byte)
	typeBuf := make([]byte, 1)
	if _, err := io.ReadFull(r.reader, typeBuf); err != nil {
		return nil, err
	}

	// Special handling for startup message (no type byte)
	if typeBuf[0] == 0 {
		// This might be part of startup message length
		// Read the rest of the length (3 more bytes)
		lengthBuf := make([]byte, 3)
		if _, err := io.ReadFull(r.reader, lengthBuf); err != nil {
			return nil, err
		}

		// Combine to get full length
		fullLengthBuf := append([]byte{0}, lengthBuf...)
		length := int(binary.BigEndian.Uint32(fullLengthBuf))

		// Read startup message data
		data := make([]byte, length-4)
		if _, err := io.ReadFull(r.reader, data); err != nil {
			return nil, err
		}

		return &PGMessage{
			Type: 0, // Startup message
			Data: data,
		}, nil
	}

	// Read message length (4 bytes)
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(r.reader, lengthBuf); err != nil {
		return nil, err
	}

	length := int(binary.BigEndian.Uint32(lengthBuf))
	if length < 4 {
		return nil, fmt.Errorf("invalid message length: %d", length)
	}

	// Read message data (length includes the 4 length bytes)
	data := make([]byte, length-4)
	if _, err := io.ReadFull(r.reader, data); err != nil {
		return nil, err
	}

	return &PGMessage{
		Type: typeBuf[0],
		Data: data,
	}, nil
}

// PGProtocolWriter writes PostgreSQL protocol messages
type PGProtocolWriter struct {
	writer io.Writer
}

// NewPGProtocolWriter creates a new protocol writer
func NewPGProtocolWriter(w io.Writer) *PGProtocolWriter {
	return &PGProtocolWriter{writer: w}
}

// WriteMessage writes a PostgreSQL protocol message
func (w *PGProtocolWriter) WriteMessage(msg *PGMessage) error {
	var buf bytes.Buffer

	// Write message type (except for startup messages)
	if msg.Type != 0 {
		buf.WriteByte(msg.Type)
	}

	// Write message length (includes 4 bytes for length itself)
	length := len(msg.Data) + 4
	if err := binary.Write(&buf, binary.BigEndian, int32(length)); err != nil {
		return err
	}

	// Write message data
	buf.Write(msg.Data)

	// Send to writer
	_, err := w.writer.Write(buf.Bytes())
	return err
}

// ParseQuery extracts the query string from a Query message
func ParseQuery(msg *PGMessage) (string, error) {
	if msg.Type != msgTypeQuery {
		return "", fmt.Errorf("not a Query message")
	}

	// Query string is null-terminated
	nullIdx := bytes.IndexByte(msg.Data, 0)
	if nullIdx == -1 {
		return string(msg.Data), nil
	}
	return string(msg.Data[:nullIdx]), nil
}

// CreateQueryMessage creates a Query message
func CreateQueryMessage(query string) *PGMessage {
	data := append([]byte(query), 0) // Add null terminator
	return &PGMessage{
		Type: msgTypeQuery,
		Data: data,
	}
}

// CreateErrorMessage creates an Error response message
func CreateErrorMessage(severity, code, message string) *PGMessage {
	var data bytes.Buffer

	// Severity
	data.WriteByte('S')
	data.WriteString(severity)
	data.WriteByte(0)

	// Error code
	data.WriteByte('C')
	data.WriteString(code)
	data.WriteByte(0)

	// Message
	data.WriteByte('M')
	data.WriteString(message)
	data.WriteByte(0)

	// End marker
	data.WriteByte(0)

	return &PGMessage{
		Type: msgTypeErrorResponse,
		Data: data.Bytes(),
	}
}

// IsQueryMessage checks if a message is a query
func IsQueryMessage(msg *PGMessage) bool {
	return msg.Type == msgTypeQuery
}

// IsTerminateMessage checks if a message is a termination request
func IsTerminateMessage(msg *PGMessage) bool {
	return msg.Type == msgTypeTerminate
}

// ExtractParseQuery best-effort extracts SQL text from a Parse (extended protocol) message.
// Parse message payload format: statementName\0 query\0 paramCount(int16) [param OIDs...]
func ExtractParseQuery(msg *PGMessage) (string, error) {
	if msg.Type != msgTypeParse {
		return "", fmt.Errorf("not a Parse message")
	}
	data := msg.Data
	// Find end of statement name (null-terminated)
	i := bytes.IndexByte(data, 0)
	if i == -1 || i+1 >= len(data) {
		return "", fmt.Errorf("malformed Parse message: missing statement name terminator")
	}
	// The query string starts after the first null terminator
	rest := data[i+1:]
	j := bytes.IndexByte(rest, 0)
	if j == -1 {
		// No terminator found; treat entire rest as query
		return string(rest), nil
	}
	return string(rest[:j]), nil
}

// LogMessage logs a protocol message for debugging
func LogMessage(prefix string, msg *PGMessage) {
	switch msg.Type {
	case msgTypeQuery:
		query, _ := ParseQuery(msg)
		log.Printf("%s Query: %s", prefix, query)
	case msgTypeRowDescription:
		log.Printf("%s RowDescription", prefix)
	case msgTypeDataRow:
		log.Printf("%s DataRow", prefix)
	case msgTypeCommandComplete:
		log.Printf("%s CommandComplete", prefix)
	case msgTypeReadyForQuery:
		log.Printf("%s ReadyForQuery", prefix)
	case msgTypeErrorResponse:
		log.Printf("%s ErrorResponse", prefix)
	case 0:
		log.Printf("%s StartupMessage", prefix)
	default:
		log.Printf("%s Message type: %c (%d)", prefix, msg.Type, msg.Type)
	}
}
