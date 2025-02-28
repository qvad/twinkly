package protocol

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
	MsgTypeQuery        = 'Q'
	MsgTypeParse        = 'P'
	MsgTypeBind         = 'B'
	MsgTypeExecute      = 'E'
	MsgTypeSync         = 'S'
	MsgTypeFlush        = 'H'
	MsgTypeTerminate    = 'X'
	MsgTypeDescribe     = 'D'
	MsgTypeClose        = 'C'
	MsgTypeFunctionCall = 'F'
	MsgTypeCopyData     = 'd'
	MsgTypeCopyDone     = 'c'
	MsgTypeCopyFail     = 'f'

	// Backend (server to client) message types
	MsgTypeRowDescription   = 'T'
	MsgTypeDataRow          = 'D'
	MsgTypeCommandComplete  = 'C'
	MsgTypeErrorResponse    = 'E'
	MsgTypeNoticeResponse   = 'N'
	MsgTypeAuthentication   = 'R'
	MsgTypeParameterStatus  = 'S'
	MsgTypeBackendKeyData   = 'K'
	MsgTypeReadyForQuery    = 'Z'
	MsgTypeParseComplete    = '1'
	MsgTypeBindComplete     = '2'
	MsgTypeCloseComplete    = '3'
	MsgTypePortalSuspended  = 's'
	MsgTypeNoData           = 'n'
	MsgTypeEmptyQuery       = 'I'
	MsgTypeCopyInResponse   = 'G'
	MsgTypeCopyOutResponse  = 'H'
	MsgTypeCopyBothResponse = 'W'

	// SSLRequestCode is the magic number for SSLRequest
	SSLRequestCode = 80877103
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

// ReadMessage reads a single PostgreSQL protocol message from the wire.
//
// PostgreSQL message format:
//   - Message type: 1 byte (ASCII character, e.g., 'Q' for Query)
//   - Message length: 4 bytes (big-endian uint32, includes length field itself)
//   - Message payload: (length - 4) bytes of message-specific data
//
// Special case: Startup messages have no type byte, only length + payload.
// This function assumes all messages have a type byte.
//
// Returns a PGMessage containing the type and payload data, or an error
// if the message is malformed or the connection is broken.
func (r *PGProtocolReader) ReadMessage() (*PGMessage, error) {
	// Read message type (1 byte)
	// This identifies the message type according to PostgreSQL protocol specification
	typeBuf := make([]byte, 1)
	if _, err := io.ReadFull(r.reader, typeBuf); err != nil {
		return nil, err
	}

	// Read message length (4 bytes, big-endian)
	// Length includes the 4 bytes of the length field itself
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(r.reader, lengthBuf); err != nil {
		return nil, err
	}

	length := int(binary.BigEndian.Uint32(lengthBuf))
	if length < 4 {
		return nil, fmt.Errorf("invalid message length: %d", length)
	}

	// Read message data (length includes the 4 length bytes we just read)
	// So actual payload size is (length - 4)
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

// WriteMessage writes a PostgreSQL protocol message to the wire.
//
// Message format written:
//   - Message type: 1 byte (only if msg.Type != 0, to support startup messages)
//   - Message length: 4 bytes big-endian (includes the 4 length bytes)
//   - Message data: variable length payload
//
// For startup messages (Type == 0), only length + data are written.
// For all other messages, the full type + length + data format is used.
//
// The length field always includes itself (4 bytes) plus the payload size.
func (w *PGProtocolWriter) WriteMessage(msg *PGMessage) error {
	var buf bytes.Buffer

	// Write message type (except for startup messages which have Type == 0)
	// Startup messages use a different wire format without the type byte
	if msg.Type != 0 {
		buf.WriteByte(msg.Type)
	}

	// Write message length (includes 4 bytes for length field itself)
	// This follows PostgreSQL protocol: length = payload_size + 4
	length := len(msg.Data) + 4
	if err := binary.Write(&buf, binary.BigEndian, int32(length)); err != nil {
		return err
	}

	// Write message payload data
	buf.Write(msg.Data)

	// Send the complete message to the underlying writer
	_, err := w.writer.Write(buf.Bytes())
	return err
}

// ParseQuery extracts the query string from a Query message
func ParseQuery(msg *PGMessage) (string, error) {
	if msg.Type != MsgTypeQuery {
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
		Type: MsgTypeQuery,
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
		Type: MsgTypeErrorResponse,
		Data: data.Bytes(),
	}
}

// IsQueryMessage checks if a message is a query
func IsQueryMessage(msg *PGMessage) bool {
	return msg.Type == MsgTypeQuery
}

// IsTerminateMessage checks if a message is a termination request
func IsTerminateMessage(msg *PGMessage) bool {
	return msg.Type == MsgTypeTerminate
}

// ExtractParseQuery extracts SQL text from a Parse message (Extended Query Protocol).
//
// Parse message payload format (PostgreSQL Extended Query Protocol):
//   - Statement name: null-terminated string (can be empty)
//   - Query string: null-terminated SQL text
//   - Parameter count: 2 bytes (int16, big-endian)
//   - Parameter type OIDs: array of 4-byte OIDs (one per parameter)
//
// This function extracts only the SQL query string portion, which is used
// for logging and analysis. It performs best-effort parsing and may not
// handle all edge cases of malformed messages.
//
// Returns the SQL query text, or an error if the message format is invalid.
func ExtractParseQuery(msg *PGMessage) (string, error) {
	if msg.Type != MsgTypeParse {
		return "", fmt.Errorf("not a Parse message")
	}
	data := msg.Data

	// Find end of statement name (null-terminated)
	// Statement name comes first in the Parse message payload
	i := bytes.IndexByte(data, 0)
	if i == -1 || i+1 >= len(data) {
		return "", fmt.Errorf("malformed Parse message: missing statement name terminator")
	}

	// The query string starts immediately after the statement name's null terminator
	rest := data[i+1:]
	j := bytes.IndexByte(rest, 0)
	if j == -1 {
		// No terminator found; treat entire rest as query (malformed but recoverable)
		return string(rest), nil
	}

	// Return the query string (everything between first and second null terminators)
	return string(rest[:j]), nil
}

// ExtractErrorCode extracts the SQLSTATE error code from an ErrorResponse message
// Returns the 5-character error code (e.g., "0A000", "42601") or empty string if not found
func ExtractErrorCode(msg *PGMessage) string {
	if msg.Type != MsgTypeErrorResponse {
		return ""
	}
	data := msg.Data
	i := 0
	for i < len(data) {
		fieldType := data[i]
		if fieldType == 0 {
			break
		}
		i++
		// Find null terminator
		end := i
		for end < len(data) && data[end] != 0 {
			end++
		}
		value := string(data[i:end])
		i = end + 1

		if fieldType == 'C' {
			return value // Error code field
		}
	}
	return ""
}

// ExtractErrorMessage extracts the error message from an ErrorResponse message
func ExtractErrorMessage(msg *PGMessage) string {
	if msg.Type != MsgTypeErrorResponse {
		return ""
	}
	data := msg.Data
	i := 0
	for i < len(data) {
		fieldType := data[i]
		if fieldType == 0 {
			break
		}
		i++
		// Find null terminator
		end := i
		for end < len(data) && data[end] != 0 {
			end++
		}
		value := string(data[i:end])
		i = end + 1

		if fieldType == 'M' {
			return value // Message field
		}
	}
	return ""
}

// LogMessage logs a protocol message for debugging
func LogMessage(prefix string, msg *PGMessage) {
	switch msg.Type {
	case MsgTypeQuery:
		query, _ := ParseQuery(msg)
		log.Printf("%s Query: %s", prefix, query)
	case MsgTypeRowDescription:
		log.Printf("%s RowDescription", prefix)
	case MsgTypeDataRow:
		log.Printf("%s DataRow", prefix)
	case MsgTypeCommandComplete:
		log.Printf("%s CommandComplete", prefix)
	case MsgTypeReadyForQuery:
		log.Printf("%s ReadyForQuery", prefix)
	case MsgTypeErrorResponse:
		log.Printf("%s ErrorResponse", prefix)
	case 0:
		log.Printf("%s StartupMessage", prefix)
	default:
		log.Printf("%s Message type: %c (%d)", prefix, msg.Type, msg.Type)
	}
}
