package protocol

import (
	"fmt"
	"strings"
)

// ErrorResponse represents a structured PostgreSQL ErrorResponse message
type ErrorResponse struct {
	Severity string
	Code     string
	Message  string
	Detail   string
	Hint     string
	Position string
}

// ParseErrorResponse parses a PGMessage of type ErrorResponse into a structured ErrorResponse
func ParseErrorResponse(msg *PGMessage) (*ErrorResponse, error) {
	if msg.Type != MsgTypeErrorResponse {
		return nil, fmt.Errorf("message is not an ErrorResponse (type: %c)", msg.Type)
	}

	resp := &ErrorResponse{}
	data := msg.Data
	i := 0

	for i < len(data) {
		if data[i] == 0 { // terminator
			break
		}
		fieldType := data[i]
		i++

		// read cstring
		start := i
		for i < len(data) && data[i] != 0 {
			i++
		}

		var val string
		if start <= len(data) {
			// Don't include the null terminator in the string
			end := i
			if end > len(data) {
				end = len(data)
			}
			val = string(data[start:end])
		}

		// skip null terminator if present
		if i < len(data) && data[i] == 0 {
			i++
		}

		switch fieldType {
		case 'S': // Severity (localized)
			if resp.Severity == "" {
				resp.Severity = val
			}
		case 'V': // Severity (non-localized)
			if resp.Severity == "" {
				resp.Severity = val
			}
		case 'C':
			resp.Code = val
		case 'M':
			resp.Message = val
		case 'D':
			resp.Detail = val
		case 'H':
			resp.Hint = val
		case 'P':
			resp.Position = val
		}
	}

	return resp, nil
}

// String returns a string representation of the ErrorResponse
func (e *ErrorResponse) String() string {
	var parts []string
	if e.Severity != "" {
		parts = append(parts, e.Severity)
	}
	if e.Code != "" {
		parts = append(parts, e.Code)
	}
	if e.Message != "" {
		parts = append(parts, e.Message)
	}
	if len(parts) == 0 {
		return "Unknown ErrorResponse"
	}
	return strings.Join(parts, " ")
}
