package protocol

import (
	"bytes"
	"testing"
)

func TestParseErrorResponse(t *testing.T) {
	t.Run("ValidErrorResponse", func(t *testing.T) {
		// Construct a raw error response payload
		// 'S' "ERROR" 0 'C' "42601" 0 'M' "syntax error" 0 0
		var buf bytes.Buffer
		buf.WriteByte('S')
		buf.WriteString("ERROR")
		buf.WriteByte(0)
		buf.WriteByte('C')
		buf.WriteString("42601")
		buf.WriteByte(0)
		buf.WriteByte('M')
		buf.WriteString("syntax error at or near \"foo\"")
		buf.WriteByte(0)
		buf.WriteByte(0) // End of fields

		msg := &PGMessage{
			Type: MsgTypeErrorResponse,
			Data: buf.Bytes(),
		}

		resp, err := ParseErrorResponse(msg)
		if err != nil {
			t.Fatalf("ParseErrorResponse failed: %v", err)
		}

		if resp.Severity != "ERROR" {
			t.Errorf("Expected Severity 'ERROR', got '%s'", resp.Severity)
		}
		if resp.Code != "42601" {
			t.Errorf("Expected Code '42601', got '%s'", resp.Code)
		}
		if resp.Message != "syntax error at or near \"foo\"" {
			t.Errorf("Expected Message 'syntax error at or near \"foo\"', got '%s'", resp.Message)
		}
	})

	t.Run("LocalizedSeverityPriority", func(t *testing.T) {
		// 'V' "FATAL" 0 'S' "ФАТАЛЬНАЯ ОШИБКА" 0 ...
		// Parser logic: first 'S' or 'V' found sets Severity if empty.
		// Let's ensure we handle both.
		var buf bytes.Buffer
		buf.WriteByte('V')
		buf.WriteString("FATAL")
		buf.WriteByte(0)
		buf.WriteByte('S')
		buf.WriteString("LOCALIZED_FATAL")
		buf.WriteByte(0)
		buf.WriteByte(0)

		msg := &PGMessage{
			Type: MsgTypeErrorResponse,
			Data: buf.Bytes(),
		}

		resp, err := ParseErrorResponse(msg)
		if err != nil {
			t.Fatalf("ParseErrorResponse failed: %v", err)
		}

		// The implementation picks the first one encountered if Severity is empty.
		// In the loop, V comes first, so Severity becomes FATAL.
		// Then S comes, Severity is not empty, so it stays FATAL.
		// Wait, the existing implementation (and my new one) does:
		// case 'S': if sev == "" { sev = val }
		// case 'V': if sev == "" { sev = val }
		// So order matters.
		if resp.Severity != "FATAL" {
			t.Errorf("Expected Severity 'FATAL', got '%s'", resp.Severity)
		}
	})

	t.Run("EmptyErrorResponse", func(t *testing.T) {
		msg := &PGMessage{
			Type: MsgTypeErrorResponse,
			Data: []byte{0}, // Just terminator
		}

		resp, err := ParseErrorResponse(msg)
		if err != nil {
			t.Fatalf("ParseErrorResponse failed: %v", err)
		}
		if resp.Code != "" || resp.Message != "" {
			t.Error("Expected empty fields for empty error response")
		}
	})

	t.Run("InvalidMessageType", func(t *testing.T) {
		msg := &PGMessage{
			Type: MsgTypeQuery, // Not 'E'
			Data: []byte{},
		}
		_, err := ParseErrorResponse(msg)
		if err == nil {
			t.Error("Expected error for non-ErrorResponse message type")
		}
	})

	t.Run("StringStringer", func(t *testing.T) {
		resp := &ErrorResponse{
			Severity: "ERROR",
			Code:     "12345",
			Message:  "something went wrong",
		}
		str := resp.String()
		expected := "ERROR 12345 something went wrong"
		if str != expected {
			t.Errorf("Expected string '%s', got '%s'", expected, str)
		}
	})
}
