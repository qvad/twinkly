package log

import (
	"encoding/hex"
	"log"
	"net"

	"github.com/qvad/twinkly/pkg/config"
)

// loggingConn wraps a net.Conn and dumps all bytes written when enabled.
// It does NOT log reads; only data the proxy sends out.
// Name should encode the direction, e.g., "→PostgreSQL", "→YugabyteDB", "→Client".

type loggingConn struct {
	net.Conn
	name    string
	enabled bool
}

func (l *loggingConn) Write(b []byte) (int, error) {
	if l.enabled && len(b) > 0 {
		log.Printf("[TX %s] %d bytes\n%s", l.name, len(b), hex.Dump(b))
	}
	return l.Conn.Write(b)
}

// WrapConn returns a connection that logs all writes when cfg.Debug.DumpNetwork is true.
// It is idempotent: if the provided conn is already a loggingConn, it will not wrap again.
func WrapConn(c net.Conn, name string, cfg *config.Config) net.Conn {
	if cfg != nil && cfg.Debug.DumpNetwork {
		if lc, ok := c.(*loggingConn); ok {
			// Update label/flags if needed, avoid double wrapping
			lc.name = name
			lc.enabled = true
			return lc
		}
		return &loggingConn{Conn: c, name: name, enabled: true}
	}
	return c
}
