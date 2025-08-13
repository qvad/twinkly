package main

import "log"

// proxyMessage forwards a non-query message. For extended protocol, avoid blocking reads
// until Sync to prevent deadlocks where the server waits for Sync before replying.
func (p *DualExecutionProxy) proxyMessage(msg *PGMessage, clientWriter *PGProtocolWriter,
	dbWriter *PGProtocolWriter, dbReader *PGProtocolReader) {
	// Always forward the message to the selected backend first
	if err := dbWriter.WriteMessage(msg); err != nil {
		log.Printf("Failed to send message: %v", err)
		return
	}

	switch msg.Type {
	case msgTypeSync:
		// After Sync, the backend will flush all outstanding responses and end with ReadyForQuery.
		// Drain and forward everything until ReadyForQuery so the client can progress.
		for {
			response, err := dbReader.ReadMessage()
			if err != nil {
				log.Printf("Failed to read response after Sync: %v", err)
				return
			}
			if err := clientWriter.WriteMessage(response); err != nil {
				log.Printf("Failed to send response to client: %v", err)
				return
			}
			if response.Type == msgTypeReadyForQuery {
				break
			}
		}
	case msgTypeFlush:
		// For Flush, do not block here. Server may send pending responses,
		// which will be drained on subsequent Sync. Avoid reading here to prevent stalls.
		return
	default:
		// For Parse/Bind/Execute/Describe/Close/etc., do not attempt to read now.
		// Many servers may not respond until Sync; reading here can deadlock the proxy.
		return
	}
}
