package helper

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net/http"
	"sync"
	"time"
	"unsafe"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/logger"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/setting/operation_setting"

	"github.com/gin-gonic/gin"
)

const (
	InitialScannerBufferSize    = 64 << 10        // 64KB
	DefaultMaxScannerBufferSize = 64 << 20        // 64MB default SSE buffer size
	DefaultPingInterval         = 10 * time.Second
	MaxPingDuration             = 30 * time.Minute // Safety limit for streaming session
)

// bufPool reuses bytes.Buffer to reduce allocations under high concurrency.
var bufPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

// Precomputed byte slices for SSE parsing - avoids allocation per line.
var (
	doneBytes  = []byte("[DONE]")
	dataPrefix = []byte("data:")
)

func getScannerBufferSize() int {
	if constant.StreamScannerMaxBufferMB > 0 {
		return constant.StreamScannerMaxBufferMB << 20
	}
	return DefaultMaxScannerBufferSize
}

// unsafeString converts []byte to string without allocation.
// SAFETY: The returned string is only valid while the byte slice is not modified.
// This is safe here because dataHandler processes the string immediately.
func unsafeString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// trimLine removes trailing \r\n and leading/trailing whitespace efficiently.
// Returns the trimmed slice (no allocation).
func trimLine(line []byte) []byte {
	// Trim trailing \r\n
	for len(line) > 0 && (line[len(line)-1] == '\n' || line[len(line)-1] == '\r') {
		line = line[:len(line)-1]
	}
	// Trim leading whitespace
	for len(line) > 0 && (line[0] == ' ' || line[0] == '\t') {
		line = line[1:]
	}
	// Trim trailing whitespace
	for len(line) > 0 && (line[len(line)-1] == ' ' || line[len(line)-1] == '\t') {
		line = line[:len(line)-1]
	}
	return line
}

// stripDataPrefix removes "data:" or "data: " prefix if present.
// Returns the data portion (no allocation).
func stripDataPrefix(line []byte) []byte {
	if !bytes.HasPrefix(line, dataPrefix) {
		return line
	}
	line = line[len(dataPrefix):]
	// Strip optional space after "data:"
	if len(line) > 0 && line[0] == ' ' {
		line = line[1:]
	}
	return line
}

// StreamScannerHandler is a lean, single-loop SSE reader/writer. It:
// - Detects [DONE] before slicing so bare markers don't hang the stream.
// - Handles "data:" with or without a space.
// - Uses one goroutine to scan lines and a select loop to write, avoiding per-line goroutines/mutexes.
// - Resets timeout on pings to avoid silent-upstream disconnects.
// - Uses []byte internally and unsafe string conversion to minimize allocations.
func StreamScannerHandler(c *gin.Context, resp *http.Response, info *relaycommon.RelayInfo, dataHandler func(data string) bool) {
	if resp == nil || dataHandler == nil {
		return
	}

	defer func() {
		if resp.Body != nil {
			_ = resp.Body.Close()
		}
	}()

	streamingTimeout := time.Duration(constant.StreamingTimeout) * time.Second
	generalSettings := operation_setting.GetGeneralSetting()
	pingEnabled := generalSettings.PingIntervalEnabled && !info.DisablePing
	pingInterval := time.Duration(generalSettings.PingIntervalSeconds) * time.Second
	if pingInterval <= 0 {
		pingInterval = DefaultPingInterval
	}

	if common.DebugEnabled {
		println("relay timeout seconds:", common.RelayTimeout)
		println("relay max idle conns:", common.RelayMaxIdleConns)
		println("relay max idle conns per host:", common.RelayMaxIdleConnsPerHost)
		println("streaming timeout seconds:", int64(streamingTimeout.Seconds()))
		println("ping interval seconds:", int64(pingInterval.Seconds()))
	}

	SetEventStreamHeaders(c)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Line channel carries raw bytes to avoid string allocation per line.
	lineCh := make(chan []byte, 32)
	errCh := make(chan error, 1)

	// Reader goroutine: uses ReadSlice + pooled buffer for oversized lines.
	go func() {
		defer close(lineCh)

		reader := bufio.NewReaderSize(resp.Body, InitialScannerBufferSize)
		buf := bufPool.Get().(*bytes.Buffer)
		defer func() {
			buf.Reset()
			bufPool.Put(buf)
		}()

		for {
			chunk, err := reader.ReadSlice('\n')

			if err == bufio.ErrBufferFull {
				// Accumulate oversized line; continue reading until newline.
				buf.Write(chunk)
				// Guard against unbounded memory growth
				if buf.Len() > getScannerBufferSize() {
					errCh <- io.ErrShortBuffer
					return
				}
				continue
			}

			if len(chunk) > 0 {
				var line []byte
				if buf.Len() > 0 {
					buf.Write(chunk)
					// Clone to avoid referencing reader's internal buffer
					line = bytes.Clone(buf.Bytes())
					buf.Reset()
				} else {
					// Clone since ReadSlice returns a slice of internal buffer
					line = bytes.Clone(chunk)
				}

				select {
				case <-ctx.Done():
					return
				case <-c.Request.Context().Done():
					return
				case lineCh <- line:
				}
			}

			if err != nil {
				if err != io.EOF {
					errCh <- err
				}
				return
			}
		}
	}()

	// Setup ping ticker - use nil channel pattern to disable in select
	var pingTicker *time.Ticker
	var pingCh <-chan time.Time
	if pingEnabled {
		pingTicker = time.NewTicker(pingInterval)
		defer pingTicker.Stop()
		pingCh = pingTicker.C
	}

	// Setup timeout timer - use nil channel pattern to disable in select
	var timeoutTimer *time.Timer
	var timeoutCh <-chan time.Time
	if streamingTimeout > 0 {
		timeoutTimer = time.NewTimer(streamingTimeout)
		defer timeoutTimer.Stop()
		timeoutCh = timeoutTimer.C
	}

	// Safety limit: max duration for the entire streaming session
	maxDurationTimer := time.NewTimer(MaxPingDuration)
	defer maxDurationTimer.Stop()

	resetTimeout := func() {
		if timeoutTimer == nil {
			return
		}
		if !timeoutTimer.Stop() {
			select {
			case <-timeoutTimer.C:
			default:
			}
		}
		timeoutTimer.Reset(streamingTimeout)
	}

	seenFirst := false

	for {
		select {
		case line, ok := <-lineCh:
			if !ok {
				logger.LogInfo(c, "streaming finished")
				return
			}

			resetTimeout()

			if len(line) == 0 {
				continue
			}

			if common.DebugEnabled {
				println(unsafeString(line))
			}

			// Trim line efficiently (no allocation)
			trimmed := trimLine(line)

			if len(trimmed) == 0 {
				continue
			}

			// Accept bare [DONE]
			if bytes.Equal(trimmed, doneBytes) {
				if common.DebugEnabled {
					println("received [DONE], stopping scanner")
				}
				return
			}

			// Strip "data:" prefix if present (no allocation)
			data := stripDataPrefix(trimmed)

			if len(data) == 0 {
				continue
			}

			// Check for [DONE] after stripping data: prefix
			if bytes.Equal(data, doneBytes) {
				if common.DebugEnabled {
					println("received [DONE], stopping scanner")
				}
				return
			}

			if !seenFirst {
				seenFirst = true
				info.SetFirstResponseTime()
			}

			// Use unsafe string conversion to avoid allocation.
			// Safe because dataHandler processes immediately and doesn't store the string.
			if !dataHandler(unsafeString(data)) {
				return
			}

		case err := <-errCh:
			if err != nil {
				logger.LogError(c, "scanner error: "+err.Error())
			}
			return

		case <-c.Request.Context().Done():
			logger.LogInfo(c, "client disconnected")
			return

		case <-ctx.Done():
			return

		case <-pingCh:
			if err := PingData(c); err != nil {
				logger.LogError(c, "ping data error: "+err.Error())
				return
			}
			if common.DebugEnabled {
				println("ping data sent")
			}
			// Treat ping as activity to avoid timeout on silent upstreams.
			resetTimeout()

		case <-timeoutCh:
			logger.LogError(c, "streaming timeout")
			SendSSEError(c, "streaming_timeout", "no data received within timeout period")
			return

		case <-maxDurationTimer.C:
			logger.LogError(c, "streaming max duration reached")
			return
		}
	}
}
