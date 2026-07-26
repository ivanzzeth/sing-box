package sniff

import (
	"context"
	"crypto/tls"
	"errors"
	"io"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing/common/bufio"
	E "github.com/sagernet/sing/common/exceptions"
)

// maxCapturedClientHello bounds what we retain for fingerprinting. A real
// ClientHello is one or two KiB; anything past this is not a handshake we need
// to describe, and the cap keeps a hostile peer from making us buffer.
const maxCapturedClientHello = 8 << 10

// captureReader tees what the TLS parser consumes, so the raw ClientHello can be
// fingerprinted afterwards without reading the connection twice.
type captureReader struct {
	r   io.Reader
	buf []byte
}

func (c *captureReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > 0 && len(c.buf) < maxCapturedClientHello {
		room := maxCapturedClientHello - len(c.buf)
		if n < room {
			room = n
		}
		c.buf = append(c.buf, p[:room]...)
	}
	return n, err
}

func TLSClientHello(ctx context.Context, metadata *adapter.InboundContext, reader io.Reader) error {
	var clientHello *tls.ClientHelloInfo
	capture := &captureReader{r: reader}
	err := tls.Server(bufio.NewReadOnlyConn(capture), &tls.Config{
		GetConfigForClient: func(argHello *tls.ClientHelloInfo) (*tls.Config, error) {
			clientHello = argHello
			return nil, nil
		},
	}).HandshakeContext(ctx)
	if clientHello != nil {
		metadata.Protocol = C.ProtocolTLS
		metadata.Domain = clientHello.ServerName
		metadata.TLSClientHello = capture.buf
		return nil
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return E.Cause1(ErrNeedMoreData, err)
	} else {
		return err
	}
}
