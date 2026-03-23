package telegramhelper

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"time"

	utls "github.com/refraction-networking/utls"
	"golang.org/x/net/http2"
)

// NewValidatorHTTPClient returns an *http.Client that uses golang.org/x/net/http2.Transport
// with a uTLS Chrome TLS fingerprint. Using http2.Transport directly (rather than
// http.Transport with a custom DialTLSContext) ensures HTTP/2 framing is handled
// correctly when the server negotiates h2 via ALPN — which t.me always does.
// The Chrome fingerprint is preserved because uTLS performs the TLS handshake.
func NewValidatorHTTPClient(timeout time.Duration) *http.Client {
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	h2Transport := &http2.Transport{
		// DialTLSContext is called by http2.Transport for each new HTTPS connection.
		// We perform the TLS handshake ourselves via uTLS so the ClientHello looks
		// like Chrome rather than Go's standard library.
		DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			host, _, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}

			conn, err := dialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}

			tlsConn := utls.UClient(conn, &utls.Config{
				ServerName:         host,
				InsecureSkipVerify: false,
			}, utls.HelloChrome_Auto)

			if err := tlsConn.HandshakeContext(ctx); err != nil {
				conn.Close()
				return nil, err
			}
			return tlsConn, nil
		},
	}

	return &http.Client{
		Transport: h2Transport,
		Timeout:   timeout,
	}
}
