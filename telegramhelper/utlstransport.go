package telegramhelper

import (
	"context"
	"net"
	"net/http"
	"time"

	utls "github.com/refraction-networking/utls"
)

// utlsRoundTripper is an http.RoundTripper that dials TLS connections using
// uTLS with a Chrome fingerprint, replacing Go's identifiable default JA3
// fingerprint with one indistinguishable from a real browser.
type utlsRoundTripper struct {
	inner *http.Transport
}

// RoundTrip implements http.RoundTripper. For HTTPS requests it replaces the
// standard TLS handshake with a uTLS Chrome handshake. HTTP requests are
// forwarded to the inner transport unchanged.
func (t *utlsRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return t.inner.RoundTrip(req)
}

// NewValidatorHTTPClient returns an *http.Client configured with a uTLS
// transport that presents a Chrome browser TLS fingerprint. Drop-in
// replacement for &http.Client{Timeout: ...}.
func NewValidatorHTTPClient(timeout time.Duration) *http.Client {
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	transport := &http.Transport{
		DialContext: dialer.DialContext,
		DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, _, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}

			conn, err := dialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}

			// Restrict ALPN to HTTP/1.1 only. HelloChrome_Auto normally
		// advertises h2, but the http.Transport with a custom DialTLSContext
		// does not set up the HTTP/2 framing layer — so if the server
		// negotiates h2 the connection breaks with a framing error.
		tlsConn := utls.UClient(conn, &utls.Config{
				ServerName:         host,
				InsecureSkipVerify: false,
				NextProtos:         []string{"http/1.1"},
			}, utls.HelloChrome_Auto)

			if err := tlsConn.HandshakeContext(ctx); err != nil {
				conn.Close()
				return nil, err
			}
			return tlsConn, nil
		},
		MaxIdleConns:          10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	return &http.Client{
		Transport: &utlsRoundTripper{inner: transport},
		Timeout:   timeout,
	}
}

// Ensure utlsRoundTripper satisfies the interface at compile time.
var _ http.RoundTripper = (*utlsRoundTripper)(nil)

