package webterm

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

func TestWebProxyTrustBoundary(t *testing.T) {
	for _, scheme := range []string{"http", "https"} {
		t.Run(scheme, func(t *testing.T) {
			settings, err := ParseSettings("", scheme+"://terminal.example.internal")
			require.NoError(t, err)
			token := strings.Repeat("f", 43)
			handler, err := NewServer(t.Context(), settings, token, func(context.Context, *Terminal) error { return nil })
			require.NoError(t, err)
			for _, tt := range []struct {
				name, host, origin string
				want               int
			}{
				{"valid", settings.Host(), settings.Origin, 204},
				{"upstream host", Address, settings.Origin, 403},
				{"foreign host", "attacker.invalid", settings.Origin, 403},
				{"foreign origin", settings.Host(), "https://attacker.invalid", 403},
				{"missing origin", settings.Host(), "", 403},
			} {
				t.Run(tt.name, func(t *testing.T) {
					r := httptest.NewRequest("POST", "http://"+Address+"/login", strings.NewReader(url.Values{"token": {token}}.Encode()))
					r.Host = tt.host
					r.Header.Set("Origin", tt.origin)
					r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
					r.Header.Set("X-Forwarded-Host", settings.Host())
					r.Header.Set("X-Forwarded-Proto", "https")
					w := httptest.NewRecorder()
					handler.ServeHTTP(w, r)
					require.Equal(t, tt.want, w.Code)
					if tt.want == 204 {
						cookies := w.Result().Cookies()
						require.Len(t, cookies, 1)
						require.Equal(t, scheme == "https", cookies[0].Secure)
						require.True(t, cookies[0].HttpOnly)
						require.Equal(t, http.SameSiteStrictMode, cookies[0].SameSite)
					} else {
						require.Empty(t, w.Result().Cookies())
					}
				})
			}
		})
	}
}

func TestWebProxyWebSocket(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	settings, err := ParseSettings("", "https://terminal.example.internal")
	require.NoError(t, err)
	token := strings.Repeat("g", 43)
	handler, err := NewServer(ctx, settings, token, func(ctx context.Context, terminal *Terminal) error {
		<-ctx.Done()
		return ctx.Err()
	})
	require.NoError(t, err)
	// Model a proxy that preserves the browser-facing Host on HTTP upstream.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Host = settings.Host()
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	headers := http.Header{"Origin": {settings.Origin}, "Cookie": {cookieName + "=" + token}}
	conn, _, err := websocket.Dial(ctx, server.URL+"/ws", &websocket.DialOptions{HTTPHeader: headers})
	require.NoError(t, err)
	conn.CloseNow()
	cancel()
	handler.Wait()
}
