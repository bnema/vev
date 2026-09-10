package webterm

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

func TestHTTPTrustBoundary(t *testing.T) {
	token := strings.Repeat("a", 43)
	server, err := NewServer(t.Context(), Settings{}, token, func(context.Context, *Terminal) error { return nil })
	require.NoError(t, err)
	tests := []struct {
		name, method, path, host, origin, site, credential string
		want                                               int
	}{
		{"shell", "GET", "/", Address, "", "", "", 200},
		{"no auth", "GET", "/health", Address, "", "", "", 401},
		{"authenticated", "GET", "/health", Address, "", "", token, 204},
		{"rebinding", "GET", "/health", "attacker.invalid", "", "", token, 403},
		{"cross origin", "GET", "/health", Address, "https://attacker.invalid", "", token, 403},
		{"cross site", "GET", "/", Address, "", "cross-site", "", 403},
		{"ws requires origin", "GET", "/ws", Address, "", "", token, 403},
		{"health is read only", "POST", "/health", Address, Origin, "", token, 405},
		{"license is read only", "POST", "/icons-license", Address, "", "", "", 405},
		{"uppercase host", "GET", "/health", strings.ToUpper(Address), "", "", token, 204},
		{"trailing dot host", "GET", "/health", "127.0.0.1.:8778", "", "", token, 204},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := httptest.NewRequest(tt.method, Origin+tt.path, nil)
			request.Host = tt.host
			request.Header.Set("Origin", tt.origin)
			request.Header.Set("Sec-Fetch-Site", tt.site)
			if tt.credential != "" {
				request.AddCookie(&http.Cookie{Name: cookieName, Value: tt.credential})
			}
			response := httptest.NewRecorder()
			server.ServeHTTP(response, request)
			require.Equal(t, tt.want, response.Code)
			require.Contains(t, response.Header().Get("Content-Security-Policy"), "frame-ancestors 'none'")
			require.NotContains(t, response.Body.String(), token)
		})
	}
}

func TestLogin(t *testing.T) {
	token := strings.Repeat("b", 43)
	server, err := NewServer(t.Context(), Settings{}, token, func(context.Context, *Terminal) error { return nil })
	require.NoError(t, err)
	for _, valid := range []bool{false, true} {
		t.Run(map[bool]string{false: "invalid", true: "valid"}[valid], func(t *testing.T) {
			supplied := "wrong"
			if valid {
				supplied = token
			}
			request := httptest.NewRequest("POST", Origin+"/login", strings.NewReader(url.Values{"token": {supplied}}.Encode()))
			request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			request.Header.Set("Origin", Origin)
			response := httptest.NewRecorder()
			server.ServeHTTP(response, request)
			if !valid {
				require.Equal(t, 403, response.Code)
				return
			}
			require.Equal(t, 204, response.Code)
			cookies := response.Result().Cookies()
			require.Len(t, cookies, 1)
			require.True(t, cookies[0].HttpOnly)
			require.Equal(t, http.SameSiteStrictMode, cookies[0].SameSite)
		})
	}
}

func TestWebSocketRoundTripAndDetach(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	exited := make(chan struct{})
	token := strings.Repeat("c", 43)
	handler, err := NewServer(ctx, Settings{}, token, func(ctx context.Context, terminal *Terminal) error {
		defer close(exited)
		if _, err := terminal.EnterRaw(); err != nil {
			return err
		}
		data := make([]byte, 1)
		if _, err := io.ReadFull(terminal.In(), data); err != nil {
			return err
		}
		if _, err := terminal.Write(data); err != nil {
			return err
		}
		if err := terminal.Flush(); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	})
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { r.Host = Address; handler.ServeHTTP(w, r) }))
	defer server.Close()
	headers := http.Header{"Origin": {Origin}, "Cookie": {cookieName + "=" + token}}
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/ws", &websocket.DialOptions{HTTPHeader: headers})
	require.NoError(t, err)
	defer conn.CloseNow()
	conn.SetReadLimit(64 << 20)
	_, data, err := conn.Read(ctx)
	require.NoError(t, err)
	require.True(t, json.Valid(data))
	require.NoError(t, conn.Write(ctx, websocket.MessageText, []byte(`{"schemaVersion":1,"type":"text","text":"Z"}`)))
	for {
		_, data, err = conn.Read(ctx)
		require.NoError(t, err)
		if strings.Contains(string(data), `"text":"Z"`) {
			break
		}
	}
	require.NoError(t, conn.CloseNow())
	select {
	case <-exited:
	case <-ctx.Done():
		t.Fatal("attachment did not detach")
	}
	handler.Wait()
}
