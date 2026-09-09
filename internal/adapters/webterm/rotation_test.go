package webterm

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

func TestRotationRevokesViewsAndCookies(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	token, err := NewToken()
	require.NoError(t, err)
	h, err := NewServer(ctx, token, func(ctx context.Context, terminal *Terminal) error {
		if err := terminal.Flush(); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	})
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { r.Host = Address; h.ServeHTTP(w, r) }))
	defer server.Close()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/ws", &websocket.DialOptions{HTTPHeader: http.Header{"Origin": {Origin}, "Cookie": {cookieName + "=" + token}}})
	require.NoError(t, err)
	defer conn.CloseNow()
	conn.SetReadLimit(64 << 20)
	_, _, err = conn.Read(ctx)
	require.NoError(t, err)
	views := func(credential string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, Origin+"/views", nil)
		r.AddCookie(&http.Cookie{Name: cookieName, Value: credential})
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	require.JSONEq(t, `{"active":1}`, views(token).Body.String())
	renewed, err := h.RenewToken()
	require.NoError(t, err)
	require.NotEqual(t, token, renewed)
	require.Equal(t, http.StatusUnauthorized, views(token).Code)
	for {
		_, _, err = conn.Read(ctx)
		if err != nil {
			break
		}
	}
	require.Error(t, err)
	h.Wait()
	require.JSONEq(t, `{"active":0}`, views(renewed).Body.String())
}
