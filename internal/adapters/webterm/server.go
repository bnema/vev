package webterm

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/bnema/vev-vt/core"
	htmlrenderer "github.com/bnema/vev-vt/html"
	"github.com/bnema/vev-vt/html/browser"
	"github.com/bnema/vev/internal/domain"
	"github.com/coder/websocket"
)

const (
	Address        = "127.0.0.1:8778"
	Origin         = "http://" + Address
	cookieName     = "vev-web-session"
	maxConnections = 8
	maxEventBytes  = 8 << 20
	writeTimeout   = 5 * time.Second
)

// RunTerminal composes a normal client attachment. Returning ends its socket.
type RunTerminal func(context.Context, *Terminal) error

type Server struct {
	settings    Settings
	ctx         context.Context
	token       string
	authCtx     context.Context
	authCancel  context.CancelFunc
	run         RunTerminal
	slots       chan struct{}
	workers     sync.WaitGroup
	mu          sync.Mutex
	stopping    bool
	activeViews int
}

func NewServer(ctx context.Context, settings Settings, token string, run RunTerminal) (*Server, error) {
	settings, err := ParseSettings(settings.Listen, settings.Origin)
	if err != nil {
		return nil, err
	}
	if len(token) < 32 || run == nil {
		return nil, errors.New("webterm: invalid server configuration")
	}
	authCtx, authCancel := context.WithCancel(ctx)
	return &Server{settings: settings, ctx: ctx, token: token, authCtx: authCtx, authCancel: authCancel, run: run, slots: make(chan struct{}, maxConnections)}, nil
}

// Wait closes admission before draining all handlers, including handshakes.
// The owner cancels the server context before calling Wait.
func (s *Server) Wait() {
	s.mu.Lock()
	s.stopping = true
	s.mu.Unlock()
	s.workers.Wait()
}

func (s *Server) admit() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping || s.ctx.Err() != nil {
		return false
	}
	s.workers.Add(1)
	return true
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("Cache-Control", "no-store")
	if r.Host != s.settings.Host() || r.Header.Get("Sec-Fetch-Site") == "cross-site" {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	if origin := r.Header.Get("Origin"); origin != "" && origin != s.settings.Origin {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	switch r.URL.Path {
	case "/":
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(indexHTML))
		return
	case "/icons-license":
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte(iconsLicense))
		return
	case "/app.js":
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
		_, _ = w.Write([]byte(appJS))
		return
	case "/terminal.js":
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
		_, _ = w.Write([]byte(browser.JavaScript()))
		return
	case "/terminal.css":
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
		_, _ = w.Write([]byte(htmlrenderer.Stylesheet() + appCSS))
		return
	case "/login":
		if r.Method != http.MethodPost || r.Header.Get("Origin") != s.settings.Origin {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1024)
		if err := r.ParseForm(); err != nil || !s.valid(r.PostForm.Get("token")) {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		http.SetCookie(w, &http.Cookie{Name: cookieName, Value: r.PostForm.Get("token"), Path: "/", HttpOnly: true, Secure: strings.HasPrefix(s.settings.Origin, "https://"), SameSite: http.SameSiteStrictMode})
		w.WriteHeader(http.StatusNoContent)
		return
	case "/health", "/ws", "/views":
		cookie, err := r.Cookie(cookieName)
		if err != nil || !s.valid(cookie.Value) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path == "/views" {
			s.mu.Lock()
			count := s.activeViews
			s.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(struct {
				Active int `json:"active"`
			}{count})
			return
		}
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Header.Get("Origin") != s.settings.Origin {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		s.serveSocket(w, r)
	default:
		http.NotFound(w, r)
	}
}
func (s *Server) valid(token string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return subtle.ConstantTimeCompare([]byte(token), []byte(s.token)) == 1
}

// Token returns the current credential to the trusted local control boundary.
func (s *Server) Token() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.token
}

// RenewToken revokes existing cookies and attachments without stopping shells.
func (s *Server) RenewToken() (string, error) {
	token, err := NewToken()
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.authCancel()
	s.authCtx, s.authCancel = context.WithCancel(s.ctx)
	s.token = token
	return token, nil
}

func NewToken() (string, error) {
	var data [32]byte
	if _, err := rand.Read(data[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(data[:]), nil
}

func (s *Server) serveSocket(w http.ResponseWriter, r *http.Request) {
	if !s.admit() {
		http.Error(w, "Server stopping", http.StatusServiceUnavailable)
		return
	}
	defer s.workers.Done()
	select {
	case s.slots <- struct{}{}:
	default:
		http.Error(w, "Too many terminals", http.StatusServiceUnavailable)
		return
	}
	defer func() { <-s.slots }()
	// Bind this handshake to the authenticated generation under the same lock
	// as rotation. A rotated credential cannot open a new-generation socket.
	s.mu.Lock()
	cookie, cookieErr := r.Cookie(cookieName)
	if cookieErr != nil || subtle.ConstantTimeCompare([]byte(cookie.Value), []byte(s.token)) != 1 {
		s.mu.Unlock()
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	ctx, cancel := context.WithCancel(s.authCtx)
	s.mu.Unlock()
	defer cancel()
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer conn.CloseNow()
	s.mu.Lock()
	s.activeViews++
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.activeViews--; s.mu.Unlock() }()
	conn.SetReadLimit(maxEventBytes)
	stop := context.AfterFunc(ctx, func() { _ = conn.CloseNow() })
	defer stop()
	terminal, err := New(ctx, domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}})
	if err != nil {
		return
	}
	defer terminal.Close()
	done := make(chan error, 2)
	go func() { done <- s.run(ctx, terminal) }()
	go func() { done <- readEvents(ctx, conn, terminal) }()
	err = writeUpdates(ctx, conn, terminal, done)
	cancel()
	terminal.Close()
	// writeUpdates consumes at most one worker result.
	remaining := 2
	var ended *workerEnded
	if errors.As(err, &ended) {
		remaining--
	}
	for range remaining {
		<-done
	}
}

type workerEnded struct{ err error }

func (e *workerEnded) Error() string { return "webterm: attachment ended" }

func readEvents(ctx context.Context, conn *websocket.Conn, terminal *Terminal) error {
	for {
		kind, data, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		if kind != websocket.MessageText {
			return errors.New("webterm: expected text event")
		}
		event, err := browser.DecodeEvent(data, browser.EventLimits{MaxColumns: MaxColumns, MaxRows: MaxRows})
		if err != nil {
			return err
		}
		if err := terminal.Handle(ctx, event); err != nil {
			return err
		}
	}
}

func writeUpdates(ctx context.Context, conn *websocket.Conn, terminal *Terminal, done <-chan error) error {
	renderer, err := htmlrenderer.New(htmlrenderer.Options{})
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-done:
			return &workerEnded{err: err}
		case <-terminal.Changes():
		}
		snapshot := terminal.Snapshot()
		frame := core.NewFrame(snapshot.Columns(), snapshot.Rows())
		for y := range snapshot.Rows() {
			frame.WriteRow(y, 0, snapshot.Row(y))
		}
		cursor := snapshot.Cursor()
		prepared, err := renderer.Prepare(frame, nil, false, htmlrenderer.Cursor{Row: cursor.Row, Column: cursor.Col, Visible: cursor.Visible, Style: htmlrenderer.CursorStyle(cursor.Style), StyleSet: cursor.StyleSet})
		if err != nil {
			return fmt.Errorf("webterm: prepare: %w", err)
		}
		payload, err := json.Marshal(struct {
			Update json.RawMessage `json:"update"`
			Mouse  bool            `json:"mouse"`
		}{Update: prepared.JSON(), Mouse: snapshot.Modes().MouseTracking != 0})
		if err != nil {
			_ = prepared.Abort()
			return err
		}
		writeCtx, cancel := context.WithTimeout(ctx, writeTimeout)
		err = conn.Write(writeCtx, websocket.MessageText, payload)
		cancel()
		if err != nil {
			_ = prepared.Abort()
			return err
		}
		if err := prepared.Commit(); err != nil {
			return err
		}
	}
}
