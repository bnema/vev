package app

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/bnema/vev/internal/adapters/webterm"
	"github.com/bnema/vev/internal/platform"
)

// Linux abstract sockets have no filesystem artifact. Peer credentials, not
// the discoverability of this name, protect the local control interface.
func webControlAddress() string {
	path, _ := filepath.Abs(platform.StateDir())
	return fmt.Sprintf("@vev-web-%d-%x", os.Getuid(), sha256.Sum256([]byte(path)))
}

func sameWebUID(conn *net.UnixConn) bool {
	raw, err := conn.SyscallConn()
	if err != nil {
		return false
	}
	var credential *syscall.Ucred
	var peerErr error
	err = raw.Control(func(fd uintptr) {
		credential, peerErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	})
	return err == nil && peerErr == nil && credential != nil && credential.Uid == uint32(os.Getuid())
}

type webAccess struct {
	Token    string           `json:"token"`
	Settings webterm.Settings `json:"settings"`
}

func startWebControl(ctx context.Context, server *webterm.Server, settings webterm.Settings) (*net.UnixListener, error) {
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: webControlAddress(), Net: "unix"})
	if err != nil {
		return nil, err
	}
	go func() {
		stop := context.AfterFunc(ctx, func() { _ = listener.Close() })
		defer stop()
		for {
			conn, err := listener.AcceptUnix()
			if err != nil {
				return
			}
			// Serialize tiny bounded control requests. No unbounded per-peer workers.
			func() {
				defer conn.Close()
				if !sameWebUID(conn) {
					return
				}
				_ = conn.SetDeadline(time.Now().Add(time.Second))
				var command [1]byte
				if _, err := io.ReadFull(conn, command[:]); err != nil {
					return
				}
				var token string
				switch command[0] {
				case 'G':
					token = server.Token()
				case 'R':
					token, err = server.RenewToken()
					if err != nil {
						return
					}
				default:
					return
				}
				_ = json.NewEncoder(conn).Encode(webAccess{Token: token, Settings: settings})
			}()
		}
	}()
	return listener, nil
}

func webControlRequest(ctx context.Context, renew bool) (webAccess, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", webControlAddress())
	if err != nil {
		return webAccess{}, err
	}
	defer conn.Close()
	if !sameWebUID(conn.(*net.UnixConn)) {
		return webAccess{}, errors.New("vev: unsafe web control peer")
	}
	deadline, _ := ctx.Deadline()
	_ = conn.SetDeadline(deadline)
	command := byte('G')
	if renew {
		command = 'R'
	}
	if _, err := conn.Write([]byte{command}); err != nil {
		return webAccess{}, err
	}
	data, err := io.ReadAll(io.LimitReader(conn, 4097))
	if err != nil {
		return webAccess{}, err
	}
	var access webAccess
	if len(data) > 4096 || json.Unmarshal(data, &access) != nil {
		return webAccess{}, errors.New("vev: invalid web control response")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(access.Token)
	settings, settingsErr := webterm.ParseSettings(access.Settings.Listen, access.Settings.Origin)
	if err != nil || len(decoded) != 32 || settingsErr != nil || settings != access.Settings {
		return webAccess{}, errors.New("vev: invalid web control response")
	}
	return access, nil
}
