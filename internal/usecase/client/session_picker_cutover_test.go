package client

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// The daemon's SSP action ends the attachment, not an in-attachment modal.
// Exercise the real worker and supervisor so a notice-only or paint-only fix
// cannot pass while the attachment still owns terminal input.
func TestSessionPickerDetachRevokesForegroundBeforePicker(t *testing.T) {
	picker := newAttachTestPicker()
	notices := make(chan LifecycleNotice, 4)
	harness := startAttachHarnessConfig(t, picker, func(cfg *SupervisorConfig) {
		cfg.NotifyLifecycle = func(notice LifecycleNotice) { notices <- notice }
	})
	stream := newSessionTestStream()
	harness.service.setOpenStream(func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		return stream, nil
	})
	picker.commit(sessionTestRequest(true))
	awaitStreamHello(t, &sync.Mutex{}, &stream)
	deliverReadyStream(t, stream)
	awaitAttachedState(t, harness.sup)
	foreground := harness.sup.attachments.authority.foreground()
	require.NotNil(t, foreground)
	stream.deliver(protocol.Detached{Reason: protocol.ReasonDetachToPicker})
	awaitPickerState(t, harness.sup)
	require.Equal(t, LifecycleNoticeDetachToPicker, (<-notices).Kind)
	require.Nil(t, harness.sup.attachments.authority.foreground())
	select {
	case <-foreground.Done():
	default:
		t.Fatal("picker published before attachment foreground drained")
	}
	require.True(t, picker.owns())
	require.Len(t, harness.service.openedRequests(), 1, "picker transfer must not reattach")
	// Reopening is possible only through a fresh client-owned selection.
	next := newSessionTestStream()
	harness.service.setOpenStream(func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		return next, nil
	})
	picker.commit(sessionTestRequest(true))
	awaitStreamHello(t, &sync.Mutex{}, &next)
	deliverReadyStream(t, next)
	awaitAttachedState(t, harness.sup)
	require.NotEqual(t, foreground.Token(), authorityToken(&harness.sup.attachments.authority))
}
