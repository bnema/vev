package client

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

type pickerControl struct{ dialer ports.ClientDialer }

func (c pickerControl) request(ctx context.Context, request protocol.PickerControlRequest) (protocol.PickerControlResponse, error) {
	if c.dialer == nil {
		return protocol.PickerControlResponse{}, errors.New("vev: picker authority unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, inventoryQueryTimeout)
	defer cancel()
	conn, err := c.dialer.Dial(ctx)
	if err != nil {
		return protocol.PickerControlResponse{}, err
	}
	var once sync.Once
	closeConn := func() { once.Do(func() { _ = conn.Close() }) }
	defer closeConn()
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			closeConn()
		case <-done:
		}
	}()
	defer close(done)
	if err := conn.SendClient(request); err != nil {
		return protocol.PickerControlResponse{}, err
	}
	message, err := conn.ReceiveServer()
	if err != nil {
		return protocol.PickerControlResponse{}, err
	}
	response, ok := message.(protocol.PickerControlResponse)
	if !ok || protocol.ValidatePickerControlResponse(response) != nil || response.RequestID != request.RequestID || response.Operation != request.Operation {
		return protocol.PickerControlResponse{}, errors.New("vev: invalid picker authority response")
	}
	return response, nil
}

func (c pickerControl) snapshot(ctx context.Context, requestID, interaction uint64) (protocol.PickerSnapshot, error) {
	response, err := c.request(ctx, protocol.PickerControlRequest{Version: protocol.Version, RequestID: requestID, Operation: protocol.PickerControlSnapshot})
	if err != nil || response.Status != protocol.PickerSourceOK || response.Snapshot == nil {
		return protocol.PickerSnapshot{}, errors.New("vev: picker authority snapshot unavailable")
	}
	snapshot := *response.Snapshot
	snapshot.InteractionID = interaction
	return snapshot, nil
}

func (c pickerControl) observe(ctx context.Context, requestID uint64, targets []protocol.ExactSessionTarget) ([]protocol.PickerRouteObservation, error) {
	response, err := c.request(ctx, protocol.PickerControlRequest{Version: protocol.Version, RequestID: requestID, Operation: protocol.PickerControlObserve, Targets: targets})
	if err != nil {
		return nil, fmt.Errorf("vev: picker authority observation unavailable: %w", err)
	}
	if response.Status != protocol.PickerSourceOK || len(response.Observations) != len(targets) {
		return nil, errors.New("vev: picker authority observation unavailable")
	}
	requested := make(map[protocol.ExactSessionTarget]struct{}, len(targets))
	for _, target := range targets {
		requested[target] = struct{}{}
	}
	for _, observation := range response.Observations {
		if _, ok := requested[observation.Target]; !ok {
			return nil, errors.New("vev: picker authority returned an unexpected observation target")
		}
	}
	return response.Observations, nil
}

func (c pickerControl) resolve(ctx context.Context, requestID uint64, selection protocol.PickerSelection) (protocol.AttachTarget, error) {
	response, err := c.request(ctx, protocol.PickerControlRequest{Version: protocol.Version, RequestID: requestID, Operation: protocol.PickerControlResolve, SourceRevision: selection.SourceRevision, SourceID: selection.SourceID, Key: selection.Key})
	if err != nil || response.Status != protocol.PickerSourceOK || response.Resolved == nil {
		return protocol.AttachTarget{}, errors.New("vev: picker authority rejected selection")
	}
	return *response.Resolved, nil
}
