package app

import (
	"context"
	"fmt"
	"time"

	"github.com/bnema/vev/internal/adapters/brokeripc"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/safedir"
)

const productionBrokerStartupTimeout = 10 * time.Second

// connectExistingBroker dials without ensuring or spawning a broker.
func connectExistingBroker(ctx context.Context) (ports.BrokerService, error) {
	path := brokeripc.SocketPath(productionBrokerLayout().Runtime)
	service, err := brokeripc.NewConnector(path, brokeripc.Config{}).Connect(ctx)
	if err != nil {
		if backendAbsent(err) {
			return nil, absentError(path)
		}
		return nil, err
	}
	if err := awaitBrokerPublication(ctx, service); err != nil {
		_ = service.Close()
		return nil, err
	}
	return service, nil
}

// connectProductionBroker connects to the sole per-user production broker.
// Process election and idle lifetime remain owned by the existing broker
// launcher and broker.Supervisor; callers receive only the semantic façade.
func connectProductionBroker(ctx context.Context) (ports.BrokerService, error) {
	if err := ensureProductionBrokerConfig(); err != nil {
		return nil, err
	}
	// Validate the shared parent before creating broker-owned descendants,
	// especially when a long XDG root maps directly into /tmp.
	if err := safedir.EnsurePrivate(ipc.SocketDir()); err != nil {
		return nil, fmt.Errorf("vev: secure broker runtime parent: %w", err)
	}
	layout := productionBrokerLayout()
	request := brokerStatusRequest{
		layout: layout, socketPath: brokeripc.SocketPath(layout.Runtime),
		deadline: time.Now().Add(productionBrokerStartupTimeout), timeout: productionBrokerStartupTimeout,
		root: layout.Root,
	}
	deps := defaultBrokerStatusDeps()
	deps.spawn = func(ctx context.Context, _ string, _ time.Duration, _ bool) error {
		return spawnProductionBrokerLauncher(ctx)
	}
	if _, err := ensureBrokerReady(ctx, request, deps); err != nil {
		return nil, fmt.Errorf("vev: ensure broker: %w", err)
	}
	service, err := brokeripc.NewConnector(request.socketPath, brokeripc.Config{}).Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("vev: connect broker: %w", err)
	}
	if err := awaitBrokerPublication(ctx, service); err != nil {
		_ = service.Close()
		return nil, fmt.Errorf("vev: await broker publication: %w", err)
	}
	return service, nil
}

func awaitBrokerPublication(ctx context.Context, service ports.BrokerService) error {
	sub, err := service.Subscribe()
	if err != nil {
		return err
	}
	defer sub.Close()
	for {
		snapshot := service.Snapshot()
		if snapshot.Epoch != 0 {
			return snapshot.Validate()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-service.Done():
			return service.Err()
		case <-sub.Changed():
		}
	}
}
