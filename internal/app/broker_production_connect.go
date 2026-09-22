package app

import (
	"context"
	"fmt"
	"time"

	"github.com/bnema/vev/internal/adapters/brokeripc"
	"github.com/bnema/vev/internal/ports"
)

const productionBrokerStartupTimeout = 10 * time.Second

// connectProductionBroker connects to the sole per-user production broker.
// Process election and idle lifetime remain owned by the existing broker
// launcher and broker.Supervisor; callers receive only the semantic façade.
func connectProductionBroker(ctx context.Context) (ports.BrokerService, error) {
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
