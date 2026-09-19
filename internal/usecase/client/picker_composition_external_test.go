package client_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
	"github.com/bnema/vev/internal/usecase/client"
)

// TestCompositionOutsideThePackageCanBuildThePicker proves the composition seam
// works from outside the package. Without it a composition could build the
// supervisor and subscribe to broker publications, but never hand it a picker,
// so it could never resolve a selection into an attachment.
func TestCompositionOutsideThePackageCanBuildThePicker(t *testing.T) {
	geometry := domain.Size{Cols: 80, Rows: 24}
	picker := client.NewPicker(nil, 0)
	require.NotNil(t, picker)

	// The exact assignment an app composition performs.
	cfg := client.SupervisorConfig{Picker: picker}
	require.Same(t, picker, cfg.Picker)

	// Exercise a real local-only publication through the exported facade.
	picker.ApplySnapshot(ports.BrokerSnapshot{
		Epoch:    1,
		Revision: 1,
		Daemons: []ports.BrokerDaemonObservation{{
			Local:           true,
			DisplayOrigin:   "local",
			Policy:          validExternalPickerPolicy(),
			Identity:        "local-daemon",
			Incarnation:     ports.BrokerDaemonIncarnation{1},
			ProtocolVersion: protocol.Version,
			Availability:    domain.RemoteAvailabilityReachable,
			LastSuccess:     time.Now(),
			InventoryKnown:  true,
			Sessions: []catalogue.RemoteCatalogSession{{
				LifecycleID: domain.SessionLifecycleID{1},
				Name:        "work",
				State:       catalogue.RemoteCatalogSessionUp,
			}},
		}},
	})
	require.NotEmpty(t, picker.Render(geometry))
	require.Empty(t, picker.RenderNotice(geometry))
}

func TestPickerZeroValueIsInertOutsideThePackage(t *testing.T) {
	geometry := domain.Size{Cols: 80, Rows: 24}
	zero := &client.Picker{}

	require.NotPanics(t, func() {
		zero.ApplySnapshot(ports.BrokerSnapshot{Epoch: 1, Revision: 1})
		zero.SetOwnsInput(true)
	})
	require.Nil(t, zero.OpsReady())
	require.Empty(t, zero.Render(geometry))
	require.Empty(t, zero.RenderNotice(geometry))

	// An absent optional picker is inert too.
	var absent *client.Picker
	require.Empty(t, absent.Render(geometry))
	require.Empty(t, absent.RenderNotice(geometry))
}

func validExternalPickerPolicy() ports.BrokerPolicy {
	return ports.BrokerPolicy{
		ProtocolVersion:      protocol.Version,
		CatalogSchemaVersion: catalogue.RemoteCatalogSchemaVersion,
		EnvironmentPolicy:    protocol.EnvironmentPolicyDaemonOwned,
		Transport:            "quic",
		Trust:                "trust",
		Launch:               "launch",
		Isolation:            "user",
	}
}
