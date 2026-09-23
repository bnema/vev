package client

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

func TestPickerShortDetails(t *testing.T) {
	tabs := func(n int) []catalogue.RemoteCatalogTab { return make([]catalogue.RemoteCatalogTab, n) }
	sessions := []struct {
		name    string
		session catalogue.RemoteCatalogSession
		want    string
	}{
		{name: "no tabs", session: catalogue.RemoteCatalogSession{}, want: ""},
		{name: "tab count", session: catalogue.RemoteCatalogSession{Tabs: tabs(2)}, want: "2"},
		{name: "attached with tabs", session: catalogue.RemoteCatalogSession{Attached: true, Tabs: tabs(2)}, want: "* 2"},
		{name: "attached without tabs", session: catalogue.RemoteCatalogSession{Attached: true}, want: "*"},
	}
	for _, tt := range sessions {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, pickerSessionDetail(tt.session))
		})
	}
	require.Equal(t, "…", pickerHostDetail(ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityUnknown}))
	require.Equal(t, "", pickerHostDetail(ports.BrokerDaemonObservation{InventoryKnown: true}))
}
