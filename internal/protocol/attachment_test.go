package protocol

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestAttachmentValidation(t *testing.T) {
	target := ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "work"}
	identity := CommittedRouteIdentity{Target: target}
	for _, tt := range []struct {
		name    string
		message interface{ Validate() error }
		valid   bool
	}{
		{"suspend", SuspendAttachment{RequestID: 1}, true},
		{"suspend zero", SuspendAttachment{}, false},
		{"suspended", AttachmentSuspended{RequestID: 1, Target: target}, true},
		{"suspended zero", AttachmentSuspended{Target: target}, false},
		{"suspended target", AttachmentSuspended{RequestID: 1}, false},
		{"activate", ActivateAttachment{RequestID: 2, Target: target, Size: domain.Size{Cols: 80, Rows: 24}}, true},
		{"activate zero", ActivateAttachment{Target: target, Size: domain.Size{Cols: 80, Rows: 24}}, false},
		{"activate target", ActivateAttachment{RequestID: 2, Size: domain.Size{Cols: 80, Rows: 24}}, false},
		{"activate geometry", ActivateAttachment{RequestID: 2, Target: target}, false},
		{"activate pixels", ActivateAttachment{RequestID: 2, Target: target, Size: domain.Size{Cols: 80, Rows: 24}, PixelWidth: 1}, false},
		{"activated", AttachmentActivated{RequestID: 2, Identity: identity, Epoch: 2, State: 1, ViewPublication: 3}, true},
		{"activated zero", AttachmentActivated{Identity: identity, Epoch: 2, State: 1, ViewPublication: 3}, false},
		{"activated identity", AttachmentActivated{RequestID: 2, Epoch: 2, State: 1, ViewPublication: 3}, false},
		{"activated epoch", AttachmentActivated{RequestID: 2, Identity: identity, State: 1, ViewPublication: 3}, false},
		{"activated state", AttachmentActivated{RequestID: 2, Identity: identity, Epoch: 2, ViewPublication: 3}, false},
		{"activated publication", AttachmentActivated{RequestID: 2, Identity: identity, Epoch: 2, State: 1}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.valid {
				require.NoError(t, tt.message.Validate())
			} else {
				require.Error(t, tt.message.Validate())
			}
		})
	}
}
