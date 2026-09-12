package protocol

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func pickerObserveTargets(seed, count int) []ExactSessionTarget {
	targets := make([]ExactSessionTarget, 0, count)
	for i := range count {
		targets = append(targets, ExactSessionTarget{
			LifecycleID: domain.SessionLifecycleID{byte(seed + i)},
			SessionName: "work",
		})
	}
	return targets
}

func TestValidatePickerControlObserveRequest(t *testing.T) {
	targets := pickerObserveTargets(1, 2)
	target := targets[0]

	valid := PickerControlRequest{Version: 1, RequestID: 9, Operation: PickerControlObserve, Targets: targets}
	require.NoError(t, ValidatePickerControlRequest(valid))

	// The pre-existing exclusive payloads keep working.
	require.NoError(t, ValidatePickerControlRequest(PickerControlRequest{Version: 1, RequestID: 9, Operation: PickerControlSnapshot}))
	require.NoError(t, ValidatePickerControlRequest(PickerControlRequest{
		Version: 1, RequestID: 9, Operation: PickerControlResolve,
		SourceRevision: 1, SourceID: PickerHomeSourceID, Key: "a/b",
	}))

	atLimit := PickerControlRequest{
		Version: 1, RequestID: 9, Operation: PickerControlObserve,
		Targets: pickerObserveTargets(1, PickerControlMaxTargets),
	}
	require.NoError(t, ValidatePickerControlRequest(atLimit))

	cases := map[string]PickerControlRequest{
		"no targets": {Version: 1, RequestID: 9, Operation: PickerControlObserve},
		"duplicate target": {
			Version: 1, RequestID: 9, Operation: PickerControlObserve,
			Targets: []ExactSessionTarget{target, target},
		},
		"unvalidated target": {
			Version: 1, RequestID: 9, Operation: PickerControlObserve,
			Targets: []ExactSessionTarget{{SessionName: "work"}},
		},
		"over the target bound": {
			Version: 1, RequestID: 9, Operation: PickerControlObserve,
			Targets: pickerObserveTargets(1, PickerControlMaxTargets+1),
		},
		"observe with source identity": {
			Version: 1, RequestID: 9, Operation: PickerControlObserve,
			SourceID: PickerHomeSourceID, Targets: targets,
		},
		"observe with key": {
			Version: 1, RequestID: 9, Operation: PickerControlObserve,
			Key: "a/b", Targets: targets,
		},
		"observe with revision": {
			Version: 1, RequestID: 9, Operation: PickerControlObserve,
			SourceRevision: 2, Targets: targets,
		},
		"snapshot with targets": {Version: 1, RequestID: 9, Operation: PickerControlSnapshot, Targets: targets},
		"resolve with targets": {
			Version: 1, RequestID: 9, Operation: PickerControlResolve,
			SourceRevision: 1, SourceID: PickerHomeSourceID, Key: "a/b", Targets: targets,
		},
		"unknown operation": {Version: 1, RequestID: 9, Operation: 9, Targets: targets},
		"zero version":      {RequestID: 9, Operation: PickerControlObserve, Targets: targets},
		"zero request id":   {Version: 1, Operation: PickerControlObserve, Targets: targets},
	}
	for name, request := range cases {
		t.Run(name, func(t *testing.T) {
			require.ErrorIs(t, ValidatePickerControlRequest(request), ErrInvalidNavigation)
		})
	}
}

func TestValidatePickerControlObserveResponse(t *testing.T) {
	targets := pickerObserveTargets(1, 2)

	valid := PickerControlResponse{
		RequestID: 9, Operation: PickerControlObserve, Status: PickerSourceOK,
		Observations: []PickerRouteObservation{
			{Target: targets[0], Presence: PickerRoutePresent, Attention: true},
			{Target: targets[1], Presence: PickerRouteAbsent},
		},
	}
	require.NoError(t, ValidatePickerControlResponse(valid))

	// A failed observe carries no payload at all.
	require.NoError(t, ValidatePickerControlResponse(PickerControlResponse{
		RequestID: 9, Operation: PickerControlObserve, Status: PickerSourceUnavailable,
	}))

	atLimit := PickerControlResponse{
		RequestID: 9, Operation: PickerControlObserve, Status: PickerSourceOK,
		Observations: pickerObservationsForTest(1, PickerControlMaxTargets, PickerRoutePresent),
	}
	require.NoError(t, ValidatePickerControlResponse(atLimit))

	cases := map[string]PickerControlResponse{
		"no observations": {
			RequestID: 9, Operation: PickerControlObserve, Status: PickerSourceOK,
		},
		"over the observation bound": {
			RequestID: 9, Operation: PickerControlObserve, Status: PickerSourceOK,
			Observations: pickerObservationsForTest(1, PickerControlMaxTargets+1, PickerRoutePresent),
		},
		"duplicate target": {
			RequestID: 9, Operation: PickerControlObserve, Status: PickerSourceOK,
			Observations: []PickerRouteObservation{
				{Target: targets[0], Presence: PickerRoutePresent},
				{Target: targets[0], Presence: PickerRouteAbsent},
			},
		},
		"zero presence": {
			RequestID: 9, Operation: PickerControlObserve, Status: PickerSourceOK,
			Observations: []PickerRouteObservation{{Target: targets[0]}},
		},
		"unknown presence value": {
			RequestID: 9, Operation: PickerControlObserve, Status: PickerSourceOK,
			Observations: []PickerRouteObservation{{Target: targets[0], Presence: 9}},
		},
		"unvalidated target": {
			RequestID: 9, Operation: PickerControlObserve, Status: PickerSourceOK,
			Observations: []PickerRouteObservation{{Target: ExactSessionTarget{SessionName: "work"}, Presence: PickerRoutePresent}},
		},
		"attention on absent": {
			RequestID: 9, Operation: PickerControlObserve, Status: PickerSourceOK,
			Observations: []PickerRouteObservation{{Target: targets[0], Presence: PickerRouteAbsent, Attention: true}},
		},
		"attention on unknown": {
			RequestID: 9, Operation: PickerControlObserve, Status: PickerSourceOK,
			Observations: []PickerRouteObservation{{Target: targets[0], Presence: PickerRouteUnknown, Attention: true}},
		},
		"observe carries snapshot": {
			RequestID: 9, Operation: PickerControlObserve, Status: PickerSourceOK,
			Snapshot:     &PickerSnapshot{},
			Observations: []PickerRouteObservation{{Target: targets[0], Presence: PickerRoutePresent}},
		},
		"observe carries resolved": {
			RequestID: 9, Operation: PickerControlObserve, Status: PickerSourceOK,
			Resolved:     &AttachTarget{},
			Observations: []PickerRouteObservation{{Target: targets[0], Presence: PickerRoutePresent}},
		},
		"failed observe carries observations": {
			RequestID: 9, Operation: PickerControlObserve, Status: PickerSourceUnavailable,
			Observations: []PickerRouteObservation{{Target: targets[0], Presence: PickerRoutePresent}},
		},
		"snapshot carries observations": {
			RequestID: 9, Operation: PickerControlSnapshot, Status: PickerSourceOK,
			Snapshot:     &PickerSnapshot{},
			Observations: []PickerRouteObservation{{Target: targets[0], Presence: PickerRoutePresent}},
		},
		"resolve carries observations": {
			RequestID: 9, Operation: PickerControlResolve, Status: PickerSourceOK,
			Resolved:     &AttachTarget{},
			Observations: []PickerRouteObservation{{Target: targets[0], Presence: PickerRoutePresent}},
		},
	}
	for name, response := range cases {
		t.Run(name, func(t *testing.T) {
			require.ErrorIs(t, ValidatePickerControlResponse(response), ErrInvalidNavigation)
		})
	}
}

func pickerObservationsForTest(seed, count int, presence PickerRoutePresence) []PickerRouteObservation {
	targets := pickerObserveTargets(seed, count)
	observations := make([]PickerRouteObservation, 0, count)
	for _, target := range targets {
		observations = append(observations, PickerRouteObservation{Target: target, Presence: presence})
	}
	return observations
}
