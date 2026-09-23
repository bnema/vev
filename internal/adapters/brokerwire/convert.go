package brokerwire

// Shared semantic converters.
//
// Conversion mirrors the port, domain, and catalogue types losslessly and
// validates every narrowing numeric cast before it happens. Wire uint32
// taxonomy fields never truncate into a smaller semantic enum: values
// whose high bits would alias a legitimate code are refused with
// errConvertRange before any cast. Identity lengths are exact (16-byte
// connection/operation/incarnation/lifecycle values); text fields enforce
// UTF-8, control/bidi, and byte bounds.

import (
	"math"

	"github.com/bnema/vev/internal/adapters/protoconv"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
	"github.com/bnema/vev/internal/protocol/wire"
)

// brokerEnum8 maps shared narrowing failures to the local range sentinel.
func brokerEnum8[T ~uint8](value uint32) (T, error) {
	v, err := protoconv.Uint8[T](value)
	if err != nil {
		return 0, errConvertRange
	}
	return v, nil
}

// brokerEnum16 maps shared narrowing failures to the local range sentinel.
func brokerEnum16[T ~uint16](value uint32) (T, error) {
	v, err := protoconv.Uint16[T](value)
	if err != nil {
		return 0, errConvertRange
	}
	return v, nil
}

func scopeToWire(epoch ports.BrokerEpoch, connection ports.BrokerConnectionID) *wire.BrokerScope {
	id := append([]byte(nil), connection[:]...)
	return &wire.BrokerScope{BrokerEpoch: uint64(epoch), ConnectionId: id}
}

func scopeFromWire(message *wire.BrokerScope) (ports.BrokerEpoch, ports.BrokerConnectionID, error) {
	var connection ports.BrokerConnectionID
	if message == nil {
		return 0, connection, errConvertRange
	}
	if message.GetBrokerEpoch() == 0 {
		return 0, connection, errConvertRange
	}
	raw := message.GetConnectionId()
	if len(raw) != len(connection) {
		return 0, connection, errConvertRange
	}
	copy(connection[:], raw)
	if err := connection.Validate(); err != nil {
		return 0, ports.BrokerConnectionID{}, errConvertRange
	}
	return ports.BrokerEpoch(message.GetBrokerEpoch()), connection, nil
}

func refToWire(epoch ports.BrokerEpoch, connection ports.BrokerConnectionID, stream ports.BrokerStreamID) *wire.BrokerStreamRef {
	return &wire.BrokerStreamRef{Scope: scopeToWire(epoch, connection), StreamId: uint64(stream)}
}

func refFromWire(message *wire.BrokerStreamRef) (ports.BrokerEpoch, ports.BrokerConnectionID, ports.BrokerStreamID, error) {
	var connection ports.BrokerConnectionID
	if message == nil {
		return 0, connection, 0, errConvertRange
	}
	epoch, conn, err := scopeFromWire(message.GetScope())
	if err != nil {
		return 0, connection, 0, err
	}
	stream := ports.BrokerStreamID(message.GetStreamId())
	if err := stream.Validate(); err != nil {
		return 0, connection, 0, errConvertRange
	}
	return epoch, conn, stream, nil
}

func operationToWire(id ports.BrokerOperationID) []byte {
	return append([]byte(nil), id[:]...)
}

func operationFromWire(raw []byte) (ports.BrokerOperationID, error) {
	var id ports.BrokerOperationID
	if len(raw) != len(id) {
		return id, errConvertRange
	}
	copy(id[:], raw)
	if err := id.Validate(); err != nil {
		return ports.BrokerOperationID{}, errConvertRange
	}
	return id, nil
}

func registrationToWire(v domain.RemoteRegistration) *wire.RemoteRegistration {
	return protoconv.BrokerRegistrationToWire(v)
}
func registrationFromWire(v *wire.RemoteRegistration) (domain.RemoteRegistration, error) {
	out, err := protoconv.BrokerRegistrationFromWire(v)
	if err != nil {
		return domain.RemoteRegistration{}, errConvertRange
	}
	return out, nil
}

func exactTargetToWire(v protocol.ExactSessionTarget) *wire.ExactTarget {
	return protoconv.BrokerExactTargetToWire(v)
}

func exactTargetFromWire(v *wire.ExactTarget) (protocol.ExactSessionTarget, error) {
	out, err := protoconv.BrokerExactTargetFromWire(v)
	if err != nil {
		return protocol.ExactSessionTarget{}, errConvertRange
	}
	return out, nil
}

func policyToWire(v ports.BrokerPolicy) *wire.BrokerWirePolicy {
	return protoconv.BrokerPolicyToWire(v)
}

func policyFromWire(v *wire.BrokerWirePolicy) (ports.BrokerPolicy, error) {
	out, err := protoconv.BrokerPolicyFromWire(v)
	if err != nil {
		return ports.BrokerPolicy{}, mapBrokerConversionError(err)
	}
	return out, nil
}

func mapBrokerConversionError(err error) error {
	switch err {
	case protoconv.ErrOutOfRange:
		return errConvertRange
	case protoconv.ErrTooLarge:
		return ErrTooLarge
	default:
		return ErrInvalidMessage
	}
}

func validateBrokerDisplayText(v string, max int) error {
	return mapOptionalBrokerError(protoconv.BrokerDisplayText(v, max))
}

func mapOptionalBrokerError(err error) error {
	if err == nil {
		return nil
	}
	return mapBrokerConversionError(err)
}

// validateBrokerEndpoint enforces the SSH target contract shared by
// AddHost/RemoveHost/OpenStream/snapshot hosts.
func validateBrokerEndpoint(endpoint string) error {
	if err := domain.ValidateRemoteHostTarget(endpoint); err != nil {
		return ErrInvalidMessage
	}
	return nil
}

// validateBrokerOrigin enforces presentation-only display-origin text. It
// may be empty (the endpoint renders instead) but never carries control
// or bidi characters.
func validateBrokerOrigin(origin string) error {
	if origin == "" {
		return nil
	}
	if err := domain.ValidateRemoteDisplayOrigin(origin); err != nil {
		return ErrInvalidMessage
	}
	return nil
}

func errorDetailToWire(detail ErrorDetail) *wire.BrokerErrorDetail {
	return &wire.BrokerErrorDetail{
		Code:          uint32(detail.Code),
		Text:          detail.Text,
		AdmissionCode: detail.AdmissionCode,
		FailureKind:   uint32(detail.FailureKind),
	}
}

func errorDetailFromWire(message *wire.BrokerErrorDetail) (ErrorDetail, error) {
	var detail ErrorDetail
	if message == nil {
		return detail, errConvertRange
	}
	code, err := brokerEnum8[ports.BrokerErrorCode](message.GetCode())
	if err != nil {
		return ErrorDetail{}, err
	}
	if err := code.Validate(); err != nil {
		return ErrorDetail{}, ErrInvalidMessage
	}
	detail.Code = code
	if err := validateBrokerDisplayText(message.GetText(), ports.BrokerMaxErrorBytes); err != nil {
		return ErrorDetail{}, err
	}
	detail.Text = message.GetText()
	admission := message.GetAdmissionCode()
	if admission > MaxBrokerAdmissionCode {
		return ErrorDetail{}, errConvertRange
	}
	detail.AdmissionCode = admission
	kind, err := brokerEnum8[domain.RemoteFailureKind](message.GetFailureKind())
	if err != nil {
		return ErrorDetail{}, err
	}
	if kind > MaxBrokerFailureKind {
		return ErrorDetail{}, ErrInvalidMessage
	}
	detail.FailureKind = kind
	failure := ports.BrokerError{Code: code, Text: detail.Text}
	if err := failure.Validate(); err != nil {
		return ErrorDetail{}, ErrInvalidMessage
	}
	return detail, nil
}

func catalogTabToWire(tab catalogue.RemoteCatalogTab) *wire.BrokerCatalogTab {
	return &wire.BrokerCatalogTab{
		Id:        tab.ID,
		Index:     uint32(tab.Index),
		Name:      tab.Name,
		Detail:    tab.Detail,
		Attention: tab.Attention,
	}
}

func catalogTabFromWire(message *wire.BrokerCatalogTab) (catalogue.RemoteCatalogTab, error) {
	var tab catalogue.RemoteCatalogTab
	if message == nil {
		return tab, errConvertRange
	}
	if len(message.GetId()) > catalogue.RemoteCatalogMaxIDBytes ||
		len(message.GetName()) > catalogue.RemoteCatalogMaxNameBytes ||
		len(message.GetDetail()) > catalogue.RemoteCatalogMaxDetailBytes {
		return catalogue.RemoteCatalogTab{}, ErrTooLarge
	}
	index := message.GetIndex()
	if index >= catalogue.RemoteCatalogMaxTabsPerSess {
		return catalogue.RemoteCatalogTab{}, errConvertRange
	}
	tab = catalogue.RemoteCatalogTab{
		ID:        message.GetId(),
		Index:     uint16(index),
		Name:      message.GetName(),
		Detail:    message.GetDetail(),
		Attention: message.GetAttention(),
	}
	return tab, nil
}

func catalogSessionToWire(session catalogue.RemoteCatalogSession) *wire.BrokerCatalogSession {
	lifecycle := append([]byte(nil), session.LifecycleID[:]...)
	var state uint32
	switch session.State {
	case catalogue.RemoteCatalogSessionUp:
		state = 1
	case catalogue.RemoteCatalogSessionDown:
		state = 2
	case catalogue.RemoteCatalogSessionBroken:
		state = 3
	}
	tabs := make([]*wire.BrokerCatalogTab, 0, len(session.Tabs))
	for _, tab := range session.Tabs {
		tabs = append(tabs, catalogTabToWire(tab))
	}
	out := &wire.BrokerCatalogSession{
		LifecycleId: lifecycle,
		Name:        session.Name,
		State:       state,
		Ephemeral:   session.Ephemeral,
		Attached:    session.Attached,
		LastUsedSeq: session.LastUsedSeq,
		ActiveTabId: session.ActiveTabID,
		Reason:      session.Reason,
	}
	if len(session.Tabs) > 0 || session.Tabs != nil {
		out.Tabs = tabs
	}
	return out
}

func catalogSessionFromWire(message *wire.BrokerCatalogSession) (catalogue.RemoteCatalogSession, error) {
	var session catalogue.RemoteCatalogSession
	if message == nil {
		return session, errConvertRange
	}
	raw := message.GetLifecycleId()
	if len(raw) != len(session.LifecycleID) {
		return catalogue.RemoteCatalogSession{}, errConvertRange
	}
	copy(session.LifecycleID[:], raw)
	var state catalogue.RemoteCatalogSessionState
	switch message.GetState() {
	case 1:
		state = catalogue.RemoteCatalogSessionUp
	case 2:
		state = catalogue.RemoteCatalogSessionDown
	case 3:
		state = catalogue.RemoteCatalogSessionBroken
	default:
		return catalogue.RemoteCatalogSession{}, errConvertRange
	}
	session.State = state
	session.Name = message.GetName()
	session.Ephemeral = message.GetEphemeral()
	session.Attached = message.GetAttached()
	session.LastUsedSeq = message.GetLastUsedSeq()
	session.ActiveTabID = message.GetActiveTabId()
	session.Reason = message.GetReason()
	wireTabs := message.GetTabs()
	// Proto3 repeated fields carry no presence: an absent or empty tab list
	// both decode as nil, and both mean an empty-present list. The catalogue
	// requires a present tabs slice, so a nil field becomes the empty list
	// rather than a conversion failure.
	if len(wireTabs) > catalogue.RemoteCatalogMaxTabsPerSess {
		return catalogue.RemoteCatalogSession{}, ErrTooLarge
	}
	session.Tabs = make([]catalogue.RemoteCatalogTab, 0, len(wireTabs))
	for i, wireTab := range wireTabs {
		tab, err := catalogTabFromWire(wireTab)
		if err != nil {
			return catalogue.RemoteCatalogSession{}, err
		}
		if uint32(tab.Index) != uint32(i) {
			return catalogue.RemoteCatalogSession{}, ErrInvalidMessage
		}
		session.Tabs = append(session.Tabs, tab)
	}
	if err := validateCatalogSession(session); err != nil {
		return catalogue.RemoteCatalogSession{}, err
	}
	return session, nil
}

// validateCatalogSession enforces the exact catalogue contract of one session
// so both encode and decode refuse a session that could not be published.
func validateCatalogSession(session catalogue.RemoteCatalogSession) error {
	catalog := catalogue.RemoteCatalog{
		ProtocolVersion: protocol.Version,
		SchemaVersion:   catalogue.RemoteCatalogSchemaVersion,
		Sessions:        []catalogue.RemoteCatalogSession{session},
	}
	if err := catalogue.ValidateRemoteCatalog(catalog); err != nil {
		return ErrInvalidMessage
	}
	return nil
}

// validateSnapshotDaemonFields enforces the per-daemon snapshot contract:
// endpoint grammar, presentation origin, rank fit, closed availability and
// failure taxonomies, and a syntactically present policy. The full port
// projection rules (identity/incarnation pairing, observed version,
// inventory, local authority) are re-checked by BrokerDaemonObservation
// validation and, for remotes, the durable projection rule.
func validateSnapshotDaemonFields(daemon ports.BrokerDaemonObservation) error {
	if daemon.Local {
		if daemon.Endpoint != "" {
			return ErrInvalidMessage
		}
	} else if err := validateBrokerEndpoint(daemon.Endpoint); err != nil {
		return err
	}
	if err := validateBrokerOrigin(daemon.DisplayOrigin); err != nil {
		return err
	}
	if daemon.Rank < 0 || daemon.Rank > math.MaxUint32 {
		return errConvertRange
	}
	if daemon.Availability < domain.RemoteAvailabilityUnknown ||
		daemon.Availability > domain.RemoteAvailabilityNoDaemon {
		return ErrInvalidMessage
	}
	if daemon.LastFailure.Kind > domain.RemoteFailureInvalidResponse {
		return ErrInvalidMessage
	}
	return nil
}

// checkEnvEntries keeps the local error taxonomy at the adapter boundary.
func checkEnvEntries(env []string) error {
	return mapOptionalBrokerError(protoconv.BrokerEnvEntries(env))
}
