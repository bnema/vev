package sessionwire

import (
	"bytes"
	"compress/zlib"
	"errors"
	"io"
	"math"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

// outputCompressionThreshold preserves the small-snapshot compression threshold: small snapshots stay
// on the canonical raw path.
const protoOutputCompressionThreshold = 1024

func helloToWire(message protocol.Hello) (*wire.Hello, error) {
	cols, rows, pixelWidth, pixelHeight, err := geometryToWire(message.Size, message.PixelWidth, message.PixelHeight)
	if err != nil {
		return nil, err
	}
	sessionTarget, err := sessionAttachTargetToWire(message.SessionTarget)
	if err != nil {
		return nil, err
	}
	exact := exactTargetToWire(message.ExactTarget)
	return &wire.Hello{
		Version:                uint32(message.Version),
		Intent:                 uint32(message.Intent),
		ClientId:               append([]byte(nil), message.ClientID[:]...),
		ResumeToken:            message.ResumeToken,
		Name:                   message.Name,
		Cols:                   cols,
		Rows:                   rows,
		PixelWidth:             pixelWidth,
		PixelHeight:            pixelHeight,
		TermEnv:                message.TermEnv,
		Cwd:                    message.Cwd,
		TrueColor:              message.TrueColor,
		MaxOutputInFlight:      uint32(message.MaxOutputInFlight),
		Env:                    append([]string(nil), message.Env...),
		SessionTarget:          sessionTarget,
		EnvironmentPolicy:      uint32(message.EnvironmentPolicy),
		ExactTarget:            exact,
		PreferredTabId:         string(message.PreferredTabID),
		NavigationCapabilities: uint32(message.NavigationCapabilities),
		Remote:                 message.Remote,
		KittyDirectGraphics:    message.KittyDirectGraphics,
	}, nil
}

func geometryToWire(size domain.Size, pixelWidth, pixelHeight int) (uint32, uint32, uint32, uint32, error) {
	for _, value := range []int{size.Cols, size.Rows, pixelWidth, pixelHeight} {
		if value < 0 || value > math.MaxUint16 {
			return 0, 0, 0, 0, errProtoConvertRange
		}
	}
	return uint32(size.Cols), uint32(size.Rows), uint32(pixelWidth), uint32(pixelHeight), nil
}

func helloFromWire(message *wire.Hello) (protocol.Hello, error) {
	var hello protocol.Hello
	if message == nil {
		return hello, errProtoConvertRange
	}
	version, err := mustUint16(message.GetVersion())
	if err != nil {
		return protocol.Hello{}, err
	}
	hello.Version = version
	intent, err := mustUint8(message.GetIntent())
	if err != nil {
		return protocol.Hello{}, err
	}
	hello.Intent = intent
	if len(message.GetClientId()) != len(hello.ClientID) {
		return protocol.Hello{}, errProtoConvertRange
	}
	copy(hello.ClientID[:], message.GetClientId())
	hello.ResumeToken = message.GetResumeToken()
	hello.Name = message.GetName()
	cols, err := mustUint16(message.GetCols())
	if err != nil {
		return protocol.Hello{}, err
	}
	rows, err := mustUint16(message.GetRows())
	if err != nil {
		return protocol.Hello{}, err
	}
	pixelWidth, err := mustUint16(message.GetPixelWidth())
	if err != nil {
		return protocol.Hello{}, err
	}
	pixelHeight, err := mustUint16(message.GetPixelHeight())
	if err != nil {
		return protocol.Hello{}, err
	}
	hello.Size = domain.Size{Cols: int(cols), Rows: int(rows)}
	hello.PixelWidth = int(pixelWidth)
	hello.PixelHeight = int(pixelHeight)
	hello.TermEnv = message.GetTermEnv()
	hello.Cwd = message.GetCwd()
	hello.TrueColor = message.GetTrueColor()
	window, err := mustUint8(message.GetMaxOutputInFlight())
	if err != nil {
		return protocol.Hello{}, err
	}
	hello.MaxOutputInFlight = window
	hello.Env = append([]string(nil), message.GetEnv()...)
	sessionTarget, err := sessionAttachTargetFromWire(message.GetSessionTarget())
	if err != nil {
		return protocol.Hello{}, err
	}
	hello.SessionTarget = sessionTarget
	hello.EnvironmentPolicy, err = enum8[protocol.EnvironmentPolicy](message.GetEnvironmentPolicy())
	if err != nil {
		return protocol.Hello{}, err
	}
	exact, err := exactTargetFromWire(message.GetExactTarget())
	if err != nil {
		return protocol.Hello{}, err
	}
	hello.ExactTarget = exact
	hello.PreferredTabID = domain.TabStableID(message.GetPreferredTabId())
	hello.NavigationCapabilities, err = enum8[protocol.NavigationCapabilities](message.GetNavigationCapabilities())
	if err != nil {
		return protocol.Hello{}, err
	}
	hello.Remote = message.GetRemote()
	hello.KittyDirectGraphics = message.GetKittyDirectGraphics()
	if err := protocol.ValidateHello(hello); err != nil {
		return protocol.Hello{}, err
	}
	return hello, nil
}

func welcomeToWire(message protocol.Welcome) (*wire.Welcome, error) {
	var identity *wire.CommittedRouteIdentity
	if message.CommittedIdentity != nil {
		converted, err := committedIdentityToWire(*message.CommittedIdentity)
		if err != nil {
			return nil, err
		}
		identity = converted
	}
	return &wire.Welcome{
		SessionId:         message.SessionID,
		SessionName:       message.SessionName,
		Ephemeral:         message.Ephemeral,
		ResumeToken:       message.ResumeToken,
		Capabilities:      message.Capabilities,
		CommittedIdentity: identity,
	}, nil
}

func welcomeFromWire(message *wire.Welcome) (protocol.Welcome, error) {
	var welcome protocol.Welcome
	if message == nil {
		return welcome, errProtoConvertRange
	}
	welcome.SessionID = message.GetSessionId()
	welcome.SessionName = message.GetSessionName()
	welcome.Ephemeral = message.GetEphemeral()
	welcome.ResumeToken = message.GetResumeToken()
	welcome.Capabilities = message.GetCapabilities()
	if message.GetCommittedIdentity() != nil {
		identity, err := committedIdentityFromWire(message.GetCommittedIdentity())
		if err != nil {
			return protocol.Welcome{}, err
		}
		welcome.CommittedIdentity = &identity
		if welcome.CommittedIdentity.Target.SessionName != welcome.SessionName || welcome.CommittedIdentity.Ephemeral != welcome.Ephemeral {
			return protocol.Welcome{}, protocol.ErrInvalidRouteWire
		}
	}
	return welcome, nil
}

func committedIdentityToWire(identity protocol.CommittedRouteIdentity) (*wire.CommittedRouteIdentity, error) {
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	return &wire.CommittedRouteIdentity{Target: exactTargetToWire(&identity.Target), Ephemeral: identity.Ephemeral}, nil
}

func committedIdentityFromWire(message *wire.CommittedRouteIdentity) (protocol.CommittedRouteIdentity, error) {
	var identity protocol.CommittedRouteIdentity
	if message == nil {
		return identity, protocol.ErrInvalidRouteWire
	}
	target, err := exactTargetFromWire(message.GetTarget())
	if err != nil || target == nil {
		return protocol.CommittedRouteIdentity{}, protocol.ErrInvalidRouteWire
	}
	identity.Target = *target
	identity.Ephemeral = message.GetEphemeral()
	if err := identity.Validate(); err != nil {
		return protocol.CommittedRouteIdentity{}, err
	}
	return identity, nil
}

func sessionAttachTargetToWire(target *protocol.SessionAttachTarget) (*wire.SessionAttachTarget, error) {
	if target == nil {
		return nil, nil
	}
	if err := target.Validate(); err != nil {
		return nil, err
	}
	message := &wire.SessionAttachTarget{SessionId: string(target.SessionID), LifecycleId: lifecycleToWire(target.LifecycleID), SessionName: target.SessionName, TabId: string(target.TabID), TabRawName: target.TabRawName, TabExpectedCount: uint32(target.TabExpectedCount), Stopped: target.Stopped}
	if target.TabIndex != protocol.NoTabIndex {
		message.TabIndex = proto.Int32(target.TabIndex)
	}
	return message, nil
}

func sessionAttachTargetFromWire(message *wire.SessionAttachTarget) (*protocol.SessionAttachTarget, error) {
	if message == nil {
		return nil, nil
	}
	if message.GetTabExpectedCount() > math.MaxUint16 {
		return nil, protocol.ErrInvalidAttachTarget
	}
	lifecycle, err := lifecycleFromWire(message.GetLifecycleId())
	if err != nil {
		return nil, err
	}
	tabIndex := protocol.NoTabIndex
	if message.TabIndex != nil {
		tabIndex = message.GetTabIndex()
	}
	target := &protocol.SessionAttachTarget{SessionID: domain.SessionID(message.GetSessionId()), LifecycleID: lifecycle, SessionName: message.GetSessionName(), TabID: domain.TabStableID(message.GetTabId()), TabIndex: tabIndex, TabRawName: message.GetTabRawName(), TabExpectedCount: uint16(message.GetTabExpectedCount()), Stopped: message.GetStopped()}
	if err := target.Validate(); err != nil {
		return nil, err
	}
	return target, nil
}

func attachTargetToWire(message protocol.AttachTarget) (*wire.AttachTarget, error) {
	if err := protocol.ValidateAttachTarget(message); err != nil {
		return nil, err
	}
	sessionTarget, err := sessionAttachTargetToWire(message.SessionTarget)
	if err != nil {
		return nil, err
	}
	return &wire.AttachTarget{
		RequestId:         message.RequestID,
		Endpoint:          message.Endpoint,
		Session:           message.Session,
		Intent:            uint32(message.Intent),
		SessionTarget:     sessionTarget,
		EnvironmentPolicy: uint32(message.EnvironmentPolicy),
		ExactTarget:       exactTargetToWire(message.ExactTarget),
		SamePeer:          message.SamePeer,
		PreferredTabId:    string(message.PreferredTabID),
		CauseActionId:     message.CauseActionID,
	}, nil
}

func attachTargetFromWire(message *wire.AttachTarget) (protocol.AttachTarget, error) {
	var target protocol.AttachTarget
	if message == nil {
		return target, protocol.ErrInvalidAttachTarget
	}
	target.RequestID = message.GetRequestId()
	target.Endpoint = message.GetEndpoint()
	target.Session = message.GetSession()
	intent, err := mustUint8(message.GetIntent())
	if err != nil {
		return protocol.AttachTarget{}, protocol.ErrInvalidAttachTarget
	}
	target.Intent = intent
	sessionTarget, err := sessionAttachTargetFromWire(message.GetSessionTarget())
	if err != nil {
		return protocol.AttachTarget{}, protocol.ErrInvalidAttachTarget
	}
	target.SessionTarget = sessionTarget
	target.EnvironmentPolicy, err = enum8[protocol.EnvironmentPolicy](message.GetEnvironmentPolicy())
	if err != nil {
		return protocol.AttachTarget{}, protocol.ErrInvalidAttachTarget
	}
	exact, err := exactTargetFromWire(message.GetExactTarget())
	if err != nil {
		return protocol.AttachTarget{}, protocol.ErrInvalidAttachTarget
	}
	target.ExactTarget = exact
	target.SamePeer = message.GetSamePeer()
	target.PreferredTabID = domain.TabStableID(message.GetPreferredTabId())
	target.CauseActionID = message.GetCauseActionId()
	if err := protocol.ValidateAttachTarget(target); err != nil {
		return protocol.AttachTarget{}, err
	}
	return target, nil
}

func outputToWire(message protocol.Output) (*wire.Output, error) {
	if err := protocol.ValidateOutput(message); err != nil {
		return nil, err
	}
	cols, rows, _, _, err := geometryToWire(message.Size, 0, 0)
	if err != nil {
		return nil, err
	}
	var context *wire.ViewContext
	if message.Context != nil {
		converted, err := viewContextToWire(*message.Context)
		if err != nil {
			return nil, err
		}
		context = converted
	}
	encoding, data, err := compressProtoOutput(message)
	if err != nil {
		return nil, err
	}
	return &wire.Output{
		Epoch:              message.Epoch,
		Base:               message.Base,
		NewState:           message.New,
		Echo:               message.Echo,
		ViewRevision:       message.ViewRevision,
		Cols:               cols,
		Rows:               rows,
		Full:               message.Full,
		Context:            context,
		Encoding:           encoding,
		UncompressedLength: uint64(len(message.Data)),
		Data:               data,
	}, nil
}

var protoOutputCompressorPool = sync.Pool{New: func() any {
	writer, err := zlib.NewWriterLevel(io.Discard, zlib.BestSpeed)
	if err != nil {
		panic(err)
	}
	return writer
}}

// compressProtoOutput keeps the frozen policy: only full snapshots at or
// above the threshold are zlib candidates, retained only when smaller.
// Callers (use cases) always see uncompressed terminal data.
func compressProtoOutput(message protocol.Output) (uint32, []byte, error) {
	if !message.Full || len(message.Data) < protoOutputCompressionThreshold {
		return 0, append([]byte(nil), message.Data...), nil
	}
	var compressed bytes.Buffer
	writer := protoOutputCompressorPool.Get().(*zlib.Writer)
	writer.Reset(&compressed)
	defer func() {
		writer.Reset(io.Discard)
		protoOutputCompressorPool.Put(writer)
	}()
	if _, err := writer.Write(message.Data); err != nil {
		return 0, nil, err
	}
	if err := writer.Close(); err != nil {
		return 0, nil, err
	}
	if compressed.Len()+5 >= len(message.Data) {
		return 0, append([]byte(nil), message.Data...), nil
	}
	return 1, compressed.Bytes(), nil
}

func outputFromWire(message *wire.Output) (protocol.Output, error) {
	var output protocol.Output
	if message == nil {
		return output, protocol.ErrInvalidOutput
	}
	output.Epoch = message.GetEpoch()
	output.Base = message.GetBase()
	output.New = message.GetNewState()
	output.Echo = message.GetEcho()
	output.ViewRevision = message.GetViewRevision()
	cols, err := mustUint16(message.GetCols())
	if err != nil {
		return protocol.Output{}, protocol.ErrInvalidOutput
	}
	rows, err := mustUint16(message.GetRows())
	if err != nil {
		return protocol.Output{}, protocol.ErrInvalidOutput
	}
	output.Size = domain.Size{Cols: int(cols), Rows: int(rows)}
	output.Full = message.GetFull()
	if message.GetContext() != nil {
		context, err := viewContextFromWire(message.GetContext())
		if err != nil {
			return protocol.Output{}, protocol.ErrInvalidOutput
		}
		output.Context = &context
	}
	if err := protocol.ValidateOutput(output); err != nil {
		return protocol.Output{}, err
	}
	decodedLen := message.GetUncompressedLength()
	if decodedLen > uint64(protocol.MaxOutputDataLen) {
		return protocol.Output{}, protocol.ErrInvalidOutput
	}
	switch message.GetEncoding() {
	case 0:
		if uint64(len(message.GetData())) != decodedLen {
			return protocol.Output{}, protocol.ErrInvalidOutput
		}
		output.Data = append([]byte(nil), message.GetData()...)
	case 1:
		if !output.Full {
			return protocol.Output{}, protocol.ErrInvalidOutput
		}
		decoded, err := decompressProtoOutput(message.GetData(), int(decodedLen))
		if err != nil {
			return protocol.Output{}, protocol.ErrInvalidOutput
		}
		output.Data = decoded
	default:
		return protocol.Output{}, protocol.ErrInvalidOutput
	}
	if err := protocol.ValidateOutput(output); err != nil {
		return protocol.Output{}, err
	}
	return output, nil
}

func decompressProtoOutput(data []byte, decodedLen int) ([]byte, error) {
	source := bytes.NewReader(data)
	reader, err := zlib.NewReader(source)
	if err != nil {
		return nil, err
	}
	decoded := make([]byte, decodedLen)
	if _, err := io.ReadFull(reader, decoded); err != nil {
		_ = reader.Close()
		return nil, err
	}
	var extra [1]byte
	if n, err := reader.Read(extra[:]); n != 0 || !errors.Is(err, io.EOF) {
		_ = reader.Close()
		if err == nil || errors.Is(err, io.EOF) {
			return nil, errors.New("sessionwire: compressed output exceeds declared length")
		}
		return nil, err
	}
	if err := reader.Close(); err != nil {
		return nil, err
	}
	// The zlib reader stops at the stream end without consuming
	// appended bytes, so trailing garbage must be rejected explicitly.
	if source.Len() != 0 {
		return nil, errors.New("sessionwire: compressed output has trailing bytes")
	}
	return decoded, nil
}

func viewContextToWire(context protocol.ViewContext) (*wire.ViewContext, error) {
	if err := context.Validate(); err != nil {
		return nil, err
	}
	route, err := committedIdentityToWire(context.Route)
	if err != nil {
		return nil, err
	}
	return &wire.ViewContext{
		Publication:   context.Publication,
		Route:         route,
		TabId:         string(context.TabID),
		FocusedPaneId: string(context.FocusedPaneID),
	}, nil
}

func viewContextFromWire(message *wire.ViewContext) (protocol.ViewContext, error) {
	var context protocol.ViewContext
	if message == nil {
		return context, protocol.ErrInvalidOutput
	}
	context.Publication = message.GetPublication()
	route, err := committedIdentityFromWire(message.GetRoute())
	if err != nil {
		return protocol.ViewContext{}, err
	}
	context.Route = route
	context.TabID = domain.TabStableID(message.GetTabId())
	context.FocusedPaneID = domain.PaneStableID(message.GetFocusedPaneId())
	if err := context.Validate(); err != nil {
		return protocol.ViewContext{}, err
	}
	return context, nil
}

func commandRequestToWire(message protocol.CommandRequest) (*wire.CommandRequest, error) {
	if len(message.Args) > math.MaxUint16 {
		return nil, errProtoConvertRange
	}
	return &wire.CommandRequest{
		Version:       uint32(message.Version),
		RequestId:     message.RequestID,
		Attached:      message.Attached,
		Self:          message.Self,
		Slug:          message.Slug,
		Args:          append([]string(nil), message.Args...),
		TargetSession: message.TargetSession,
		TargetTab:     message.TargetTab,
		TargetPane:    message.TargetPane,
		Json:          message.JSON,
	}, nil
}

func commandRequestFromWire(message *wire.CommandRequest) (protocol.CommandRequest, error) {
	var request protocol.CommandRequest
	if message == nil {
		return request, errProtoConvertRange
	}
	version, err := mustUint16(message.GetVersion())
	if err != nil {
		return protocol.CommandRequest{}, err
	}
	request.Version = version
	request.RequestID = message.GetRequestId()
	request.Attached = message.GetAttached()
	request.Self = message.GetSelf()
	request.Slug = message.GetSlug()
	request.Args = append([]string(nil), message.GetArgs()...)
	request.TargetSession = message.GetTargetSession()
	request.TargetTab = message.GetTargetTab()
	request.TargetPane = message.GetTargetPane()
	request.JSON = message.GetJson()
	return request, nil
}
