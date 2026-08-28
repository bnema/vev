package protocol

// ClientMessage is a closed set of messages accepted from a session client.
type ClientMessage interface{ clientMessage() }

// ServerMessage is a closed set of messages emitted by a session daemon.
type ServerMessage interface{ serverMessage() }

func (Hello) clientMessage()                      {}
func (Input) clientMessage()                      {}
func (Resize) clientMessage()                     {}
func (Detach) clientMessage()                     {}
func (Ping) clientMessage()                       {}
func (List) clientMessage()                       {}
func (Kill) clientMessage()                       {}
func (Theme) clientMessage()                      {}
func (Ack) clientMessage()                        {}
func (ImagePush) clientMessage()                  {}
func (ClientNotice) clientMessage()               {}
func (CommandRequest) clientMessage()             {}
func (OutputResetRequest) clientMessage()         {}
func (RemotePreviewRequest) clientMessage()       {}
func (RouteAttentionSubscription) clientMessage() {}
func (SamePeerSwitchRequest) clientMessage()      {}
func (ParkedRouteRequest) clientMessage()         {}
func (RecentRouteSnapshot) clientMessage()        {}
func (RouteNavigationFailure) clientMessage()     {}

func (Welcome) serverMessage()                {}
func (ErrorMsg) serverMessage()               {}
func (Output) serverMessage()                 {}
func (Detached) serverMessage()               {}
func (Pong) serverMessage()                   {}
func (Sessions) serverMessage()               {}
func (CommandResult) serverMessage()          {}
func (NavigationDirective) serverMessage()    {}
func (AttachTarget) serverMessage()           {}
func (RemotePreview) serverMessage()          {}
func (CommittedRouteIdentity) serverMessage() {}
func (RouteNavigationAction) serverMessage()  {}
func (RouteNavigationFailure) serverMessage() {}
func (RoutePosition) serverMessage()          {}
func (SamePeerSwitchFailure) serverMessage()  {}
func (ParkedRouteResponse) serverMessage()    {}
