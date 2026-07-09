package ports

// Bracketed-paste markers bracket terminal paste payloads when bracketed-paste
// mode is enabled. The client coalescer and daemon key router both depend on
// these bytes staying in sync.
var (
	BracketedPasteOpenMarker  = []byte("\x1b[200~")
	BracketedPasteCloseMarker = []byte("\x1b[201~")
)
