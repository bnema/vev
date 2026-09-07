package term

const (
	altScreenEnter        = "\x1b[?1049h"
	altScreenExit         = "\x1b[?1049l"
	cursorHide            = "\x1b[?25l"
	cursorShow            = "\x1b[?25h"
	cursorStyleDefault    = "\x1b[0 q"
	mouseEnable           = "\x1b[?1002h\x1b[?1006h"
	mouseDisable          = "\x1b[?1002l\x1b[?1006l"
	bracketedPasteEnable  = "\x1b[?2004h"
	bracketedPasteDisable = "\x1b[?2004l"
	colorSchemeEnable     = "\x1b[?2031h"
	colorSchemeDisable    = "\x1b[?2031l"
	autowrapDisable       = "\x1b[?7l"
	autowrapEnable        = "\x1b[?7h"
)

const visualEnter = altScreenEnter + autowrapDisable + cursorHide + mouseEnable + bracketedPasteEnable + colorSchemeEnable

const visualRestore = cursorShow + cursorStyleDefault + mouseDisable +
	bracketedPasteDisable + colorSchemeDisable + autowrapEnable + altScreenExit

// VisualEnterSequence returns a fresh copy of the visual initialization used
// after acquiring a terminal. It intentionally excludes OS raw-mode setup.
func VisualEnterSequence() []byte { return []byte(visualEnter) }

// VisualRestoreSequence returns a fresh copy of the idempotent visual cleanup.
func VisualRestoreSequence() []byte { return []byte(visualRestore) }
