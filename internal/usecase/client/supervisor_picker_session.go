package client

// pickerSession groups the picker state the supervisor carries across its run:
// the commit observed while connecting, the selected row's preview
// subscription, and the runner for the picker's kill operations. Like the
// fields it owns, it is only touched from the run goroutine. The zero value is
// ready once preview has its clock.
type pickerSession struct {
	// pendingPickerKey retains a commit observed while connecting. It is
	// revalidated against the adopted service just like a ready-phase commit.
	pendingPickerKey string
	preview          previewManager
	// kills runs the picker's `x` operations off the run goroutine.
	kills pickerKills
}
