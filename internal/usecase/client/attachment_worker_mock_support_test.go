package client

import "fmt"

// String keeps testify from reflecting over a live foreground. Arguments.Diff
// formats every actual argument with "%T=%v", which walks the mutable fields of
// *attachmentForeground while attachmentRun.cleanup is changing them and races.
func (f *attachmentForeground) String() string {
	return fmt.Sprintf("attachmentForeground(%p)", f)
}
