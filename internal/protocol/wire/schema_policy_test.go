package wire

import (
	"os"
	"strings"
	"testing"
)

// TestCommandResultV58CleanBreakPolicy pins both halves of the intentional
// v58 clean break: Buf permits field deletion only under strict versioning,
// while the deleted field number and name remain unavailable for reuse.
func TestCommandResultV58CleanBreakPolicy(t *testing.T) {
	buf, err := os.ReadFile("../../../buf.yaml")
	if err != nil {
		t.Fatal(err)
	}
	policy := string(buf)
	for _, want := range []string{
		"ignore_only:",
		"FIELD_NO_DELETE:",
		"- internal/protocol/wire/schema/command.proto",
	} {
		if !strings.Contains(policy, want) {
			t.Fatalf("buf breaking policy must scope the v58 field deletion to command.proto: missing %q", want)
		}
	}
	if strings.Contains(policy, "- FIELD_NO_DELETE") {
		t.Fatal("buf breaking policy must not permit field deletion globally")
	}

	schema, err := os.ReadFile("schema/command.proto")
	if err != nil {
		t.Fatal(err)
	}
	text := string(schema)
	for _, reservation := range []string{"reserved 2;", `reserved "ok";`} {
		if !strings.Contains(text, reservation) {
			t.Fatalf("CommandResult must retain %s", reservation)
		}
	}
}
