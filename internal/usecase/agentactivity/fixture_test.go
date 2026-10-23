package agentactivity

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fixture is a captured pane: screen rows in order and the terminal title.
//
// A .txt file holds a header comment line, a "---" separator, then one line
// per grid row. An optional .title file holds "agent: <title>" for a title
// the agent set or "shell: <title>" for one it did not.
type fixture struct {
	rows           []string
	title          string
	titleFromAgent bool
}

func loadFixture(t testing.TB, agent, name string) fixture {
	t.Helper()
	base := filepath.Join("testdata", agent, name)
	raw, err := os.ReadFile(base + ".txt")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	header, body, ok := strings.Cut(string(raw), "\n---\n")
	if !ok || !strings.HasPrefix(header, "# ") {
		t.Fatalf("%s.txt: missing '# version: description' header and --- separator", base)
	}
	f := fixture{rows: strings.Split(strings.TrimSuffix(body, "\n"), "\n")}
	if raw, err := os.ReadFile(base + ".title"); err == nil {
		source, title, ok := strings.Cut(strings.TrimSpace(string(raw)), ": ")
		if !ok || (source != "agent" && source != "shell") {
			t.Fatalf("%s.title: want 'agent: <title>' or 'shell: <title>'", base)
		}
		f.title, f.titleFromAgent = title, source == "agent"
	} else if !os.IsNotExist(err) {
		t.Fatalf("read title: %v", err)
	}
	return f
}

// window returns the fixture rows the daemon would classify.
func (f fixture) window() []string {
	start, end := Window(len(f.rows), func(y int) bool {
		for _, r := range f.rows[y] {
			if !BlankCell(r, false) {
				return false
			}
		}
		return true
	})
	return f.rows[start:end]
}

// addLine feeds one row of plain text into e.
func addLine(e *Extractor, line string) {
	e.BeginRow()
	for _, c := range line {
		e.WriteCell(c, false)
	}
	e.EndRow()
}

// feed extracts the fixture's window into e the way the daemon does: only the
// rows of the window, cell by cell.
func (f fixture) feed(e *Extractor) Input {
	e.Reset()
	for _, row := range f.window() {
		e.BeginRow()
		for _, r := range row {
			e.WriteCell(r, false)
		}
		e.EndRow()
	}
	return e.Input(f.title, f.titleFromAgent)
}
