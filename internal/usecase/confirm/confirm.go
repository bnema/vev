package confirm

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

// Confirmer asks a yes/no question on a pair of streams.
type Confirmer struct {
	in  io.Reader
	out io.Writer
}

// NewConfirmer builds a confirmation prompt. Only y/yes answers confirm; empty,
// n/no, and unknown answers all decline.
func NewConfirmer(in io.Reader, out io.Writer) Confirmer {
	return Confirmer{in: in, out: out}
}

// Confirm writes question with a [y/N] default and reads one answer line.
func (c Confirmer) Confirm(question string) (bool, error) {
	if c.out != nil {
		if _, err := fmt.Fprintf(c.out, "%s [y/N] ", question); err != nil {
			return false, err
		}
	}
	answer, err := bufio.NewReader(c.in).ReadString('\n')
	if err != nil && err != io.EOF {
		return false, err
	}
	switch strings.ToLower(strings.TrimSpace(answer)) {
	case "y", "yes":
		return true, nil
	default:
		return false, nil
	}
}
