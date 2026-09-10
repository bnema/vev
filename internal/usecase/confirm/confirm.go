package confirm

import (
	"bufio"
	"context"
	"errors"
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

// ConfirmContext writes question with a [y/N] default and reads one answer
// line, releasing the caller with the cancellation error if ctx ends first.
// The reader goroutine may stay blocked, so use this only where the process
// exits (or detaches from the console) afterward rather than continuing to
// interact on the same stream.
func (c Confirmer) ConfirmContext(ctx context.Context, question string) (bool, error) {
	if ctx == nil {
		return c.Confirm(question)
	}
	type outcome struct {
		ok  bool
		err error
	}
	result := make(chan outcome, 1)
	go func() {
		ok, err := c.Confirm(question)
		result <- outcome{ok: ok, err: err}
	}()
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case reply := <-result:
		return reply.ok, reply.err
	}
}

// Confirm writes question with a [y/N] default and reads one answer line.
func (c Confirmer) Confirm(question string) (bool, error) {
	if c.out != nil {
		if _, err := fmt.Fprintf(c.out, "%s [y/N] ", question); err != nil {
			return false, err
		}
	}
	answer, err := bufio.NewReader(c.in).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return false, err
	}
	switch strings.ToLower(strings.TrimSpace(answer)) {
	case "y", "yes":
		return true, nil
	default:
		return false, nil
	}
}
