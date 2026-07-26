package recovery

import (
	"context"
	"errors"
	"fmt"

	"github.com/bnema/vev/internal/domain"
)

// MarkBroken records a restoration failure only when name still identifies the
// incarnation that failed. It is fenced with Discard so a late restore cannot
// degrade the replacement incarnation.
func (c *Coordinator) MarkBroken(ctx context.Context, name string, expected domain.IncarnationID, reason string) (domain.CatalogueRecord, bool, error) {
	if c == nil || c.catalogue == nil || c.locks == nil {
		return domain.CatalogueRecord{}, false, errors.New("recovery: incomplete broken-session dependencies")
	}
	if err := ctx.Err(); err != nil {
		return domain.CatalogueRecord{}, false, err
	}
	if reason == "" {
		return domain.CatalogueRecord{}, false, errors.New("recovery: empty broken-session reason")
	}
	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()
	unlock := c.locks.Lock([]string{name})
	defer unlock()

	record, ok, err := c.catalogue.Record(name)
	if err != nil {
		return domain.CatalogueRecord{}, false, err
	}
	if !ok || record.IncarnationID != expected {
		return domain.CatalogueRecord{}, false, nil
	}
	if record.DegradedReason == reason {
		return record, true, nil
	}
	record.DegradedReason = reason
	if err := record.Validate(); err != nil {
		return domain.CatalogueRecord{}, false, fmt.Errorf("recovery: invalid broken-session transition: %w", err)
	}
	if err := c.catalogue.Replace(name, record); err != nil {
		return domain.CatalogueRecord{}, false, err
	}
	return record, true, nil
}
