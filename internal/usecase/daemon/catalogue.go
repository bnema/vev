package daemon

import (
	"github.com/bnema/vev/internal/domain"
)

// Catalogue helpers keep persistence optional for ephemeral and focused unit
// tests while ensuring every durable named-session mutation uses the authority
// port when one is configured.
func (d *Daemon) catalogueRecord(name string) (domain.CatalogueRecord, bool) {
	if d == nil || d.catalogue == nil {
		return domain.CatalogueRecord{}, false
	}
	return d.catalogue.Record(name)
}

func (d *Daemon) createCatalogueRecord(record domain.CatalogueRecord) error {
	if d == nil || d.catalogue == nil {
		return nil
	}
	return d.catalogue.Create(record)
}

func (d *Daemon) updateCatalogueMetadata(update domain.CatalogueMetadataUpdate) error {
	if d == nil || d.catalogue == nil {
		return nil
	}
	return d.catalogue.UpdateMetadata(update)
}

func (d *Daemon) deleteCatalogueRecord(name string) error {
	if d == nil || d.catalogue == nil {
		return nil
	}
	return d.catalogue.Delete(name)
}
