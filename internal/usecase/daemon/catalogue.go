package daemon

import (
	"github.com/bnema/vev/internal/domain"
)

// Catalogue helpers keep persistence optional for ephemeral and focused unit
// tests while ensuring every durable named-session mutation uses the authority
// port when one is configured.
func (d *Daemon) catalogueRecord(name string) (domain.CatalogueRecord, bool, error) {
	if d == nil || d.catalogue == nil {
		return domain.CatalogueRecord{}, false, nil
	}
	return d.catalogue.Record(name)
}

func (d *Daemon) updateCatalogueMetadata(update domain.CatalogueMetadataUpdate) error {
	if d == nil || d.catalogue == nil {
		return nil
	}
	return d.catalogue.UpdateMetadata(update)
}

// persistSessionMetadata serializes authority writes without holding daemon or
// session state locks during I/O. Only a successfully committed newer revision
// subsumes an older caller. Failed revisions retain their rollback until later
// failed mutations have unwound, keeping live metadata at the durable boundary.
func (d *Daemon) persistSessionMetadata(sess *session, version uint64, update domain.CatalogueMetadataUpdate, rollback func() bool) (bool, error) {
	if sess == nil {
		return false, nil
	}
	sess.metadataPersistMu.Lock()
	defer sess.metadataPersistMu.Unlock()

	if sess.metadataDurableVersion >= version {
		return false, nil
	}
	if err := d.updateCatalogueMetadata(update); err != nil {
		if sess.metadataFailedRollbacks == nil {
			sess.metadataFailedRollbacks = make(map[uint64]func() bool)
		}
		sess.metadataFailedRollbacks[version] = rollback
		// A later mutation may have been reserved while authority I/O blocked.
		// It must settle before an older failed mutation can be unwound.
		sess.mu.Lock()
		current := sess.metadataVersion
		sess.mu.Unlock()
		if current > sess.metadataLiveVersion {
			sess.metadataLiveVersion = current
		}
		rollbackRejected := sess.reconcileFailedMetadataLocked()
		return rollbackRejected, err
	}

	sess.metadataDurableVersion = version
	for failedVersion := range sess.metadataFailedRollbacks {
		if failedVersion <= version {
			delete(sess.metadataFailedRollbacks, failedVersion)
		}
	}
	return false, nil
}

// reconcileFailedMetadataLocked unwinds only a contiguous failed suffix. An
// older failure remains pending while a newer mutation is still writing.
// Rollbacks run only after authority I/O has returned and acquire state locks in
// the daemon-before-session order.
func (s *session) reconcileFailedMetadataLocked() bool {
	for s.metadataLiveVersion > s.metadataDurableVersion {
		version := s.metadataLiveVersion
		rollback, failed := s.metadataFailedRollbacks[version]
		if !failed {
			return false
		}
		if rollback != nil && !rollback() {
			return true
		}
		delete(s.metadataFailedRollbacks, version)
		s.metadataLiveVersion--
	}
	return false
}
