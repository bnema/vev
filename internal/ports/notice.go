package ports

import "github.com/bnema/vev/internal/domain"

// NoticeStore persists undeliverable notices across daemon restarts.
type NoticeStore interface {
	Append(n domain.Notification) error
	// Claim atomically reserves pending notices for import. A live claim cannot
	// be replayed or acknowledged by another store; it remains recoverable after
	// the claimant process crashes, until its owning store calls Ack.
	Claim() ([]domain.Notification, error)
	// Ack permanently removes the notices returned by this store's claim.
	Ack() error
}
