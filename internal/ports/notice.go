package ports

import "github.com/bnema/vev/internal/domain"

// NoticeStore persists undeliverable notices across daemon restarts.
type NoticeStore interface {
	Append(n domain.Notification) error
	// Claim atomically reserves pending notices for import. Repeating Claim on
	// the owning store replays its live claim; another store cannot replay or
	// acknowledge it. The claim remains recoverable after the claimant process
	// crashes, until its owning store calls Ack.
	Claim() ([]domain.Notification, error)
	// Ack permanently removes the notices returned by this store's claim.
	Ack() error
}
