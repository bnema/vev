package ports

import "github.com/bnema/vev/internal/domain"

// NoticeStore persists undeliverable notices across daemon restarts.
type NoticeStore interface {
	Append(n domain.Notification) error
	// Claim atomically reserves pending notices for import. A claim remains
	// recoverable until Ack is called, including across process crashes.
	Claim() ([]domain.Notification, error)
	// Ack permanently removes the notices returned by the current claim.
	Ack() error
}
