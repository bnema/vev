package ports

import "github.com/bnema/vev/internal/domain"

// NoticeStore persists undeliverable notices across daemon restarts.
type NoticeStore interface {
	Append(n domain.Notification) error
	// Drain returns all stored notices and truncates the store.
	Drain() ([]domain.Notification, error)
}
