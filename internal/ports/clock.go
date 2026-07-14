package ports

import "time"

// Clock abstracts time so usecases can be tested without real delays.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
}

// Timer abstracts a cancellable, resettable time.Timer.
type Timer interface {
	C() <-chan time.Time
	Reset(d time.Duration) bool
	Stop() bool
}
