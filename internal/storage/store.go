package storage

import "errors"

// ErrOffsetNotFound is returned when a requested offset does not exist.
var ErrOffsetNotFound = errors.New("offset not found")

// Store is the interface for an append-only message log.
type Store interface {
	// Append writes data to the log and returns the assigned offset.
	Append(data []byte) (offset int64, err error)

	// Read retrieves the message at the given offset.
	Read(offset int64) ([]byte, error)

	// CurrentOffset returns the next offset that will be assigned.
	CurrentOffset() int64

	// TimestampAt returns the timestamp (Unix millis) of the message at the given offset.
	TimestampAt(offset int64) (int64, error)

	// FindOffsetByTimestamp returns the first offset whose timestamp >= ts (Unix millis).
	// Returns -1 if no matching offset is found.
	FindOffsetByTimestamp(ts int64) (int64, error)

	// Close flushes and closes the underlying storage.
	Close() error
}
