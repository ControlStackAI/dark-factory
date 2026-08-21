package sqlite

import "errors"

var (
	ErrUnsupportedSchema = errors.New("unsupported sqlite schema version")
	ErrCorruptDatabase   = errors.New("corrupt sqlite database")
	ErrInvalidRecord     = errors.New("invalid durable record")
)
