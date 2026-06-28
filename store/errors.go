package store

import (
	"errors"

	sqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// ErrInvalidProjectID is returned (wrapped) by any method given a project_id
// that is empty or unsafe as a path component. It is a CLIENT error — the HTTP
// layer maps it to 400, not 500.
var ErrInvalidProjectID = errors.New("store: invalid project_id")

// ErrInvalidPath is returned (wrapped) by a KV put given an empty path. Also a
// CLIENT error → HTTP 400.
var ErrInvalidPath = errors.New("store: invalid path")

// isUniqueViolation reports whether err is a SQLite UNIQUE-constraint failure
// (extended result code SQLITE_CONSTRAINT_UNIQUE). Used to map a token_hash
// collision onto ErrTenantExists rather than a generic DB error. PRIMARYKEY
// conflicts are handled by ON CONFLICT upserts, so only the secondary UNIQUE
// index reaches here.
func isUniqueViolation(err error) bool {
	var serr *sqlite.Error
	if !errors.As(err, &serr) {
		return false
	}
	return serr.Code() == sqlite3.SQLITE_CONSTRAINT_UNIQUE
}
