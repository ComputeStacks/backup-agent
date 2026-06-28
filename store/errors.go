package store

import (
	"errors"

	sqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

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
