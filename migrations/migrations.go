// Package migrations embeds the SQL schema migrations so the binary can
// apply them without shipping extra files.
package migrations

import "embed"

// FS contains the migration files, applied in lexical order.
//
//go:embed *.sql
var FS embed.FS
