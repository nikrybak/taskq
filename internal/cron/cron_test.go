package cron

import (
	"errors"
	"testing"
	"time"
)

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	v, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestNext(t *testing.T) {
	tests := []struct {
		expr string
		from string
		want string
	}{
		{"* * * * *", "2026-03-10T12:30:15Z", "2026-03-10T12:31:00Z"},
		{"* * * * *", "2026-03-10T12:30:00Z", "2026-03-10T12:31:00Z"},
		{"*/15 * * * *", "2026-03-10T12:31:00Z", "2026-03-10T12:45:00Z"},
		{"*/15 * * * *", "2026-03-10T12:59:00Z", "2026-03-10T13:00:00Z"},
		{"5/20 * * * *", "2026-03-10T12:26:00Z", "2026-03-10T12:45:00Z"},
		{"0 9 * * 1-5", "2026-03-13T10:00:00Z", "2026-03-16T09:00:00Z"}, // Friday -> Monday
		{"30 2 1 * *", "2026-01-31T23:00:00Z", "2026-02-01T02:30:00Z"},
		{"0 0 29 2 *", "2026-03-01T00:00:00Z", "2028-02-29T00:00:00Z"}, // next leap day
		{"0 0 * * 7", "2026-03-10T00:00:00Z", "2026-03-15T00:00:00Z"},  // 7 is Sunday
		{"0 12 1,15 * *", "2026-03-02T00:00:00Z", "2026-03-15T12:00:00Z"},
		{"59 23 31 12 *", "2026-06-01T00:00:00Z", "2026-12-31T23:59:00Z"},
		// Both day fields restricted: the 13th OR any Friday, whichever is first.
		{"0 0 13 * 5", "2026-03-01T00:00:00Z", "2026-03-06T00:00:00Z"},
		{"@hourly", "2026-03-10T12:30:00Z", "2026-03-10T13:00:00Z"},
		{"@daily", "2026-03-10T12:30:00Z", "2026-03-11T00:00:00Z"},
		{"@weekly", "2026-03-10T12:30:00Z", "2026-03-15T00:00:00Z"},
		{"@monthly", "2026-03-10T12:30:00Z", "2026-04-01T00:00:00Z"},
		{"@yearly", "2026-03-10T12:30:00Z", "2027-01-01T00:00:00Z"},
	}
	for _, tt := range tests {
		t.Run(tt.expr+"@"+tt.from, func(t *testing.T) {
			s, err := Parse(tt.expr)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			got := s.Next(mustTime(t, tt.from))
			if want := mustTime(t, tt.want); !got.Equal(want) {
				t.Fatalf("Next = %s, want %s", got.Format(time.RFC3339), tt.want)
			}
		})
	}
}

func TestNextNeverFires(t *testing.T) {
	s := MustParse("0 0 30 2 *")
	if got := s.Next(mustTime(t, "2026-01-01T00:00:00Z")); !got.IsZero() {
		t.Fatalf("expected zero time, got %s", got)
	}
}

func TestParseErrors(t *testing.T) {
	tests := []string{
		"",
		"* * * *",
		"* * * * * *",
		"60 * * * *",
		"* 24 * * *",
		"* * 0 * *",
		"* * * 13 *",
		"* * * * 8",
		"*/0 * * * *",
		"5-1 * * * *",
		"a * * * *",
		"1,,2 * * * *",
		"@reboot",
	}
	for _, expr := range tests {
		t.Run(expr, func(t *testing.T) {
			if _, err := Parse(expr); !errors.Is(err, ErrSyntax) {
				t.Fatalf("Parse(%q) error = %v, want ErrSyntax", expr, err)
			}
		})
	}
}
