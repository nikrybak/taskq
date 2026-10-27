// Package cron parses standard five-field cron expressions and computes the
// next activation time.
//
// Supported syntax per field: "*", single values, ranges "a-b", lists "a,b",
// and steps "*/n" or "a-b/n". Day-of-week accepts 0-7 where both 0 and 7 are
// Sunday. The descriptors @yearly, @annually, @monthly, @weekly, @daily,
// @midnight and @hourly are also accepted.
package cron

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Schedule is a parsed cron expression.
type Schedule struct {
	minute, hour, dom, month, dow uint64
	// domStar and dowStar track whether the field was unrestricted, which
	// changes how day-of-month and day-of-week combine (see matchDay).
	domStar, dowStar bool
	expr             string
}

type bounds struct{ min, max int }

var (
	minuteBounds = bounds{0, 59}
	hourBounds   = bounds{0, 23}
	domBounds    = bounds{1, 31}
	monthBounds  = bounds{1, 12}
	dowBounds    = bounds{0, 7}
)

var descriptors = map[string]string{
	"@yearly":   "0 0 1 1 *",
	"@annually": "0 0 1 1 *",
	"@monthly":  "0 0 1 * *",
	"@weekly":   "0 0 * * 0",
	"@daily":    "0 0 * * *",
	"@midnight": "0 0 * * *",
	"@hourly":   "0 * * * *",
}

// ErrSyntax is wrapped by every parse error.
var ErrSyntax = errors.New("invalid cron expression")

// Parse parses a five-field cron expression.
func Parse(expr string) (*Schedule, error) {
	spec := strings.TrimSpace(expr)
	if d, ok := descriptors[spec]; ok {
		spec = d
	}
	fields := strings.Fields(spec)
	if len(fields) != 5 {
		return nil, fmt.Errorf("%w %q: expected 5 fields, got %d", ErrSyntax, expr, len(fields))
	}

	s := &Schedule{expr: expr}
	var err error
	if s.minute, err = parseField(fields[0], minuteBounds); err != nil {
		return nil, fieldErr(expr, "minute", err)
	}
	if s.hour, err = parseField(fields[1], hourBounds); err != nil {
		return nil, fieldErr(expr, "hour", err)
	}
	if s.dom, err = parseField(fields[2], domBounds); err != nil {
		return nil, fieldErr(expr, "day-of-month", err)
	}
	if s.month, err = parseField(fields[3], monthBounds); err != nil {
		return nil, fieldErr(expr, "month", err)
	}
	if s.dow, err = parseField(fields[4], dowBounds); err != nil {
		return nil, fieldErr(expr, "day-of-week", err)
	}
	if s.dow&(1<<7) != 0 {
		s.dow |= 1 << 0
	}
	s.domStar = fields[2] == "*" || strings.HasPrefix(fields[2], "*/")
	s.dowStar = fields[4] == "*" || strings.HasPrefix(fields[4], "*/")
	return s, nil
}

// MustParse is like Parse but panics on error. It is meant for expressions
// that are constants in code.
func MustParse(expr string) *Schedule {
	s, err := Parse(expr)
	if err != nil {
		panic(err)
	}
	return s
}

// String returns the original expression.
func (s *Schedule) String() string { return s.expr }

func fieldErr(expr, field string, err error) error {
	return fmt.Errorf("%w %q: %s: %v", ErrSyntax, expr, field, err)
}

func parseField(field string, b bounds) (uint64, error) {
	var bits uint64
	for part := range strings.SplitSeq(field, ",") {
		v, err := parsePart(part, b)
		if err != nil {
			return 0, err
		}
		bits |= v
	}
	return bits, nil
}

func parsePart(part string, b bounds) (uint64, error) {
	rangePart, stepPart, hasStep := strings.Cut(part, "/")
	step := 1
	if hasStep {
		n, err := strconv.Atoi(stepPart)
		if err != nil || n <= 0 {
			return 0, fmt.Errorf("bad step %q", stepPart)
		}
		step = n
	}

	lo, hi := b.min, b.max
	switch {
	case rangePart == "*":
	case strings.Contains(rangePart, "-"):
		a, z, _ := strings.Cut(rangePart, "-")
		var err error
		if lo, err = parseValue(a, b); err != nil {
			return 0, err
		}
		if hi, err = parseValue(z, b); err != nil {
			return 0, err
		}
		if lo > hi {
			return 0, fmt.Errorf("range %q is reversed", rangePart)
		}
	default:
		v, err := parseValue(rangePart, b)
		if err != nil {
			return 0, err
		}
		lo = v
		// "5/15" means "from 5 to the end, every 15", as in Vixie cron.
		hi = v
		if hasStep {
			hi = b.max
		}
	}

	var bits uint64
	for v := lo; v <= hi; v += step {
		bits |= 1 << uint(v)
	}
	return bits, nil
}

func parseValue(s string, b bounds) (int, error) {
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("bad value %q", s)
	}
	if v < b.min || v > b.max {
		return 0, fmt.Errorf("value %d out of range %d-%d", v, b.min, b.max)
	}
	return v, nil
}

// maxSearch bounds Next for expressions that can never match, such as
// "0 0 30 2 *" (February 30th).
const maxSearch = 5 * 366 * 24 * time.Hour

// Next returns the first activation strictly after t, in t's location. It
// returns the zero time if the schedule never fires.
func (s *Schedule) Next(t time.Time) time.Time {
	loc := t.Location()
	t = t.Truncate(time.Minute).Add(time.Minute)
	limit := t.Add(maxSearch)

	// Each mismatch skips to the start of the next candidate unit instead of
	// stepping minute by minute; the loop converges in a few dozen iterations.
	for t.Before(limit) {
		if s.month&(1<<uint(t.Month())) == 0 {
			t = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, loc)
			continue
		}
		if !s.matchDay(t) {
			t = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, loc)
			continue
		}
		if s.hour&(1<<uint(t.Hour())) == 0 {
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, 0, 0, 0, loc)
			continue
		}
		if s.minute&(1<<uint(t.Minute())) == 0 {
			t = t.Add(time.Minute)
			continue
		}
		return t
	}
	return time.Time{}
}

// matchDay implements the classic cron rule: when both day-of-month and
// day-of-week are restricted, a day matches if either one does.
func (s *Schedule) matchDay(t time.Time) bool {
	domOK := s.dom&(1<<uint(t.Day())) != 0
	dowOK := s.dow&(1<<uint(t.Weekday())) != 0
	if s.domStar || s.dowStar {
		return domOK && dowOK
	}
	return domOK || dowOK
}
