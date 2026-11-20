package cron

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Schedule represents a parsed 5-field cron expression (minute hour dom month dow).
type Schedule struct {
	expr    string
	minutes [60]bool
	hours   [24]bool
	dom     [32]bool // 1..31
	months  [13]bool // 1..12
	dow     [7]bool  // 0..6 (Sunday=0)
	domStar bool
	dowStar bool
}

var presets = map[string]string{
	"@yearly":   "0 0 1 1 *",
	"@annually": "0 0 1 1 *",
	"@monthly":  "0 0 1 * *",
	"@weekly":   "0 0 * * 0",
	"@daily":    "0 0 * * *",
	"@midnight": "0 0 * * *",
	"@hourly":   "0 * * * *",
}

// Validate checks whether a cron expression is syntactically valid.
func Validate(expr string) error {
	_, err := Parse(expr)
	return err
}

// Parse parses a standard 5-field cron expression or macro (@hourly, @daily, etc.).
func Parse(expr string) (*Schedule, error) {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return nil, fmt.Errorf("cron expression must not be empty")
	}

	expanded := trimmed
	if macro, ok := presets[strings.ToLower(trimmed)]; ok {
		expanded = macro
	}

	fields := strings.Fields(expanded)
	if len(fields) != 5 {
		return nil, fmt.Errorf("cron expression %q must have 5 fields (minute hour day-of-month month day-of-week)", expr)
	}

	s := &Schedule{
		expr:    trimmed,
		domStar: fields[2] == "*",
		dowStar: fields[4] == "*",
	}

	if err := parseField(fields[0], 0, 59, s.minutes[:]); err != nil {
		return nil, fmt.Errorf("cron minute field %q: %w", fields[0], err)
	}
	if err := parseField(fields[1], 0, 23, s.hours[:]); err != nil {
		return nil, fmt.Errorf("cron hour field %q: %w", fields[1], err)
	}
	if err := parseField(fields[2], 1, 31, s.dom[:]); err != nil {
		return nil, fmt.Errorf("cron day-of-month field %q: %w", fields[2], err)
	}
	if err := parseField(fields[3], 1, 12, s.months[:]); err != nil {
		return nil, fmt.Errorf("cron month field %q: %w", fields[3], err)
	}

	// Weekday accepts 0..7 where both 0 and 7 represent Sunday.
	var rawDow [8]bool
	if err := parseField(fields[4], 0, 7, rawDow[:]); err != nil {
		return nil, fmt.Errorf("cron day-of-week field %q: %w", fields[4], err)
	}
	for i := 0; i < 7; i++ {
		s.dow[i] = rawDow[i]
	}
	if rawDow[7] {
		s.dow[0] = true
	}

	return s, nil
}

// Expr returns the original expression string.
func (s *Schedule) Expr() string {
	return s.expr
}

// Next returns the next time strictly after `after` matching the schedule.
// Returns the zero Time if no match is found within 4 years.
func (s *Schedule) Next(after time.Time) time.Time {
	t := after.Truncate(time.Minute).Add(time.Minute)
	limit := after.AddDate(4, 0, 0)

	for !t.After(limit) {
		if !s.months[int(t.Month())] {
			// Advance to the start of the next month.
			t = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, t.Location())
			continue
		}
		if !s.dayMatches(t) {
			t = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, t.Location())
			continue
		}
		if !s.hours[t.Hour()] {
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, 0, 0, 0, t.Location())
			continue
		}
		if !s.minutes[t.Minute()] {
			t = t.Add(time.Minute)
			continue
		}
		return t
	}
	return time.Time{}
}

func (s *Schedule) dayMatches(t time.Time) bool {
	domMatch := s.dom[t.Day()]
	dowMatch := s.dow[int(t.Weekday())]

	// Standard cron semantics: if both DOM and DOW are restricted (non-*), either match triggers.
	if !s.domStar && !s.dowStar {
		return domMatch || dowMatch
	}
	return domMatch && dowMatch
}

func parseField(field string, minVal, maxVal int, target []bool) error {
	parts := strings.Split(field, ",")
	for _, part := range parts {
		if part == "" {
			return fmt.Errorf("empty list segment")
		}
		if err := parsePart(part, minVal, maxVal, target); err != nil {
			return err
		}
	}
	return nil
}

func parsePart(part string, minVal, maxVal int, target []bool) error {
	step := 1
	rangePart := part
	if slashIdx := strings.IndexByte(part, '/'); slashIdx != -1 {
		rangePart = part[:slashIdx]
		stepStr := part[slashIdx+1:]
		parsedStep, err := strconv.Atoi(stepStr)
		if err != nil || parsedStep <= 0 {
			return fmt.Errorf("invalid step %q", stepStr)
		}
		step = parsedStep
	}

	var start, end int
	switch {
	case rangePart == "*":
		start, end = minVal, maxVal
	case strings.Contains(rangePart, "-"):
		bounds := strings.Split(rangePart, "-")
		if len(bounds) != 2 {
			return fmt.Errorf("invalid range %q", rangePart)
		}
		var err error
		start, err = strconv.Atoi(bounds[0])
		if err != nil {
			return fmt.Errorf("invalid range start %q", bounds[0])
		}
		end, err = strconv.Atoi(bounds[1])
		if err != nil {
			return fmt.Errorf("invalid range end %q", bounds[1])
		}
		if start < minVal || end > maxVal || start > end {
			return fmt.Errorf("range %d-%d out of bounds [%d..%d]", start, end, minVal, maxVal)
		}
	default:
		val, err := strconv.Atoi(rangePart)
		if err != nil {
			return fmt.Errorf("invalid value %q", rangePart)
		}
		if val < minVal || val > maxVal {
			return fmt.Errorf("value %d out of bounds [%d..%d]", val, minVal, maxVal)
		}
		start = val
		if strings.Contains(part, "/") {
			end = maxVal
		} else {
			end = val
		}
	}

	for v := start; v <= end; v += step {
		target[v] = true
	}
	return nil
}
