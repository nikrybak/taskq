package main

import (
	"os"
	"strings"
	"testing"
)

func TestParseSchedules(t *testing.T) {
	got, err := parseSchedules([]byte(`[
		{"name": "nightly", "cron": "0 3 * * *", "type": "checksum", "payload": {"data": "x", "rounds": 10}},
		{"name": "ping", "cron": "@hourly", "type": "send-webhook", "max_attempts": 2}
	]`))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Name != "nightly" || got[0].Job.MaxAttempts != 5 || got[1].Job.MaxAttempts != 2 {
		t.Fatalf("unexpected schedules: %+v", got)
	}
}

func TestParseSchedulesErrors(t *testing.T) {
	_, err := parseSchedules([]byte(`[
		{"name": "", "cron": "* * * * *", "type": "t"},
		{"name": "a", "cron": "bogus", "type": "t"},
		{"name": "b", "cron": "* * * * *", "type": ""},
		{"name": "c", "cron": "* * * * *", "type": "t"},
		{"name": "c", "cron": "* * * * *", "type": "t"}
	]`))
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{"#0", `"a"`, `"b"`, "duplicate"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %s: %v", want, err)
		}
	}
}

func TestExampleSchedulesFileIsValid(t *testing.T) {
	raw, err := os.ReadFile("../../schedules.example.json")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parseSchedules(raw); err != nil {
		t.Fatal(err)
	}
}
