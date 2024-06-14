package hb_tracker

import (
	"aardappel/internal/hb_tracker"
	"testing"
)

func TestTrackerSize(t *testing.T) {
	tracker := hb_tracker.NewHeartBeatTracker(7)
	if sz := tracker.Size(); sz != 7 {
		t.Errorf("Unexpected tracker size %d, %d", sz, 7)
	}
}

func TestAddUnexpectedPart(t *testing.T) {
	tracker := hb_tracker.NewHeartBeatTracker(3)
	if err := tracker.AddHb(1, 3); err == nil {
		t.Error("Expect an error in case event out from allowed part range")
	}
	_, ready := tracker.GetReady()
	if ready {
		t.Error("Expect ready status - we haven't add heartbeat for each part")
	}
}

func TestNotAllPart(t *testing.T) {
	tracker := hb_tracker.NewHeartBeatTracker(3)
	_ = tracker.AddHb(3, 0)
	_, ready := tracker.GetReady()
	if ready {
		t.Error("Expect ready status - we haven't add heartbeat for each part")
	}
}

func TestGetLowestHb(t *testing.T) {
	tracker := hb_tracker.NewHeartBeatTracker(3)
	_ = tracker.AddHb(3, 0)
	_ = tracker.AddHb(5, 0)
	_ = tracker.AddHb(6, 0)

	_ = tracker.AddHb(2, 1)
	_ = tracker.AddHb(7, 1)

	_ = tracker.AddHb(4, 2)

	vt, ready := tracker.GetReady()
	if !ready {
		t.Error("Expect ready status - we have added heartbeat for each part")
	}
	if vt != 4 {
		t.Errorf("Unexpected timestamp, got: %d", vt)
	}
}
