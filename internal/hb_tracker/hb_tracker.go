package hb_tracker

import (
	"fmt"
)

type heartBeat struct {
	vt    uint64
	known bool
}

type HeartBeatTracker struct {
	streams []heartBeat
}

func NewHeartBeatTracker(parts uint32) *HeartBeatTracker {
	if parts == 0 {
		return nil
	}

	var hbt HeartBeatTracker
	hbt.streams = make([]heartBeat, parts)
	return &hbt
}

func (ht *HeartBeatTracker) Size() uint32 {
	return uint32(len(ht.streams))
}

func (ht *HeartBeatTracker) AddHb(step uint64, part uint32) error {
	if part >= uint32(len(ht.streams)) {
		return fmt.Errorf("HeartBeat from unexpected part: %d", part)
	}

	hb := &ht.streams[part]

	if !hb.known {
		hb.vt = step
		hb.known = true
	} else {
		if hb.vt > step {
			return fmt.Errorf("attempt to add step: %d less then last vsible: %d for part: %d",
				step, hb.vt, part)
		} else {
			hb.vt = step
			hb.known = true
		}
	}
	return nil
}

func (ht *HeartBeatTracker) GetReady() (uint64, bool) {
	if !ht.streams[0].known {
		return 0, false
	}

	maybeMin := ht.streams[0].vt

	for i := 1; i < len(ht.streams); i++ {
		if !ht.streams[i].known {
			return 0, false
		}
		maybeMin = min(maybeMin, ht.streams[i].vt)
	}

	return maybeMin, true
}
