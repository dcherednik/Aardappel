package hb_tracker

import (
	"aardappel/internal/types"
	"fmt"
)

type heartBeat struct {
	vt uint64
}

type HeartBeatTracker struct {
	streams         map[types.StreamId]heartBeat
	totalStreamsNum int
}

func NewHeartBeatTracker(total int) *HeartBeatTracker {
	if total == 0 {
		return nil
	}

	var hbt HeartBeatTracker
	hbt.streams = make(map[types.StreamId]heartBeat)
	hbt.totalStreamsNum = total
	return &hbt
}

func (ht *HeartBeatTracker) AddHb(data types.HbData) error {
	hb, ok := ht.streams[data.PartitionId]
	if ok {
		if hb.vt > data.Step {
			return fmt.Errorf("attempt to add step: %d less then last vsible: %d for part: %d",
				data.Step, hb.vt, data.PartitionId)
		} else {
			hb.vt = data.Step
		}
	} else {
		hb.vt = data.Step
	}
	ht.streams[data.PartitionId] = hb

	if len(ht.streams) > ht.totalStreamsNum {
		return fmt.Errorf("Resulted stream count: %d grather than total count: %d",
			len(ht.streams), ht.totalStreamsNum)
	}
	return nil
}

func (ht *HeartBeatTracker) GetReady() (types.HbData, bool) {
	var resHb types.HbData

	if len(ht.streams) != ht.totalStreamsNum {
		return resHb, false
	}

	var inited bool
	for k, v := range ht.streams {
		if !inited {
			resHb.PartitionId = k
			resHb.Step = v.vt
			inited = true
		} else {
			if v.vt < resHb.Step {
				resHb.PartitionId = k
				resHb.Step = v.vt
			}
		}
	}

	return resHb, true
}

func (ht *HeartBeatTracker) Commit(data types.HbData) bool {
	hb, ok := ht.streams[data.PartitionId]
	if !ok {
		return true
	} else {
		if hb.vt > data.Step {
			return false
		} else {
			delete(ht.streams, data.PartitionId)
			return true
		}
	}
}
