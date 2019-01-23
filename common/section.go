package common

import (
	"errors"
	"fmt"
)

const PerSectionIdSize uint32 = 100000

var NotInRangeSet = errors.New("id not int the set range")

type RangeID struct {
	IdBegin uint32
	Size    uint32
}

type SetID RangeID

type Section struct {
	SectionId   uint32
	MaxSeq      uint64
	SectionName string
	IdSize      uint32
}

type Set struct {
	setId       RangeID
	saxSeqsData []uint64
}

func (s *Set) String() string {
	return fmt.Sprintf("set_%d_%d", s.setId.IdBegin, s.setId.Size)
}

func (s *Set) SetMaxSeq(id uint32, maxSeq uint64) error {
	b, index := CalcSectionId(s.setId, id)
	if !b {
		err := fmt.Errorf("setSectionsData - max_seq invalid: " +
			"local seq = %d,req_seq = %d, in set: %s", id, maxSeq, s.String())
		return err
	}
	s.saxSeqsData[index] = maxSeq
	return nil
}

func CalcSectionId(rangeId RangeID, id uint32) (bool, uint32) {
	if !CheckIDByRange(rangeId, id) {
		return false, 0
	}
	return true, (id - rangeId.IdBegin) / PerSectionIdSize
}

// 检查id是否在当前set里
func CheckIDByRange(rangeId RangeID, id uint32) bool {
	return id >= rangeId.IdBegin && id < rangeId.IdBegin+rangeId.Size
}
