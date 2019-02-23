package common

import (
	"errors"
	"fmt"
	"github.com/imkuqin-zw/seqsvr/lib/mmap"
	"os"
	"path/filepath"
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
	saxSeqsData mmap.MMap
}

func NewSet(idBegin, size uint32, fileDir string) (set *Set, err error) {
	var memSize = int64(size) << 3
	var f *os.File
	fileName := fmt.Sprintf("set_%d_%d", idBegin, size)
	filePath := filepath.Join(fileDir, fileName)
	if f, err = OpenFileForMmap(filePath, memSize); err != nil {
		return
	}
	set = &Set{
		setId: RangeID{
			IdBegin: idBegin,
			Size:    size,
		},
	}
	if set.saxSeqsData, err = mmap.Map(f, mmap.RDWR, 0); err != nil {
		return
	}
	return
}

func OpenFileForMmap(filePath string, memSize int64) (f *os.File, err error) {
	if f, err = os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644); err != nil {
		return
	}
	var fs os.FileInfo
	if fs, err = f.Stat(); err != nil {
		return
	}
	if fs.Size() < memSize {
		if err = f.Truncate(memSize); err != nil {
			return
		}
	}
	return
}

// 判断所给路径文件/文件夹是否存在
func Exists(path string) bool {
	_, err := os.Stat(path) //os.Stat获取文件信息
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}

func (s *Set) String() string {
	return fmt.Sprintf("set_%d_%d", s.setId.IdBegin, s.setId.Size)
}

func (s *Set) SetMaxSeq(id uint32, maxSeq uint64) error {
	b, index := CalcSectionId(s.setId, id)
	if !b {
		err := fmt.Errorf("setSectionsData - max_seq invalid: "+
			"local seq = %d,req_seq = %d, in set: %s", id, maxSeq, s.String())
		return err
	}
	s.saxSeqsData[index] = maxSeq
	s.saxSeqsData.Flush()
	return nil
}

func (s *Set) GetMaxSeq(id uint32) (uint64, error) {
	b, index := CalcSectionId(s.setId, id)
	if !b {
		err := fmt.Errorf("getSectionsData - max_seq invalid: "+
			"local seq = %d in set: %s", id, s.String())
		return 0, err
	}
	return s.saxSeqsData[index], nil
}

func (s *Set) Copy(data []byte) {
	s.saxSeqsData.Copy(data)
	s.saxSeqsData.Flush()
}

func (s *Set) GetBytes() []byte {
	return s.saxSeqsData.GetBytes()
}

func (s *Set) Close() error {
	return s.saxSeqsData.Unmap()
}

//获取section在set里的位置
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
