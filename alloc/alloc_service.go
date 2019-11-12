package alloc

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/coreos/etcd/clientv3"
	"seqsvr/common"
	"sync"
	"sync/atomic"
)

var ErrNotFoundUid = errors.New("not found the uid in the node")
var ErrVersion = errors.New("not found the uid in the node")
var ErrTimeout = errors.New("not found the uid in the node")
var ErrMigrate = errors.New("not found the uid in the node")

type Stat uint32

const (
	_ Stat = iota
	PreMigrate
	Migrate
	Rollback
)

const (
	_ Stat = iota
	ServiceNormal
	ServiceMigrate
)

type Migration struct {
	Stat      Stat                          `json:"stat"`
	ChangedId []common.SectionID            `json:"changed_id"`
	Change    map[string][]common.SectionID `json:"change"`
	Version   uint64                        `json:"version"`
}

type Service struct {
	name     string
	rMut     sync.RWMutex
	RVersion uint64
	section  map[common.SectionID]common.Section

	nextRVersion uint64
	nextSection  map[common.SectionID]common.Section

	stat Stat

	ctx    context.Context
	cancel context.CancelFunc

	etcdV3 *clientv3.Client

	StoreClient StoreClient
}

func (s *Service) preMigrate(m *Migration) {
	s.rMut.Lock()
	defer s.rMut.Unlock()
	s.stat = ServiceMigrate
	s.nextRVersion = m.Version
	s.nextSection = s.section
	for _, sectionId := range m.ChangedId {
		delete(s.nextSection, sectionId)
	}
	for _, sectionId := range m.Change[s.name] {
		section := common.Section{
			RangeID: common.RangeID{IdBegin: uint32(sectionId), Size: common.PerSectionIdSize},
			Mut:     sync.RWMutex{},
			MaxSeq:  s.StoreClient.GetCurMaxSeqNum(uint32(sectionId)),
			SeqNum:  make([]uint64, common.PerSectionIdSize),
		}
		common.Memset(section.SeqNum, section.MaxSeq)
		s.nextSection[sectionId] = section
	}
	_, err := s.etcdV3.Put(s.ctx, "/migrate/preConfirm/"+s.name, "prepared")
	if err != nil {

	}
}

func (s *Service) migrate() {
	s.rMut.Lock()
	defer s.rMut.Unlock()
	s.section = s.nextSection
	s.RVersion = s.nextRVersion
	s.stat = ServiceNormal
	_, err := s.etcdV3.Put(s.ctx, "/migrate/migrateConfirm/"+s.name, "migrated")
	if err != nil {

	}
}

func (s *Service) rollback() {
	s.rMut.Lock()
	defer s.rMut.Unlock()
	s.nextSection = nil
	s.nextRVersion = 0
	s.stat = ServiceNormal
	_, err := s.etcdV3.Put(s.ctx, "/migrate/rollbackConfirm/"+s.name, "rollback")
	if err != nil {

	}
}

func (s *Service) watchRouter() {
	rch := s.etcdV3.Watch(context.Background(), "/seqsvr/router/migrate/task", clientv3.WithPrefix())
	for {
		select {
		case rsp := <-rch:
			for _, ev := range rsp.Events {
				if ev.Type == clientv3.EventTypePut {
					var task Migration
					if err := json.Unmarshal(ev.Kv.Value, &task); err != nil {
						continue
					}
					switch task.Stat {
					case PreMigrate: //TODO 迁移准备, 赋值nextSection, 阻塞所有请求, 并向etcd发送确认（/migrate/preConfirm）
						s.preMigrate(&task)
					case Migrate: // TODO 赋值section并清空nextSection, 处理请求, 并向etcd发送确认（/migrate/migrateConfirm）
						s.migrate()
					case Rollback: // TODO 清空nextSection，并向etcd发送确认（/migrate/rollbackConfirm）
						s.rollback()
					}
				}
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Service) GetRouter() (map[string][]uint32, error) {
	s.rMut.RLock()
	defer s.rMut.RUnlock()
	return nil, nil
}

func (s *Service) FetchNextSeqNum(uid uint32, v uint64) (uint64, bool, error) {
	s.rMut.RLock()
	defer s.rMut.RUnlock()

	if s.stat == ServiceMigrate {
		return 0, false, ErrMigrate
	}

	var routerChange bool
	if s.RVersion > v {
		routerChange = true
	}

	var seqNum uint64
	section, ok := s.section[common.GetSectionIDByUid(uid)]
	if !ok {
		return 0, routerChange, ErrNotFoundUid
	}
	if ok {
		_, index := common.CalcIndex(section.RangeID, uid)
		section.Mut.RLock()
		seqNum = atomic.AddUint64(&section.SeqNum[index], 1)
		if seqNum > section.MaxSeq {
			section.Mut.RUnlock()
			section.Mut.Lock()
			if seqNum > section.MaxSeq {
				section.MaxSeq = s.StoreClient.UpdateMaxSeqNum(uid)
			}
			section.Mut.Unlock()
		} else {
			section.Mut.RUnlock()
		}
	}
	return seqNum, routerChange, nil
}
