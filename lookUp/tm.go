package main

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"sync"
)

type TMService struct {
	Version    uint64
	etcdPrefix string
	etcdV3     *clientv3.Client
	rMut       sync.RWMutex

	master bool
}

func (s *TMService) BeMaster() {
	for {
		session, err := concurrency.NewSession(s.etcdV3, concurrency.WithTTL(1))
		if err != nil {
			continue
		}

		mutex := concurrency.NewMutex(session, "/lock")
		if mutex == nil {
			continue
		}
		errMutex := mutex.Lock(context.TODO())
		if errMutex != nil {
			continue
		}
		s.rMut.Lock()
		s.master = true
		s.etcdV3.Get(context.TODO())
		s.rMut.Unlock()
		go func() {
			select {
			case <-session.Done():
				s.rMut.Lock()
				s.master = false
				s.rMut.Unlock()
				s.BeMaster()
			}
		}()

	}
}

func main() {

}
