package service

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/micro/protobuf/proto"
	"go.uber.org/zap"
	"io"
	"net"
	"os"
	"path/filepath"
	"seqsvr/common"
	"seqsvr/lib/logger"
	"seqsvr/protobuf/storesvr"
	"seqsvr/store/config"
	"sync"
	"time"
)

var (
	// ErrNotLeader is returned when a node attempts to execute a leader-only
	// operation.
	ErrNotLeader = errors.New("not leader")

	// ErrOpenTimeout is returned when the Store does not apply its initial
	// logs within the specified time.
	ErrOpenTimeout = errors.New("timeout waiting for initial logs application")

	// ErrStoreInvalidState is returned when a Store is in an invalid
	// state for the requested operation.
	ErrStoreInvalidState = errors.New("store not in valid state")
)

const (
	retainSnapshotCount = 2
	connectionPoolCount = 5
	connectionTimeout   = 10 * time.Second
	raftLogCacheSize    = 512
	leaderWaitDelay     = 100 * time.Millisecond
	appliedWaitDelay    = 100 * time.Millisecond
)

const (
	_ = iota
	RAFT_CMD_SET_MAX_SEQ
	RAFT_CMD_GET_MAX_SEQ
	METADATA_SET
	METADATA_DEL
)

type Store struct {
	set   *common.Set
	setMu sync.RWMutex

	raft        *raft.Raft
	raftID      string
	raftDir     string
	raftTcpAddr string
	raftTn      *raft.NetworkTransport
	raftLog     raft.LogStore         // Persistent log store.
	raftStable  raft.StableStore      // Persistent k-v store.
	boltStore   *raftboltdb.BoltStore // Physical store.

	leaderNotifyCh    chan bool
	ApplyTimeout      time.Duration
	SnapshotInterval  time.Duration
	SnapshotThreshold uint64

	metaMu sync.RWMutex
	meta   map[string]map[string]string

	closedMu sync.Mutex
	closed   bool
	wg       sync.WaitGroup
}

func NewStore(config *config.StoreConf) (*Store, error) {
	var err error
	s := &Store{
		raftID:            config.Raft.RaftID,
		raftDir:           config.Raft.RaftDir,
		raftTcpAddr:       config.Raft.TcpAddr,
		ApplyTimeout:      config.Raft.ApplyTimeout,
		meta:              make(map[string]map[string]string),
		leaderNotifyCh:    make(chan bool, 1),
		SnapshotInterval:  config.Raft.SnapshotInterval,
		SnapshotThreshold: config.Raft.SnapshotThreshold,
	}
	s.set, err = common.NewSet(config.IdBegin, config.Size, config.DataFileDir)
	if err != nil {
		return nil, err
	}
	return s, nil

}

func (s *Store) Open(bootstrap bool) error {
	s.closedMu.Lock()
	defer s.closedMu.Unlock()
	if s.closed {
		return ErrStoreInvalidState
	}
	var err error
	raftConf := raft.DefaultConfig()
	raftConf.LocalID = raft.ServerID(s.raftID)
	hclog.New(&hclog.LoggerOptions{
		Name:       "[raft] ",
		Level:      hclog.Debug,
		JSONFormat: true,
	})
	raftConf.Logger = hclog.Default()
	//raftConf.NotifyCh = s.leaderNotifyCh
	raftConf.ShutdownOnRemove = false
	if raftConf.SnapshotInterval != 0 {
		raftConf.SnapshotInterval = s.SnapshotInterval
	}
	if raftConf.SnapshotThreshold == 0 {
		raftConf.SnapshotThreshold = s.SnapshotThreshold
	}

	if err := os.MkdirAll(s.raftDir, 0755); err != nil {
		return err
	}
	dbPath := filepath.Join(s.raftDir, fmt.Sprintf("raft_%s.db", s.raftID))

	// Create Raft-compatible network layer.
	s.raftTn, err = newRaftTransport(s.raftTcpAddr)
	if err != nil {
		return err
	}

	// Create the log store and stable store.

	s.boltStore, err = raftboltdb.NewBoltStore(dbPath)
	if err != nil {
		return err
	}
	s.raftStable = s.boltStore
	s.raftLog, err = raft.NewLogCache(raftLogCacheSize, s.boltStore)
	if err != nil {
		return err
	}

	// Create the snapshot store. This allows Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(s.raftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}
	raftNode, err := raft.NewRaft(raftConf, s, s.raftLog, s.raftStable, snapshots, s.raftTn)
	if err != nil {
		return err
	}
	if bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raftConf.LocalID,
					Address: s.raftTn.LocalAddr(),
				},
			},
		}
		raftNode.BootstrapCluster(configuration)
	}
	s.raft = raftNode
	return nil
}

// Close closes the store. If wait is true, waits for a graceful shutdown.
// Once closed, a Store may not be re-opened.
func (s *Store) Close(wait bool) error {
	s.closedMu.Lock()
	defer s.closedMu.Unlock()
	if s.closed {
		return nil
	}
	defer func() {
		s.closed = true
	}()

	s.wg.Wait()
	s.setMu.Lock()
	defer s.setMu.Unlock()
	if err := s.set.Close(); err != nil {
		return err
	}

	if s.raft != nil {

		f := s.raft.Shutdown()
		if wait {
			if e := f.(raft.Future); e.Error() != nil {
				return e.Error()
			}
		}
		s.raft = nil
	}

	if s.boltStore != nil {
		if err := s.boltStore.Close(); err != nil {
			return err
		}
		s.boltStore = nil
	}
	s.raftLog = nil
	s.raftStable = nil
	return nil
}

func newRaftTransport(addr string) (*raft.NetworkTransport, error) {
	address, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(address.String(), address, connectionPoolCount, connectionTimeout, os.Stderr)
	if err != nil {
		return nil, err
	}
	return transport, nil
}

// Get returns the value for the given key.
func (s *Store) Get(uid uint32) (uint64, error) {
	cmd := &storesvr.Cmd{
		Typ:    RAFT_CMD_GET_MAX_SEQ,
		GetSeq: &storesvr.ReqGetMaxSeq{Uid: uid},
	}
	b, err := proto.Marshal(cmd)
	if err != nil {
		return 0, err
	}
	f := s.raft.Apply(b, s.ApplyTimeout)
	if e := f.(raft.Future); e.Error() != nil {
		if e.Error() == raft.ErrNotLeader {
			return 0, ErrNotLeader
		}
		return 0, e.Error()
	}

	switch r := f.Response().(type) {
	case error:
		return 0, r
	case uint64:
		return r, nil
	default:
		panic("unsupported type")
	}
}

func (s *Store) Set(req storesvr.UidMaxSeq) error {
	cmd := &storesvr.Cmd{
		Typ: RAFT_CMD_SET_MAX_SEQ,
		SetSeq: &storesvr.ReqSetMaxSeq{
			Uid:    req.Uid,
			MaxSeq: req.MaxSeq,
		},
	}
	b, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}
	f := s.raft.Apply(b, s.ApplyTimeout)
	if e := f.(raft.Future); e.Error() != nil {
		if e.Error() == raft.ErrNotLeader {
			return ErrNotLeader
		}
		return e.Error()
	}
	switch r := f.Response().(type) {
	case error:
		return r
	case nil:
		return nil
	default:
		return errors.New("unsupported type")
	}
}

func (s *Store) Apply(l *raft.Log) interface{} {
	s.setMu.RLock()
	defer s.setMu.RUnlock()

	var c storesvr.Cmd
	if err := proto.Unmarshal(l.Data, &c); err != nil {
		logger.Error(fmt.Sprintf("failed to unmarshal cluster command", zap.Error(err)))
		return err
	}

	switch c.Typ {
	case RAFT_CMD_SET_MAX_SEQ:
		logger.Debug("apply RAFT_CMD_SET_MAX_SEQ", zap.Any("params", c.SetSeq))
		return s.set.SetMaxSeq(c.SetSeq.Uid, c.SetSeq.MaxSeq)
	case RAFT_CMD_GET_MAX_SEQ:
		logger.Debug("apply RAFT_CMD_GET_MAX_SEQ", zap.Any("params", c.GetSeq))
		maxSeq, err := s.set.GetMaxSeq(c.GetSeq.Uid)
		if err != nil {
			return err
		}
		return maxSeq
	case METADATA_SET:
		logger.Debug("apply METADATA_SET", zap.Any("params", c.SetMetadata))
		s.metaMu.Lock()
		defer s.metaMu.Unlock()
		if _, ok := s.meta[c.SetMetadata.RaftId]; !ok {
			s.meta[c.SetMetadata.RaftId] = make(map[string]string)
		}
		for k, v := range c.SetMetadata.Data {
			s.meta[c.SetMetadata.RaftId][k] = v
		}
		logger.Debug("apply METADATA_SET SUCCESS")
		return nil
	case METADATA_DEL:
		logger.Debug("apply METADATA_DEL", zap.Any("params", c.DelMetadata))
		s.metaMu.Lock()
		defer s.metaMu.Unlock()
		delete(s.meta, c.DelMetadata.RaftId)
		logger.Debug("apply METADATA_DEL SUCCESS")
		return nil
	default:
		return fmt.Errorf("unknown command: %v", c.Typ)
	}
}

func (s *Store) Snapshot() (raft.FSMSnapshot, error) {
	s.setMu.RLock()
	defer s.setMu.RUnlock()
	return &fsmSnapshot{data: s.set.GetBytes()}, nil
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state.
func (s *Store) Restore(rc io.ReadCloser) error {
	s.setMu.Lock()
	defer s.setMu.Unlock()

	// Get size of database.
	var sz uint64
	if err := binary.Read(rc, binary.LittleEndian, &sz); err != nil {
		return err
	}

	// Now read in the database file data and restore.
	data := make([]byte, sz)
	if _, err := io.ReadFull(rc, data); err != nil {
		return err
	}

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	s.set.Copy(data)
	return nil
}

// remove removes the node, with the given ID, from the cluster.
func (s *Store) remove(id string) error {
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	f := s.raft.RemoveServer(raft.ServerID(id), 0, 0)
	if f.Error() != nil {
		if f.Error() == raft.ErrNotLeader {
			return ErrNotLeader
		}
		return f.Error()
	}
	c := &storesvr.Cmd{
		Typ: METADATA_DEL,
		DelMetadata: &storesvr.ReqDelMetadata{
			RaftId: id,
		},
	}
	b, err := proto.Marshal(c)
	if err != nil {
		return err
	}
	f = s.raft.Apply(b, s.ApplyTimeout)
	if e := f.(raft.Future); e.Error() != nil {
		if e.Error() == raft.ErrNotLeader {
			return ErrNotLeader
		}
		e.Error()
	}
	return nil
}

func (s *Store) IsLeader() bool {
	return s.raft.State() == raft.Leader
}

func (s *Store) GetLeaderAddr() string {
	return string(s.raft.Leader())
}

// LeaderID returns the node ID of the Raft leader. Returns a
// blank string if there is no leader, or an error.
func (s *Store) LeaderID() (string, error) {
	addr := s.GetLeaderAddr()
	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		logger.Error("failed to get raft configuration", zap.Error(err))
		return "", err
	}

	for _, srv := range configFuture.Configuration().Servers {
		if srv.Address == raft.ServerAddress(addr) {
			return string(srv.ID), nil
		}
	}
	return "", nil
}

// WaitForLeader blocks until a leader is detected, or the timeout expires.
func (s *Store) WaitForLeader(timeout time.Duration) (string, error) {
	tck := time.NewTicker(leaderWaitDelay)
	defer tck.Stop()
	tmr := time.NewTimer(timeout)
	defer tmr.Stop()

	for {
		select {
		case <-tck.C:
			l := s.GetLeaderAddr()
			if l != "" {
				return l, nil
			}
		case <-tmr.C:
			return "", fmt.Errorf("timeout expired")
		}
	}
}

// WaitForApplied waits for all Raft log entries to to be applied to the
// underlying database.
func (s *Store) WaitForApplied(timeout time.Duration) error {
	if timeout == 0 {
		return nil
	}
	logger.Infof("waiting for up to %s for application of initial logs", timeout)
	if err := s.WaitForAppliedIndex(s.raft.LastIndex(), timeout); err != nil {
		return ErrOpenTimeout
	}
	return nil
}

// WaitForAppliedIndex blocks until a given log index has been applied,
// or the timeout expires.
func (s *Store) WaitForAppliedIndex(idx uint64, timeout time.Duration) error {
	tck := time.NewTicker(appliedWaitDelay)
	defer tck.Stop()
	tmr := time.NewTimer(timeout)
	defer tmr.Stop()

	for {
		select {
		case <-tck.C:
			if s.raft.AppliedIndex() >= idx {
				return nil
			}
		case <-tmr.C:
			return fmt.Errorf("timeout expired")
		}
	}
}

// SetMetadata adds the metadata md to any existing metadata for
// this node.
func (s *Store) SetMetadata(md map[string]string) error {
	return s.setMetadata(s.raftID, md)
}

func (s *Store) Join(nodeId, addr string, metadata map[string]string) error {
	logger.Infof("received request to join node at %s", addr)
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		logger.Error("failed to get raft configuration", zap.Error(err))
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == raft.ServerID(nodeId) || srv.Address == raft.ServerAddress(addr) {
			// However if *both* the ID and the address are the same, the no
			// join is actually needed.
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeId) {
				logger.Infof("node %s at %s already member of cluster, ignoring join request",
					nodeId, addr)
				return nil
			}
			if err := s.remove(nodeId); err != nil {
				logger.Infof("failed to remove node: %v", err)
				return err
			}
		}
	}
	f := s.raft.AddVoter(raft.ServerID(nodeId), raft.ServerAddress(addr), 0, 0)
	if e := f.(raft.Future); e.Error() != nil {
		if e.Error() == raft.ErrNotLeader {
			return ErrNotLeader
		}
		return e.Error()
	}

	if err := s.setMetadata(nodeId, metadata); err != nil {
		return err
	}

	logger.Infof("node at %s joined successfully", addr)
	return nil
}

// setMetadata adds the metadata md to any existing metadata for
// the given node ID.
func (s *Store) setMetadata(id string, md map[string]string) error {
	// Check local data first.
	if func() bool {
		s.metaMu.RLock()
		defer s.metaMu.RUnlock()
		if _, ok := s.meta[id]; ok {
			for k, v := range md {
				if s.meta[id][k] != v {
					return false
				}
			}
			return true
		}
		return false
	}() {
		// Local data is same as data being pushed in,
		// nothing to do.
		return nil
	}
	c := &storesvr.Cmd{
		Typ: METADATA_SET,
		SetMetadata: &storesvr.ReqSetMetadata{
			RaftId: id,
			Data:   md,
		},
	}
	b, err := proto.Marshal(c)
	if err != nil {
		return err
	}
	f := s.raft.Apply(b, s.ApplyTimeout)
	if e := f.(raft.Future); e.Error() != nil {
		if e.Error() == raft.ErrNotLeader {
			return ErrNotLeader
		}
		return e.Error()
	}
	switch r := f.Response().(type) {
	case error:
		return r
	case nil:
		return nil
	default:
		return errors.New("unsupported type")
	}
}

// Metadata returns the value for a given key, for a given node ID.
func (s *Store) GetMetadata(id, key string) string {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()

	if _, ok := s.meta[id]; !ok {
		return ""
	}
	v, ok := s.meta[id][key]
	if ok {
		return v
	}

	return ""
}

func (s *Store) GetAllNodeMetaByKey(key string) []string {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	result := make([]string, 0, len(s.meta))
	for _, item := range s.meta {
		temp, _ := item[key]
		result = append(result, temp)
	}
	return result
}
