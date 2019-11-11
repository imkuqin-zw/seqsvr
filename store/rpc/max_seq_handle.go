package rpc

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"seqsvr/lib/grpcerr"
	"seqsvr/lib/logger"
	"seqsvr/protobuf/storesvr"
	"seqsvr/store/consts"
	"seqsvr/store/err_status"
	"seqsvr/store/service"
)

type MaxSeqHandle struct {
	store *service.Store
	minId uint32
	maxId uint32
}

func NewMaxSeqHandle(idBegin, size uint32, store *service.Store) *MaxSeqHandle {
	return &MaxSeqHandle{
		store: store,
		minId: idBegin,
		maxId: idBegin + size,
	}
}

func (s *MaxSeqHandle) GetMinId() uint32 {
	return s.minId
}

func (s *MaxSeqHandle) GetMaxId() uint32 {
	return s.maxId
}

func (s *MaxSeqHandle) RpcUpdateMaxSeq(ctx context.Context, seq *storesvr.UidMaxSeq) (*storesvr.NoContent, error) {
	if seq.Uid < s.minId || seq.Uid > s.maxId {
		return nil, grpcerr.New(codes.InvalidArgument,
			fmt.Sprintf(err_status.ErrStr[err_status.UidNotInRange], s.minId, s.maxId),
			err_status.UidNotInRange)
	}
	if !s.store.IsLeader() {
		return nil, grpcerr.New(codes.PermissionDenied,
			err_status.ErrStr[err_status.NotLeader],
			err_status.NotLeader,
			&storesvr.Leader{Addr: s.leaderAPIAddr(), Nodes: s.allNodeAddr()})
	}
	return &storesvr.NoContent{}, s.store.Set(*seq)
}

func (s *MaxSeqHandle) RpcGetSeqMax(ctx context.Context, uid *storesvr.Uid) (*storesvr.MaxSeq, error) {
	if uid.Value < s.minId || uid.Value > s.maxId {
		return nil, grpcerr.New(codes.InvalidArgument,
			fmt.Sprintf(err_status.ErrStr[err_status.UidNotInRange], s.minId, s.maxId),
			err_status.UidNotInRange)
	}
	if !s.store.IsLeader() {
		return nil, grpcerr.New(codes.PermissionDenied,
			err_status.ErrStr[err_status.NotLeader],
			err_status.NotLeader,
			&storesvr.Leader{Addr: s.leaderAPIAddr(), Nodes: s.allNodeAddr()})
	}
	val, err := s.store.Get(uid.Value)
	if err != nil {
		return nil, err
	}
	return &storesvr.MaxSeq{Value: val}, nil
}

func (s *MaxSeqHandle) RpcJoin(ctx context.Context, req *storesvr.ReqNodeJoin) (*storesvr.NoContent, error) {
	if !s.store.IsLeader() {
		return nil, grpcerr.New(codes.PermissionDenied,
			err_status.ErrStr[err_status.NotLeader],
			err_status.NotLeader,
			&storesvr.Leader{Addr: s.leaderAPIAddr(), Nodes: s.allNodeAddr()})
	}
	if err := s.store.Join(req.NodeId, req.Addr, req.Metadata); err != nil {
		return nil, err
	}
	return &storesvr.NoContent{}, nil
}

func (s *MaxSeqHandle) RpcGetAllSvrNode(ctx context.Context, req *storesvr.NoContent) (*storesvr.AllSvrNode, error) {
	if !s.store.IsLeader() {
		return nil, grpcerr.New(codes.InvalidArgument,
			err_status.ErrStr[err_status.NotLeader],
			err_status.NotLeader)
	}
	return &storesvr.AllSvrNode{
		Nodes: s.store.GetAllNodeMetaByKey(consts.META_KEY_API_ADDR),
	}, nil

}

func (s *MaxSeqHandle) leaderAPIAddr() string {
	id, err := s.store.LeaderID()
	if err != nil {
		return ""
	}
	logger.Debug("leader", zap.String("id", id))
	return s.store.GetMetadata(id, "api_addr")
}

func (s *MaxSeqHandle) allNodeAddr() []string {
	return s.store.GetAllNodeMetaByKey(consts.META_KEY_API_ADDR)
}
