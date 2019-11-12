package alloc

import (
	"context"
	"google.golang.org/grpc/codes"
	"seqsvr/lib/grpcerr"
	"seqsvr/protobuf/allocsvr"
)

type Server struct {
	service *Service
}

func (s *Server) FetchNextSeqNum(ctx context.Context, in *allocsvr.Uid) (*allocsvr.SeqNum, error) {
	seqNum, b, err := s.service.FetchNextSeqNum(in.Uid, in.Version)
	if err != nil {
		if err == ErrMigrate {
			return nil, grpcerr.New(codes.Unavailable, err.Error(), 14)
		}
	}
	rsp := &allocsvr.SeqNum{SeqNum: seqNum}
	if b {
		router, err := s.service.GetRouter()
		if err != nil {

		} else {
			rsp.Router = make(map[string]*allocsvr.SectionIdArr)
			for k, v := range router {
				tmp := &allocsvr.SectionIdArr{Val: v}
				rsp.Router[k] = tmp
			}
		}
	}

	return rsp, nil
}
