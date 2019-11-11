package alloc

import (
	"context"
	"seqsvr/protobuf/allocsvr"
)

type AllocSrv struct {
}

func (s *AllocSrv) FetchNextSeqNum(ctx context.Context, in *allocsvr.Uid) (*allocsvr.SeqNum, error) {

}
