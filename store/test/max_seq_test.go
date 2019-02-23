package test

import (
	"testing"
	"context"
	"github.com/imkuqin-zw/seqsvr/protobuf/storesvr"
	"fmt"
)

func TestRpcUpdateMaxSeq(t *testing.T) {
	ctx := context.Background()
	req := &storesvr.UidMaxSeq{Uid:1, MaxSeq:4}
	_, err := Client.RpcUpdateMaxSeq(ctx, req)
	if err != nil {
		fmt.Println("RpcUpdateMaxSeq: ", err.Error())
	}
	return
}

func TestRpcGetSeqMax(t *testing.T) {
	ctx := context.Background()
	req := &storesvr.Uid{Value:1}
	res, err := Client.RpcGetSeqMax(ctx, req)
	if err != nil {
		fmt.Println("RpcGetSeqMax: ", err.Error())
	}
	fmt.Println("max seq", res.Value)
	return
}