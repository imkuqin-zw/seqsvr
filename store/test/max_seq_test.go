package test

import (
	"context"
	"fmt"
	"seqsvr/protobuf/storesvr"
	"sync"
	"testing"
	"time"
)

func TestRpcUpdateMaxSeq(t *testing.T) {
	ctx := context.Background()
	req := &storesvr.UidMaxSeq{Uid: 1, MaxSeq: 4}
	_, err := Client.RpcUpdateMaxSeq(ctx, req)
	if err != nil {
		fmt.Println("RpcUpdateMaxSeq: ", err.Error())
	}
	return
}

func TestRpcGetSeqMax(t *testing.T) {
	ctx := context.Background()
	req := &storesvr.Uid{Value: 1}
	res, err := Client.RpcGetSeqMax(ctx, req)
	if err != nil {
		fmt.Println("RpcGetSeqMax: ", err.Error())
	}
	fmt.Println("max seq", res.Value)
	return
}

func TestMuChan(t *testing.T) {
	chanal := make(chan bool)
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		select {
		case <-chanal:
			fmt.Println("1")
			return
		}
	}()
	go func() {
		defer wg.Done()
		select {
		case <-chanal:
			fmt.Println("2")
			return
		}
	}()

	go func() {
		time.Sleep(3 * time.Second)
		close(chanal)
	}()
	wg.Wait()
}
