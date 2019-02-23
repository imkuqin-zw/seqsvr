package client

import (
	"testing"
	"fmt"
	"context"
	"time"
	"github.com/imkuqin-zw/seqsvr/protobuf/storesvr"
	"github.com/imkuqin-zw/seqsvr/lib/logger"
	"go.uber.org/zap"
)

var loggerConfig = &zap.Config{
	Level:            zap.NewAtomicLevelAt(zap.DebugLevel),
	OutputPaths:      []string{"stdout"},
	ErrorOutputPaths: []string{"stdout"},
	Encoding:         "console",
	Development:      false,
}

func TestCluster_RpcGetMaxSeq(t *testing.T) {
	logger.InitLogger(loggerConfig)
	cluster, err := NewCluster([]string{"127.0.0.1:30003", "127.0.0.1:30001", "127.0.0.1:30001"}, time.Second*5)
	if err != nil {
		fmt.Println("new cluster", err.Error())
		return
	}
	defer cluster.Close()
	ctx := context.Background()
	for {
		leader := cluster.GetLeader()
		result, err := leader.RpcGetSeqMax(ctx, &storesvr.Uid{Value: 1})
		if err != nil {
			fmt.Println("rpc error", err.Error())
			time.Sleep(500 * time.Millisecond)
			continue
		}
		fmt.Println(result.Value)
		return
	}
	return
}

func TestCluster_RpcUpdateMaxSeq(t *testing.T) {
	logger.InitLogger(loggerConfig)
	cluster, err := NewCluster([]string{"127.0.0.1:30001", "127.0.0.1:30002", "127.0.0.1:30003"}, time.Second*5)
	if err != nil {
		fmt.Println("new cluster", err.Error())
		return
	}
	defer cluster.Close()
	ctx := context.Background()
	for {
		leader := cluster.GetLeader()
		_, err = leader.RpcUpdateMaxSeq(ctx, &storesvr.UidMaxSeq{Uid: 1, MaxSeq: 10000})
		if err != nil {
			fmt.Println("rpc error", err.Error())
			time.Sleep(500 * time.Millisecond)
			continue
		}
		break
	}
	fmt.Println("SUCCESS")
	return
}


func TestCluster_GetMaxSeq(t *testing.T) {
	logger.InitLogger(loggerConfig)
	cluster, err := NewCluster([]string{"127.0.0.1:30003", "127.0.0.1:30001", "127.0.0.1:30001"}, time.Second*5)
	if err != nil {
		fmt.Println("new cluster", err.Error())
		return
	}
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	num, err := cluster.GetMaxSeq(ctx, 1)
	if err != nil {
		fmt.Println("GetMaxSeq", err.Error())
	}
	fmt.Println(num, err)
}

func TestCluster_UpdateMaxSeq(t *testing.T) {
	logger.InitLogger(loggerConfig)
	cluster, err := NewCluster([]string{"127.0.0.1:30003", "127.0.0.1:30001", "127.0.0.1:30001"}, time.Second*5)
	if err != nil {
		fmt.Println("new cluster", err.Error())
		return
	}
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = cluster.UpdateMaxSeq(ctx, 1, 20000)
	if err != nil {
		fmt.Println("GetMaxSeq", err.Error())
	}
	fmt.Println("success")
}