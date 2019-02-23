package client

import (
	"github.com/imkuqin-zw/seqsvr/protobuf/storesvr"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"github.com/imkuqin-zw/seqsvr/lib/logger"
	"errors"
	"sync"
	"time"
	"context"
	"github.com/imkuqin-zw/seqsvr/lib/grpcerr"
	"sync/atomic"
	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
)

var NotUsableClientErr = errors.New("not usable client")
var TimeoutErr = errors.New("request timeout")

type Cluster struct {
	sync.RWMutex
	nodes            []string
	leader           string
	leaderClient     storesvr.StoreServerClient
	tls              int64
	closed           int32
	closeUpdateNodes chan bool
}

func NewCluster(nodes []string, duration time.Duration) (*Cluster, error) {
	cluster := &Cluster{
		nodes:            nodes,
		closeUpdateNodes: make(chan bool, 1),
	}
	for _, node := range nodes {
		interceptor := grpc_middleware.ChainUnaryClient(
			grpc_zap.UnaryClientInterceptor(logger.Logger),
			UnaryClientInterceptor,
			grpcerr.UnaryClientInterceptor,
		)
		conn, err := grpc.Dial(node, grpc.WithInsecure(), grpc.WithUnaryInterceptor(interceptor))
		if err != nil {
			logger.Debug("did not connect", zap.String("addr", node), zap.Error(err))
			continue
		}
		cluster.leader = node
		cluster.leaderClient = storesvr.NewStoreServerClient(conn)
		break
	}
	if cluster.leaderClient == nil {
		return nil, NotUsableClientErr
	}
	cluster.tls = time.Now().UnixNano()
	go cluster.UpdateNodes(duration)
	return cluster, nil
}

func (c *Cluster) UpdateNodes(duration time.Duration) {
	t := time.NewTicker(duration)
	for {
		select {
		case <-t.C:
			c.updateNodes()
		case <-c.closeUpdateNodes:
			logger.Infof("cluster client closed")
			return
		}
	}
}

func (c *Cluster) Close() {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return
	}
	c.closeUpdateNodes <- true
}

func (c *Cluster) updateNodes() {
	c.Lock()
	defer c.Unlock()
	tls := time.Now().UnixNano()
	if tls < c.tls {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	unusable := map[string]bool{c.leader: true}
	client := c.leaderClient
	leader := c.leader
A:
	res, err := client.RpcGetAllSvrNode(ctx, &storesvr.NoContent{})
	if err == nil {
		c.leaderClient = client
		c.leader = leader
		c.nodes = res.Nodes
		c.tls = tls
		return
	}
	logger.Debug("cluster UpdateNodes Leader", zap.Error(err))
	for _, node := range c.nodes {
		if _, ok := unusable[node]; ok {
			continue
		}
		interceptor := grpc_middleware.ChainUnaryClient(
			grpc_zap.UnaryClientInterceptor(logger.Logger),
			UnaryClientInterceptor,
			grpcerr.UnaryClientInterceptor,
		)
		conn, err := grpc.Dial(node, grpc.WithInsecure(), grpc.WithUnaryInterceptor(interceptor))
		if err != nil {
			logger.Debug("did not connect", zap.String("addr", node), zap.Error(err))
			continue
		}
		client = storesvr.NewStoreServerClient(conn)
		unusable[node] = true
		goto A
	}
	logger.Debug("cluster UpdateNodes Not Node can be used")
	return
}

func (c *Cluster) GetLeader() storesvr.StoreServerClient {
	c.RLock()
	defer c.RUnlock()
	return c.leaderClient
}

func (c *Cluster) GetMaxSeq(ctx context.Context, uid uint32) (uint64, error) {
	for {
		select {
		case <-ctx.Done():
			return 0, TimeoutErr
		default:
			leader := c.GetLeader()
			result, err := leader.RpcGetSeqMax(ctx, &storesvr.Uid{Value: 1})
			if err == nil {
				return result.Value, nil
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func (c *Cluster) UpdateMaxSeq(ctx context.Context, uid uint32, maxSeq uint64) error {
	for {
		select {
		case <-ctx.Done():
			return TimeoutErr
		default:
			leader := c.GetLeader()
			_, err := leader.RpcUpdateMaxSeq(ctx, &storesvr.UidMaxSeq{Uid: 1, MaxSeq: maxSeq})
			if err == nil {
				return nil
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

//func (c *Cluster) ChangeLeader(node string, nodes []string) error {
//	c.Lock()
//	defer c.Unlock()
//	tls := time.Now().UnixNano()
//	if tls < c.tls || c.leader == node {
//		return nil
//	}
//	conn, err := grpc.Dial(node, grpc.WithInsecure(), grpc.WithUnaryInterceptor(UnaryClientInterceptor))
//	if err == nil {
//		c.leader = node
//		c.leaderClient = storesvr.NewStoreServerClient(conn)
//		c.tls = tls
//		c.nodes = nodes
//		return nil
//	} else {
//		var unusableNode = map[string]bool{c.leader: true, node: true}
//		logger.Debug("did not connect", zap.String("addr", node), zap.Error(err))
//		for _, item := range c.nodes {
//			if _, ok := unusableNode[item]; !ok {
//				conn, err = grpc.Dial(node, grpc.WithInsecure(), grpc.WithUnaryInterceptor(UnaryClientInterceptor))
//				if err != nil {
//					logger.Debug("did not connect", zap.String("addr", node), zap.Error(err))
//					continue
//				}
//				c.leader = item
//				c.tls = tls
//				c.leaderClient = storesvr.NewStoreServerClient(conn)
//				return nil
//			}
//		}
//		return NotUsableClient
//	}
//	return nil
//}
