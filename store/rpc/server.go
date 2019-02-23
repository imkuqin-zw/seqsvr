package rpc

import (
	"github.com/imkuqin-zw/seqsvr/protobuf/storesvr"
	"github.com/imkuqin-zw/seqsvr/store/config"
	"net"
	"github.com/imkuqin-zw/seqsvr/lib/logger"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"github.com/imkuqin-zw/seqsvr/lib/grpcerr"
	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
)

type Server struct {
	conf         config.RpcConf
	maxSeqHandle *MaxSeqHandle
}

func NewServer(conf config.RpcConf, maxHandle *MaxSeqHandle) *Server {
	return &Server{
		conf:         conf,
		maxSeqHandle: maxHandle,
	}
}

func (s *Server) Run() error {
	lis, err := net.Listen("tcp", s.conf.SvrAddr)
	if err != nil {
		logger.Panic("failed to listen ", zap.Error(err))
	}
	svr := grpc.NewServer(grpc_middleware.WithUnaryServerChain(
		grpc_zap.UnaryServerInterceptor(logger.Logger),
		grpcerr.UnaryServerInterceptor,
	))
	storesvr.RegisterStoreServerServer(svr, s.maxSeqHandle)
	logger.Info("grpc server start", zap.String("addr", s.conf.SvrAddr))
	return svr.Serve(lis)
}
