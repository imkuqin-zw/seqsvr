package client

import (
	"github.com/imkuqin-zw/seqsvr/lib/grpcerr"
	"github.com/imkuqin-zw/seqsvr/store/err_status"
	"github.com/imkuqin-zw/seqsvr/protobuf/storesvr"
	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"context"
	"google.golang.org/grpc"
	"github.com/golang/protobuf/ptypes"
	"github.com/imkuqin-zw/seqsvr/lib/logger"
)

func UnaryClientInterceptor(ctx context.Context, method string,
	req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
) error {
	for {
		err := invoker(ctx, method, req, reply, cc, opts...)
		if err != nil {
			if grpcErr, ok := err.(*grpcerr.Error); ok {
				if grpcErr.ErrCode == uint32(err_status.NotLeader) {
					if len(grpcErr.Detail) > 0 {
						var err1 error
						var leader storesvr.Leader
						if err1 = ptypes.UnmarshalAny(grpcErr.Detail[0], &leader); err1 != nil || leader.Addr == "" {
							return err
						}
						interceptor := grpc_middleware.ChainUnaryClient(
							grpc_zap.UnaryClientInterceptor(logger.Logger),
							grpcerr.UnaryClientInterceptor,
						)
						cc, err1 = grpc.Dial(leader.Addr, grpc.WithInsecure(), grpc.WithUnaryInterceptor(interceptor))
						if err1 != nil {
							return err
						}
						continue
					}
				}
			}
			return err
		}
		break
	}
	return nil
}
