package grpcerr

import (
	"context"
	"encoding/base64"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func (e *Error) Error() string {
	return e.String()
}

func New(code codes.Code, msg string, errCode uint32, details ...proto.Message) error {
	grpcErr := &Error{
		Code:    uint32(code),
		Message: msg,
		ErrCode: errCode,
	}
	if len(details) > 0 {
		for _, item := range details {
			any, err := ptypes.MarshalAny(item)
			if err != nil {
				return status.Errorf(code, msg)
			}
			grpcErr.Detail = append(grpcErr.Detail, any)
		}
	}
	return grpcErr
}

func MarshalError(err error, ctx context.Context) error {
	rerr, ok := err.(*Error)
	if !ok {
		return err
	}
	pberr, marshalerr := proto.Marshal(rerr)
	if marshalerr == nil {
		md := metadata.Pairs("rpc-error", base64.StdEncoding.EncodeToString(pberr))
		_ = grpc.SetTrailer(ctx, md)
	}
	return status.Errorf(codes.Code(rerr.Code), rerr.Message)
}

func UnmarshalError(err error, md metadata.MD) error {
	vals, ok := md["rpc-error"]
	if !ok {
		return err
	}
	buf, err := base64.StdEncoding.DecodeString(vals[0])
	if err != nil {
		return err
	}
	var grpcErr Error
	if err := proto.Unmarshal(buf, &grpcErr); err != nil {
		return err
	}
	return &grpcErr
}

func UnaryServerInterceptor(ctx context.Context, req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	resp, err := handler(ctx, req)
	err = MarshalError(err, ctx)
	return resp, err
}

func UnaryClientInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	md := metadata.MD{}
	opts = append(opts, grpc.Trailer(&md))
	err := invoker(ctx, method, req, reply, cc, opts...)
	if err != nil {
		return UnmarshalError(err, md)
	}
	return err
}
