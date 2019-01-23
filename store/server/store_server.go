package server

import (
	"github.com/imkuqin-zw/seqsvr/common"
	"os"
	"syscall"
)

type StoreServer struct {
	set common.Set
}

func (s *StoreServer) Init() {
	syscall.Mmap()
}
