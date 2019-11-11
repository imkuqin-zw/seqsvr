package main

import (
	"fmt"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"path/filepath"
	"seqsvr/common"
	"seqsvr/lib/logger"
	"seqsvr/store/config"
	"seqsvr/store/consts"
	"seqsvr/store/rpc"
	"seqsvr/store/service"
	"syscall"
)

func main() {
	conf, err := config.LoadConfig()
	if err != nil {
		panic(fmt.Sprintf("load config file: %s", err.Error()))
	}
	logger.InitLogger(conf.Log)
	store, err := service.NewStore(conf.StoreConf)
	if err != nil {
		logger.Fatal("NewStoreServer", zap.Error(err))
		return
	}
	// Is this a brand new node?
	dbPath := filepath.Join(conf.StoreConf.Raft.RaftDir, fmt.Sprintf("raft_%s.db", conf.StoreConf.Raft.RaftID))
	newNode := !common.PathExists(filepath.Join(conf.StoreConf.Raft.RaftDir, dbPath))
	bootstrap := conf.Bootstrap && newNode
	if err = store.Open(bootstrap); err != nil {
		logger.Panic("store open", zap.Error(err))
		return
	}
	defer func() {
		if err := store.Close(true); err != nil {
			logger.Error("failed to close store", zap.Error(err))
		}
	}()
	meta := map[string]string{
		consts.META_KEY_API_ADDR: conf.RpcConf.SvrAddr,
	}

	if !bootstrap {
		rpc.Join(conf.RpcConf, conf.StoreConf.Raft.TcpAddr, conf.StoreConf.Raft.RaftID, meta)
	}

	store.WaitForLeader(conf.StoreConf.Raft.OpenTimeout)
	store.WaitForApplied(conf.StoreConf.Raft.ApplyTimeout)

	// This may be a standalone server. In that case set its own metadata.
	if err := store.SetMetadata(meta); err != nil && err != service.ErrNotLeader {
		// Non-leader errors are OK, since metadata will then be set through
		// consensus as a result of a join. All other errors indicate a problem.
		logger.Fatal("failed to set store metadata", zap.Error(err))
	}

	go func() {
		maxSeqHandle := rpc.NewMaxSeqHandle(conf.StoreConf.IdBegin, conf.StoreConf.Size, store)
		server := rpc.NewServer(*(conf.RpcConf), maxSeqHandle)
		if err := server.Run(); err != nil {
			logger.Panic("server run ", zap.Error(err))
		}
	}()

	// Block until signalled.
	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Kill, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	<-terminate
	logger.Info("store server stopped")
}
