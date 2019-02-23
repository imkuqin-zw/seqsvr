package test

import (
	"github.com/imkuqin-zw/seqsvr/protobuf/storesvr"
	"fmt"
	"github.com/imkuqin-zw/seqsvr/store/config"
)

var Client storesvr.StoreServerClient

func init() {
	*config.ConfPath = "../store.yaml"
	conf, err := config.LoadConfig()
	if err != nil {
		panic(fmt.Sprintf("load config file: %s", err.Error()))
	}

	Client = storesvr.NewStoreServerClient()
}
