package test

import (
	"seqsvr/protobuf/storesvr"
)

var Client storesvr.StoreServerClient

func init() {
	//*config.ConfPath = "../store.yaml"
	//conf, err := config.LoadConfig()
	//if err != nil {
	//	panic(fmt.Sprintf("load config file: %s", err.Error()))
	//}
	//
	//Client = storesvr.NewStoreServerClient()
}
