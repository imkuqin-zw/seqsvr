package config

import (
	"io/ioutil"
	"flag"
	"gopkg.in/yaml.v2"
	"go.uber.org/zap"
	"time"
)

var ConfPath = flag.String("conf", "./store.yaml", "config path")

type Config struct {
	Bootstrap bool        `yaml:"bootstrap"`
	StoreConf *StoreConf  `yaml:"store_config"`
	Log       *zap.Config `yaml:"log"`
	RpcConf   *RpcConf    `yaml:"rpc_conf"`
}

type RpcConf struct {
	SvrAddr  string   `yaml:"svr_addr"`
	JoinAddr []string `yaml:"join_addr"`
}

//func (conf *RpcConf) GetTag() string {
//	return conf.tag
//}

type StoreConf struct {
	DataFileDir string    `yaml:"data_file_dir"`
	IdBegin     uint32    `yaml:"id_begin"`
	Size        uint32    `yaml:"size"`
	Raft        *RaftConf `yaml:"raft"`
}

type RaftConf struct {
	RaftDir           string        `yaml:"raft_dir"`
	TcpAddr           string        `yaml:"tcp_addr"`
	RaftID            string        `yaml:"raft_id"`
	ApplyTimeout      time.Duration `yaml:"apply_timeout"`
	OpenTimeout       time.Duration `yaml:"open_timeout"`
	SnapshotInterval  time.Duration `yaml:"snapshot_interval"`
	SnapshotThreshold uint64        `taml:"snapshot_threshold"`
}

func LoadConfig() (conf *Config, err error) {
	flag.Parse()
	var configBody []byte
	configBody, err = ioutil.ReadFile(*ConfPath)
	if err != nil {
		return
	}
	conf = &Config{}
	if err = yaml.Unmarshal(configBody, conf); err != nil {
		return
	}
	return
}
