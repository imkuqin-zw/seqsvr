package alloc

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"strconv"
)

type Consistence interface {
}

type EtcdConsistence struct {
	etcdV3 *clientv3.Client
	ctx    context.Context
	rsp    chan interface{}
}

func (c *EtcdConsistence) Watch(router string) {
	rch := c.etcdV3.Watch(context.Background(), router, clientv3.WithPrefix())
	for {
		select {
		case rsp := <-rch:
			for _, ev := range rsp.Events {
				version, err := strconv.Atoi(string(ev.Kv.Value))
				if err != nil {

				}

				fmt.Printf("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
			}
			c.rsp <- rsp
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *EtcdConsistence) GetChange() <-chan interface{} {
	return c.rsp
}
