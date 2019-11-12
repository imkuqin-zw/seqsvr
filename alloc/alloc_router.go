package alloc

type Node struct {
	Addr string `json:"addr"`
	Port string `json:"port"`
}

type Router struct {
	version uint64
}

func (r *Router) Watch() {

}
