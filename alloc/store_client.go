package alloc

type StoreClient interface {
	UpdateMaxSeqNum(uid uint32) uint64
	GetCurMaxSeqNum(uid uint32) uint64
}
