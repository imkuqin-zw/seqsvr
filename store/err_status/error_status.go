package err_status

const (
	_             uint32 = iota
	NotLeader
	UidNotInRange
)

var ErrStr = map[uint32]string{
	NotLeader:     "not leader",
	UidNotInRange: "uid %v no longer between %v and %v",
}
