package sortedmap

type LLRBTree interface {
	SortedMap
	Reset()
}

// LLRBTreeCallbacks specifies the interface to a set of callbacks provided by the client
type LLRBTreeCallbacks interface {
	DumpCallbacks
}

func NewLLRBTree(compare Compare, callbacks LLRBTreeCallbacks) (tree LLRBTree) {
	tree = &llrbTreeStruct{Compare: compare, LLRBTreeCallbacks: callbacks, root: nil}
	return
}
