package sortedmap

import (
	"testing"
)

func TestLLRBTreeReset(t *testing.T) {
	context := &commonLLRBTreeTestContextStruct{t: t}
	context.tree = NewLLRBTree(CompareInt, context)
	context.tree.Reset()
}
