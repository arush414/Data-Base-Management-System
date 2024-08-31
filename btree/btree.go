package btree

import (
	"github.com/infinity1729/Data-Base-Management-System/utils"
)


type BTree struct {
	root uint64 //in memory pointer to the root node
	//callbacks for managing on-disk pages
	get func(uint64) BNode //dereference a pointer
	new func(BNode) uint64 //allocate a page
	del func(uint64)       //deallocate a page
}

// check to verify that the constant values are within the limits
func init() {
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	utils.Assert(node1max <= BTREE_PAGE_SIZE, "Maximum Size Limit Exceeded") // maximum KV
}


//Deletion of the key-value pair(node) from the BTree
