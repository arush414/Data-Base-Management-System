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
func (tree *BTree) Delete(key []byte) bool {
	if !(len(key) != 0) {
		panic("Key is not equal to 0")
	}

	if !(len(key) <= BTREE_MAX_KEY_SIZE) {
		panic("length of key is greater than MAX KEY SIZE")
	}

	//if the tree is empty
	if tree.root == 0 {
		return false
	}

	updated := treeDelete(tree, tree.get(tree.root), key)
	if len(updated.data) == 0 {
		return false //not found
	}

	tree.del(tree.root)
	if updated.btype() == BNODE_NODE && updated.nkeys() == 1 {
		// remove a level, since root has only 1 child
		tree.root = updated.getPtr(0)
	} else {
		tree.root = tree.new(updated)
	}

	return true
}