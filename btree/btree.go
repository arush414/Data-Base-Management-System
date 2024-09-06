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

// the interface
func (tree *BTree) Insert(key []byte, val []byte) {
	utils.Assert(len(key) != 0, "Nothing to insert length of key is 0")
	utils.Assert(len(key) <= BTREE_MAX_KEY_SIZE, "Size of the key exceeds maximum key size")
	utils.Assert(len(val) <= BTREE_MAX_VAL_SIZE, "Size of the value exceeds maximum value size")
	if tree.root == 0 {
		// create the first node
		root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		root.setHeader(BNODE_LEAF, 2)
		// a dummy key, this makes the tree cover the whole key space.
		// thus a lookup can always find a containing node (nodeLookupLE) function.
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, val)
		tree.root = tree.new(root) // pointer to the root
		return
	}
	node := tree.get(tree.root) // get the root
	tree.del(tree.root)         // delete the root
	node = treeInsert(tree, node, key, val)
	nsplit, splitted := nodeSplit3(node)
	if nsplit > 1 {
		// the root was split, add a new level.
		root := BNode{data: make([]byte, BTREE_PAGE_SIZE)} // new root
		root.setHeader(BNODE_NODE, nsplit)                 // number of keys is simply number of splits
		for i, knode := range splitted[:nsplit] {
			ptr, key := tree.new(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.new(root) // update the root
	} else {
		tree.root = tree.new(splitted[0]) // incase lower nodes are splitted so updated the root
	}
}
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