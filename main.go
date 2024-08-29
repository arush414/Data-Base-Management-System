package main

import (
	"fmt"
)

/*A node consists of:
1. A fixed-sized header containing the type of the node (leaf node or internal node) and the number of keys.
2. A list of pointers to the child nodes. (Used by internal nodes).
3. A list of offsets pointing to each key-value pair.
4. Packed KV pairs:

| type	| nkeys |   pointers |  offsets 	| key-values
| 	2B 	| 	2B	| nkeys * 8B | nkeys * 2B 	| ...

This is the format of the KV pair. Lengths followed by data.
| klen | vlen | key | val |
| 	2B 	|	2B | ... | ... |
*/

// data types
type BNode struct {
	data []byte // can be dumped to the disk
}

const (
	BNODE_NODE = 0 //internal node without values
	BNODE_LEAF = 1 //leaf node with values
)

type BTree struct {
	root uint64 //in memory pointer to the root node
	//callbacks for managing on-disk pages
	get func(uint64) BNode //dereference a pointer
	new func(BNode) uint64 //allocate a page
	del func(uint64)       //deallocate a page
}

const (
	HEADER             = 4
	BTREE_PAGE_SIZE    = 4096
	BTREE_MAX_KEY_SIZE = 1000
	BTREE_MAX_VAL_SIZE = 3000
)

// General assert function , prints message when condition fails
func assert(condition bool, message string) {
	if !condition {
		panic(fmt.Sprintf("Condition Failed %s\n", message))
	}
}

// 4.2 init function for initialising nodes size
func init() {
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	assert(node1max <= BTREE_PAGE_SIZE, "Size Limit Exceeded") // maximum KV
}

func main() {

}
