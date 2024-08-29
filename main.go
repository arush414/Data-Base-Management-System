package main

import (
	"encoding/binary"
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
	assert(node1max <= BTREE_PAGE_SIZE, "Maximum Size Limit Exceeded") // maximum KV
}

// Header Functions

// btype function for getting the information about the node ie internal node or leaf node
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node.data[0:2])
}

// nkeys function to get number of keys in the node
func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

// setHeader function to set or update the header of the given node
func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

// Pointers Functions

// getPtr function to get the specific pointer of the bnode given index of the pointer
func (node BNode) getPtr(idx uint16) uint64 {
	assert(0 <= idx && idx < node.nkeys(), "Index value is not present between 0 and nkeys-1")
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node.data[pos:])
}

// setPtr function to set the specific pointer of the bnode given index of the pointer
func (node BNode) setPtr(idx uint16, val uint64) {
	assert(0 <= idx && idx < node.nkeys(), "Index value is not present between 0 and nkeys-1")
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node.data[pos:], val)
}

// Offset Functions

// offsetPos function to get the specific offset position of the bnode given the index
// !Internal Function
func offsetPos(node BNode, idx uint16) uint16 {
	assert(1 <= idx && idx <= node.nkeys(), "Index value is not present between 1 and nkeys")
	return HEADER + 8*node.nkeys() + 2*(idx-1) // using 1 based indexing
}

// getOffset function to get the offset data given the index value
func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node.data[offsetPos(node, idx):])
}

// setOffset function to set the offset data given the index value
func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node.data[offsetPos(node, idx):], offset)
}

// key-values

// kvPos function to get starting index position for 'idx' key value pair
func (node BNode) kvPos(idx uint16) uint16 {
	assert(idx >= 0 && idx <= node.nkeys(), "Index is not present between 0 and nkeys")
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

// getKey function to get the keys given the idx value
func (node BNode) getKey(idx uint16) []byte {
	assert(idx >= 0 && idx < node.nkeys(), "Index is not present between 0 and nkeys")
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	return node.data[pos+4:][:klen]
}

// getVal function to the values given the idx value
func (node BNode) getVal(idx uint16) []byte {
	assert(idx < node.nkeys(), "Index is not present between 0 and nkeys")
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos+0:])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
	return node.data[pos+4+klen:][:vlen]
}

// get size of node
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

func main() {

}
