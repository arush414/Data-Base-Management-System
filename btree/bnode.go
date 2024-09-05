package btree

import (
	"encoding/binary"

	"github.com/infinity1729/Data-Base-Management-System/utils"
)

type BNode struct {
	data []byte // can be dumped to the disk
}

// Header Functions

// btype function for getting the information about the node ie internal node or leaf node
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node.data[0:2])
}

// nkeys function: get number of keys in the node
func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

// setHeader function: set or update the header of the given node
func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

// Pointers Functions

// getPtr function: get the child pointer of the bnode given index of the pointer
func (node BNode) getPtr(idx uint16) uint64 {
	utils.Assert(0 <= idx && idx < node.nkeys(), "Index value is not present between 0 and nkeys-1")
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node.data[pos:])
}

// setPtr function: set the child pointer of the bnode given index of the pointer
func (node BNode) setPtr(idx uint16, val uint64) {
	utils.Assert(0 <= idx && idx < node.nkeys(), "Index value is not present between 0 and nkeys-1")
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node.data[pos:], val)
}

// Offset Functions

// getOffset function: return the offset position of KV pair given the index value
func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node.data[offsetPos(node, idx):])
}

// setOffset function: set the offset data given the index value
func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node.data[offsetPos(node, idx):], offset)
}

// key-values

// kvPos function: get starting offset position for 'idx' key value pair
func (node BNode) kvPos(idx uint16) uint16 {
	utils.Assert(idx >= 0 && idx <= node.nkeys(), "Index is not present between 0 and nkeys")
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

// getKey function: get the key given the idx value
func (node BNode) getKey(idx uint16) []byte {
	utils.Assert(idx >= 0 && idx < node.nkeys(), "Index is not present between 0 and nkeys")
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	return node.data[pos+4:][:klen]
}

// getVal function: the values given the idx value
func (node BNode) getVal(idx uint16) []byte {
	utils.Assert(idx < node.nkeys(), "Index is not present between 0 and nkeys")
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos+0:])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
	return node.data[pos+4+klen:][:vlen]
}

// get size of node
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}
