package btree

// This file contains the unit tests to verify the BTree implementation.

import (
	"sort"
	"testing"
	"unsafe"

	"github.com/infinity1729/Data-Base-Management-System/utils"
)

type Checker struct {
	tree  BTree
	ref   map[string]string
	pages map[uint64]BNode
}

func newChecker() *Checker {
	pages := map[uint64]BNode{}
	return &Checker{
		tree: BTree{
			get: func(ptr uint64) BNode {
				node, ok := pages[ptr]
				utils.Assert(ok, "Page not present")
				return node
			},
			new: func(node BNode) uint64 {
				utils.Assert(node.nbytes() <= BTREE_PAGE_SIZE, "Node max size exceeded")
				key := uint64(uintptr(unsafe.Pointer(&node.data[0])))
				utils.Assert(pages[key].data == nil, "Page already present")
				pages[key] = node
				return key
			},
			del: func(ptr uint64) {
				_, ok := pages[ptr]
				utils.Assert(ok, "Page not present")
				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func (c *Checker) add(key string, val string) {
	c.tree.Insert([]byte(key), []byte(val))
	c.ref[key] = val
}

func (c *Checker) del(key string) bool {
	delete(c.ref, key)
	return c.tree.Delete([]byte(key))
}

func (c *Checker) dump() ([]string, []string) {
	keys := []string{}
	vals := []string{}

	var nodeDump func(uint64)
	nodeDump = func(ptr uint64) {
		node := c.tree.get(ptr)
		nkeys := node.nkeys()
		if node.btype() == BNODE_LEAF {
			for i := uint16(0); i < nkeys; i++ {
				keys = append(keys, string(node.getKey(i)))
				vals = append(vals, string(node.getVal(i)))
			}
		} else {
			for i := uint16(0); i < nkeys; i++ {
				ptr := node.getPtr(i)
				nodeDump(ptr)
			}
		}
	}

	nodeDump(c.tree.root)
	utils.Assert(keys[0] == "", "First key value not null")
	utils.Assert(vals[0] == "", "First key value not null")
	return keys[1:], vals[1:]
}

// the sorting interface
type sortIF struct {
	len  int
	less func(i, j int) bool
	swap func(i, j int)
}

func (self sortIF) Len() int {
	return self.len
}
func (self sortIF) Less(i, j int) bool {
	return self.less(i, j)
}
func (self sortIF) Swap(i, j int) {
	self.swap(i, j)
}

func (c *Checker) verify(t *testing.T) {
	keys, vals := c.dump()

	rkeys, rvals := []string{}, []string{}
	for k, v := range c.ref {
		rkeys = append(rkeys, k)
		rvals = append(rvals, v)
	}
	utils.Equal(t, len(rkeys), len(keys))
	sort.Stable(sortIF{
		len:  len(rkeys),
		less: func(i, j int) bool { return rkeys[i] < rkeys[j] },
		swap: func(i, j int) {
			k, v := rkeys[i], rvals[i]
			rkeys[i], rvals[i] = rkeys[j], rvals[j]
			rkeys[j], rvals[j] = k, v
		},
	})

	utils.Equal(t, rkeys, keys)
	utils.Equal(t, rvals, vals)

	// Verify the tree structure (if the child pointer points to correct key)
	var nodeVerify func(BNode)
	nodeVerify = func(node BNode) {
		nkeys := node.nkeys()
		utils.Assert(nkeys >= 1, "Node has no keys")
		if node.btype() == BNODE_LEAF {
			return
		}
		for i := uint16(0); i < nkeys; i++ {
			key := node.getKey(i)
			kid := c.tree.get(node.getPtr(i))
			utils.Equal(t, key, kid.getKey(0))
			nodeVerify(kid)
		}
	}
	nodeVerify(c.tree.get(c.tree.root))
}
