package iavl

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/cosmos/iavl/v2/metrics"
	"github.com/dustin/go-humanize"
	"github.com/emicklei/dot"
	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
)

var log = zlog.Output(zerolog.ConsoleWriter{
	Out:        os.Stderr,
	TimeFormat: time.Stamp,
})

type Tree struct {
	version        int64
	root           *Node
	metrics        *metrics.TreeMetrics
	sql            *SqliteDb
	lastCheckpoint int64
	orphans        []*nodeDiff
	pool           *NodePool

	workingBytes uint64
	workingSize  int64

	// options
	maxWorkingSize uint64

	branches []*Node
	leaves   []*Node
	sequence uint32

	// debug
	// when emitDotGraphs is set to true, the tree will emit a dot graph after each Set, Remove and rotate operation
	emitDotGraphs bool
	lastDotGraph  *dot.Graph
	dotGraphs     []*dot.Graph

	leafSequence     leafSeq
	lastLeafSequence leafSeq
}

func NewTree(sql *SqliteDb, pool *NodePool) *Tree {
	tree := &Tree{
		sql:            sql,
		pool:           pool,
		metrics:        &metrics.TreeMetrics{},
		maxWorkingSize: 2 * 1024 * 1024 * 1024,
		lastDotGraph:   dot.NewGraph(dot.Directed),
	}
	tree.sql.metrics = tree.metrics
	return tree
}

func (tree *Tree) LoadVersion(version int64) error {
	if tree.sql == nil {
		return fmt.Errorf("sql is nil")
	}

	tree.version = version
	tree.root = nil
	tree.orphans = nil
	tree.workingBytes = 0
	tree.workingSize = 0

	var err error
	tree.root, err = tree.sql.LoadRoot(version)
	if err != nil {
		return err
	}
	// TODO
	tree.lastCheckpoint = version
	tree.leafSequence, err = tree.sql.getLastLeafSeq()
	tree.lastLeafSequence = tree.leafSequence
	if err != nil {
		return err
	}
	return nil
}

func (tree *Tree) WarmTree() error {
	var i int
	start := time.Now()
	log.Info().Msgf("loading tree into memory version=%d", tree.version)
	loadTreeSince := time.Now()
	tree.loadTree(&i, &loadTreeSince, tree.root)
	log.Info().Msgf("loaded %s tree nodes into memory version=%d dur=%s",
		humanize.Comma(int64(i)),
		tree.version, time.Since(start).Round(time.Millisecond))
	err := tree.sql.WarmLeaves()
	if err != nil {
		return err
	}
	return tree.sql.queryReport(5)
}

func (tree *Tree) loadTree(i *int, since *time.Time, node *Node) *Node {
	if node.isLeaf() {
		return nil
	}
	*i++
	if *i%1_000_000 == 0 {
		log.Info().Msgf("loadTree i=%s, r/s=%s",
			humanize.Comma(int64(*i)),
			humanize.Comma(int64(1_000_000/time.Since(*since).Seconds())),
		)
		*since = time.Now()
	}
	// balanced subtree with two leaves, skip 2 queries
	if node.subtreeHeight == 1 || (node.subtreeHeight == 2 && node.size == 3) {
		return node
	}

	node.leftNode = tree.loadTree(i, since, node.left(tree))
	node.rightNode = tree.loadTree(i, since, node.right(tree))
	return node
}

func (tree *Tree) SaveVersion() ([]byte, int64, error) {
	tree.version++
	sqlSave, err := newSqlSaveVersion(tree.sql, tree)
	if err != nil {
		return nil, 0, err
	}
	sqlSave.saveBranches = tree.version == 1

	start := time.Now()
	if err = sqlSave.deepSave(tree.root); err != nil {
		return nil, 0, fmt.Errorf("failed to save tree: %w", err)
	}
	if err = sqlSave.finish(); err != nil {
		return nil, 0, err
	}

	if tree.version == 1 {
		// TODO this is a hack for tests
		if err = tree.sql.write.Exec("CREATE INDEX leaf_idx on leaf (seq);"); err != nil {
			return nil, 0, err
		}
		if err = tree.sql.write.Exec("CREATE INDEX branch_idx on branch (sort_key);"); err != nil {
			return nil, 0, err
		}
	}

	dur := time.Since(start)
	tree.metrics.WriteDurations = append(tree.metrics.WriteDurations, dur)
	tree.metrics.WriteTime += dur

	if err = tree.sql.SaveRoot(tree.version, tree.root); err != nil {
		return nil, 0, fmt.Errorf("failed to save root: %w", err)
	}

	tree.lastLeafSequence = tree.leafSequence
	//tree.sql.queryLeaf, tree.sql.queryBranch = nil, nil
	return tree.root.hash, tree.version, nil
}

type saveStats struct {
	nodeBz uint64
	dbBz   uint64
	count  int64
}

// Set sets a key in the working tree. Nil values are invalid. The given
// key/value byte slices must not be modified after this call, since they point
// to slices stored within IAVL. It returns true when an existing value was
// updated, while false means it was a new key.
func (tree *Tree) Set(key, value []byte) (updated bool, err error) {

	updated, err = tree.set(key, value)
	if err != nil {
		return false, err
	}
	if updated {
		tree.metrics.TreeUpdate++
	} else {
		tree.metrics.TreeNewNode++
	}

	tree.emitDotGraph(tree.root)

	return updated, nil
}

func (tree *Tree) set(key []byte, value []byte) (updated bool, err error) {
	if value == nil {
		return updated, fmt.Errorf("attempt to store nil value at key '%s'", key)
	}

	if tree.root == nil {
		tree.root = tree.NewNode(key, value)
		return updated, nil
	}

	tree.root, updated, err = tree.recursiveSet(tree.root, key, value)
	return updated, err
}

func (tree *Tree) recursiveSet(node *Node, key []byte, value []byte) (
	newSelf *Node, updated bool, err error,
) {
	if node == nil {
		panic("node is nil")
	}
	if node.isLeaf() {
		switch bytes.Compare(key, node.key) {
		case -1: // setKey < leafKey
			tree.metrics.PoolGet += 2
			parent := tree.pool.Get()
			parent.sortKey = MinRightToken(key, node.key)
			parent.version = tree.version + 1
			parent.key = node.key
			parent.subtreeHeight = 1
			parent.size = 2
			parent.dirty = true
			parent.setLeft(tree.NewNode(key, value))
			parent.setRight(node)

			tree.workingBytes += parent.sizeBytes()
			tree.workingSize++
			return parent, false, nil
		case 1: // setKey > leafKey
			tree.metrics.PoolGet += 2
			parent := tree.pool.Get()
			parent.sortKey = MinRightToken(key, node.key)
			parent.version = tree.version + 1
			parent.key = key
			parent.subtreeHeight = 1
			parent.size = 2
			parent.dirty = true
			parent.setLeft(node)
			parent.setRight(tree.NewNode(key, value))

			tree.workingBytes += parent.sizeBytes()
			tree.workingSize++
			return parent, false, nil
		default:
			tree.mutateNode(node)
			node.value = value
			node._hash()
			node.value = nil
			return node, true, nil
		}

	} else {
		tree.addOrphan(node)
		tree.mutateNode(node)

		var child *Node
		if bytes.Compare(key, node.key) < 0 {
			child, updated, err = tree.recursiveSet(node.left(tree), key, value)
			if err != nil {
				return nil, updated, err
			}
			node.setLeft(child)
		} else {
			child, updated, err = tree.recursiveSet(node.right(tree), key, value)
			if err != nil {
				return nil, updated, err
			}
			node.setRight(child)
		}

		if updated {
			return node, updated, nil
		}

		if string(key) == "b224d146eb" {
			fmt.Println("here")
		}
		if bytes.HasPrefix(key, node.sortKey) {
			if bytes.Compare(key, node.key) < 0 {
				node.sortKey = MinRightToken(node.key, key)
			} else {
				node.sortKey = MinLeftToken(node.key, key)
			}
		}

		err = node.calcHeightAndSize(tree)
		if err != nil {
			return nil, false, err
		}
		newNode, err := tree.balance(node)
		if err != nil {
			return nil, false, err
		}
		return newNode, updated, err
	}
}

// Remove removes a key from the working tree. The given key byte slice should not be modified
// after this call, since it may point to data stored inside IAVL.
func (tree *Tree) Remove(key []byte) ([]byte, bool, error) {
	if tree.root == nil {
		return nil, false, nil
	}
	newRoot, _, value, removed, err := tree.recursiveRemove(tree.root, key)
	if err != nil {
		return nil, false, err
	}
	if !removed {
		return nil, false, nil
	}

	tree.metrics.TreeDelete++

	tree.root = newRoot
	tree.emitDotGraph(tree.root)
	return value, true, nil
}

// removes the node corresponding to the passed key and balances the tree.
// It returns:
// - the hash of the new node (or nil if the node is the one removed)
// - the node that replaces the orig. node after remove
// - new leftmost leaf key for tree after successfully removing 'key' if changed.
// - the removed value
func (tree *Tree) recursiveRemove(node *Node, key []byte) (newSelf *Node, newKey []byte, newValue []byte, removed bool, err error) {
	if node.isLeaf() {
		if bytes.Equal(key, node.key) {
			tree.returnNode(node)
			return nil, nil, node.value, true, nil
		}
		return node, nil, nil, false, nil
	}

	if err != nil {
		return nil, nil, nil, false, err
	}

	// node.key < key; we go to the left to find the key:
	if bytes.Compare(key, node.key) < 0 {
		newLeftNode, newKey, value, removed, err := tree.recursiveRemove(node.left(tree), key)
		if err != nil {
			return nil, nil, nil, false, err
		}

		if !removed {
			return node, nil, value, removed, nil
		}

		// left node held value, was removed
		// collapse `node.rightNode` into `node`
		if newLeftNode == nil {
			right := node.right(tree)
			k := node.key
			tree.returnNode(node)
			return right, k, value, removed, nil
		}

		tree.addOrphan(node)
		tree.mutateNode(node)

		node.setLeft(newLeftNode)
		err = node.calcHeightAndSize(tree)
		if err != nil {
			return nil, nil, nil, false, err
		}
		node, err = tree.balance(node)
		if err != nil {
			return nil, nil, nil, false, err
		}

		return node, newKey, value, removed, nil
	}
	// node.key >= key; either found or look to the right:
	newRightNode, newKey, value, removed, err := tree.recursiveRemove(node.right(tree), key)
	if err != nil {
		return nil, nil, nil, false, err
	}

	if !removed {
		return node, nil, value, removed, nil
	}

	// right node held value, was removed
	// collapse `node.leftNode` into `node`
	if newRightNode == nil {
		left := node.left(tree)
		tree.returnNode(node)
		return left, nil, value, removed, nil
	}

	tree.addOrphan(node)
	tree.mutateNode(node)

	node.setRight(newRightNode)
	if newKey != nil {
		node.sortKey = MinRightToken(node.key, newKey)
		node.key = newKey
	}
	err = node.calcHeightAndSize(tree)
	if err != nil {
		return nil, nil, nil, false, err
	}

	node, err = tree.balance(node)
	if err != nil {
		return nil, nil, nil, false, err
	}

	return node, nil, value, removed, nil
}

func (tree *Tree) Size() int64 {
	return tree.root.size
}

func (tree *Tree) Height() int8 {
	return tree.root.subtreeHeight
}

func (tree *Tree) nextNodeKey() NodeKey {
	tree.sequence++
	nk := NewNodeKey(tree.version+1, tree.sequence)
	return nk
}

func (tree *Tree) mutateNode(node *Node) {
	// node has already been mutated in working set
	if node.hash == nil {
		return
	}
	node.hash = nil
	node.version = tree.version + 1

	if node.dirty {
		return
	}

	node.dirty = true
	tree.workingBytes += node.sizeBytes()
	tree.workingSize++
}

func (tree *Tree) addOrphan(node *Node) {
	if node.hash == nil {
		return
	}
	tree.orphans = append(tree.orphans, &nodeDiff{
		new:         node,
		prevHeight:  node.subtreeHeight,
		prevSortKey: node.key,
	})
}

// NewNode returns a new node from a key, value and version.
func (tree *Tree) NewNode(key []byte, value []byte) *Node {
	node := tree.pool.Get()

	node.version = tree.version + 1
	tree.leafSequence++
	node.leafSeq = tree.leafSequence

	node.key = key
	node.sortKey = key
	node.value = value
	node.subtreeHeight = 0
	node.size = 1

	node._hash()
	node.value = nil
	node.dirty = true
	tree.workingBytes += node.sizeBytes()
	tree.workingSize++
	return node
}

func (tree *Tree) returnNode(node *Node) {
	if node.dirty {
		tree.workingBytes -= node.sizeBytes()
		tree.workingSize--
	}
	tree.orphans = append(tree.orphans, &nodeDiff{
		delete:      true,
		prevSortKey: node.sortKey,
		prevHeight:  node.subtreeHeight,
	})
	tree.pool.Put(node)
}

func (tree *Tree) emitDotGraph(root *Node) *dot.Graph {
	if !tree.emitDotGraphs {
		return nil
	}
	tree.lastDotGraph = writeDotGraph(root, tree.lastDotGraph)
	tree.dotGraphs = append(tree.dotGraphs, tree.lastDotGraph)
	return tree.lastDotGraph
}

func (tree *Tree) emitDotGraphWithLabel(root *Node, label string) {
	g := tree.emitDotGraph(root)
	if g == nil {
		return
	}
	g.Label(label)
}
