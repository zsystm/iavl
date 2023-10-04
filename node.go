package iavl

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"sync"
	"unsafe"

	encoding "github.com/cosmos/iavl/v2/internal"
)

// NodeKey represents a key of node in the DB.
type NodeKey [12]byte

func (nk NodeKey) Version() int64 {
	return int64(binary.BigEndian.Uint64(nk[:]))
}

func (nk NodeKey) Sequence() uint32 {
	return binary.BigEndian.Uint32(nk[8:])
}

func NewNodeKey(version int64, sequence uint32) NodeKey {
	var nk NodeKey
	binary.BigEndian.PutUint64(nk[:], uint64(version))
	binary.BigEndian.PutUint32(nk[8:], sequence)
	return nk
}

// String returns a string representation of the node key.
func (nk NodeKey) String() string {
	return fmt.Sprintf("(%d, %d)", nk.Version(), nk.Sequence())
}

var emptyNodeKey = NodeKey{}

func (nk NodeKey) IsEmpty() bool {
	return nk == emptyNodeKey
}

type branchKey struct {
	sortKey []byte
	sHeight int8
}

func (bk *branchKey) Equals(other *Node) bool {
	return bytes.Equal(bk.sortKey, other.sortKey)
}

type leafSeq uint64

type nodeDiff struct {
	new         *Node
	prevHeight  int8
	prevSortKey []byte
	delete      bool
}

// Node represents a node in a Tree.
type Node struct {
	key           []byte
	sortKey       []byte
	value         []byte
	hash          []byte
	version       int64
	size          int64
	leftNode      *Node
	rightNode     *Node
	subtreeHeight int8

	dirty  bool
	poolId uint64

	leftLeaf  leafSeq
	rightLeaf leafSeq
	leafSeq   leafSeq

	leftBranch  *branchKey
	rightBranch *branchKey

	// if not nil then the node was mutated in place
	// the sortKey and sHeight may have changed, but not necessarily
	// invariant: when not nil hash must also be nil
	// when operating in fast mode these changes are thrown away.
	// when operating in full persist at every SaveVersion, they are written to disk.
	lastBranchKey *branchKey
}

func (node *Node) isLeaf() bool {
	return node.subtreeHeight == 0
}

func (node *Node) setLeft(leftNode *Node) {
	node.leftNode = leftNode
	if leftNode.isLeaf() {
		node.leftLeaf = leftNode.leafSeq
		node.leftBranch = nil
	} else {
		node.leftBranch = &branchKey{
			sortKey: leftNode.sortKey,
			sHeight: leftNode.subtreeHeight,
		}
		node.leftLeaf = 0
	}
}

func (node *Node) setRight(rightNode *Node) {
	node.rightNode = rightNode
	if rightNode.isLeaf() {
		node.rightLeaf = rightNode.leafSeq
		node.rightBranch = nil
	} else {
		node.rightBranch = &branchKey{
			sortKey: rightNode.sortKey,
			sHeight: rightNode.subtreeHeight,
		}
		node.rightLeaf = 0
	}
}

func (node *Node) left(t *Tree) *Node {
	leftNode, err := node.getLeftNode(t)
	if err != nil {
		panic(err)
	}
	return leftNode
}

func (node *Node) right(t *Tree) *Node {
	rightNode, err := node.getRightNode(t)
	if err != nil {
		panic(err)
	}
	return rightNode
}

// getLeftNode will never be called on leaf nodes. all tree nodes have 2 children.
func (node *Node) getLeftNode(t *Tree) (*Node, error) {
	if node.isLeaf() {
		return nil, fmt.Errorf("leaf node has no left node")
	}
	if node.leftNode != nil {
		return node.leftNode, nil
	}
	//node.leftNode = t.cache.Get(node.leftNodeKey)
	if node.leftNode == nil {
		var err error
		node.leftNode, err = t.sql.getLeftNode(node)

		if err != nil {
			return nil, err
		}
	}
	return node.leftNode, nil
}

func (node *Node) getRightNode(t *Tree) (*Node, error) {
	if node.isLeaf() {
		return nil, fmt.Errorf("leaf node has no right node")
	}
	if node.rightNode != nil {
		return node.rightNode, nil
	}
	//node.rightNode = t.cache.Get(node.rightNodeKey)
	if node.rightNode == nil {
		var err error
		node.rightNode, err = t.sql.getRightNode(node)

		if err != nil {
			return nil, err
		}
	}
	return node.rightNode, nil
}

// NOTE: mutates height and size
func (node *Node) calcHeightAndSize(t *Tree) error {
	leftNode, err := node.getLeftNode(t)
	if err != nil {
		return err
	}

	rightNode, err := node.getRightNode(t)
	if err != nil {
		return err
	}

	node.subtreeHeight = maxInt8(leftNode.subtreeHeight, rightNode.subtreeHeight) + 1
	node.size = leftNode.size + rightNode.size
	return nil
}

func maxInt8(a, b int8) int8 {
	if a > b {
		return a
	}
	return b
}

// NOTE: assumes that node can be modified
// TODO: optimize balance & rotate
func (tree *Tree) balance(node *Node) (newSelf *Node, err error) {
	if node.hash != nil {
		return nil, fmt.Errorf("unexpected balance() call on persisted node")
	}
	balance, err := node.calcBalance(tree)
	if err != nil {
		return nil, err
	}

	if balance > 1 {
		tree.emitDotGraphWithLabel(tree.root, fmt.Sprintf("left too heavy, re-balancing %s-%d",
			node.key, node.subtreeHeight))

		lftBalance, err := node.leftNode.calcBalance(tree)
		if err != nil {
			return nil, err
		}

		if lftBalance >= 0 {
			// Left Left Case
			newNode, err := tree.rotateRight(node)
			if err != nil {
				return nil, err
			}
			tree.emitDotGraphWithLabel(newNode, "left left case; subtree right rotated")
			return newNode, nil
		}
		// Left Right Case
		newLeftNode, err := tree.rotateLeft(node.leftNode)
		if err != nil {
			return nil, err
		}
		node.setLeft(newLeftNode)
		tree.emitDotGraphWithLabel(node, "left right case; left subtree left rotated")

		newNode, err := tree.rotateRight(node)
		if err != nil {
			return nil, err
		}
		tree.emitDotGraphWithLabel(newNode, "left right case; subtree right rotated")

		return newNode, nil
	}
	if balance < -1 {
		tree.emitDotGraphWithLabel(tree.root, fmt.Sprintf("right too heavy, re-balancing %s-%d",
			node.key, node.subtreeHeight))

		rightNode, err := node.getRightNode(tree)
		if err != nil {
			return nil, err
		}

		rightBalance, err := rightNode.calcBalance(tree)
		if err != nil {
			return nil, err
		}
		if rightBalance <= 0 {
			// Right Right Case
			newNode, err := tree.rotateLeft(node)
			if err != nil {
				return nil, err
			}
			tree.emitDotGraphWithLabel(newNode, "right right case; subtree left rotated")
			return newNode, nil
		}

		newRightNode, err := tree.rotateRight(rightNode)
		if err != nil {
			return nil, err
		}
		node.setRight(newRightNode)
		tree.emitDotGraphWithLabel(node, "right left case; right subtree right rotated")

		newNode, err := tree.rotateLeft(node)
		if err != nil {
			return nil, err
		}
		tree.emitDotGraphWithLabel(newNode, "right left case; subtree left rotated")
		return newNode, nil
	}
	// Nothing changed
	return node, nil
}

func (node *Node) calcBalance(t *Tree) (int, error) {
	leftNode, err := node.getLeftNode(t)
	if err != nil {
		return 0, err
	}

	rightNode, err := node.getRightNode(t)
	if err != nil {
		return 0, err
	}

	return int(leftNode.subtreeHeight) - int(rightNode.subtreeHeight), nil
}

// Rotate right and return the new node and orphan.
func (tree *Tree) rotateRight(node *Node) (*Node, error) {
	//log.Debug().Msgf("rotateRight: %s", node.key)

	var err error
	tree.addOrphan(node)
	tree.mutateNode(node)

	tree.addOrphan(node.left(tree))
	newNode := node.left(tree)
	tree.mutateNode(newNode)

	node.setLeft(newNode.right(tree))
	newNode.setRight(node)

	err = node.calcHeightAndSize(tree)
	if err != nil {
		return nil, err
	}

	err = newNode.calcHeightAndSize(tree)
	if err != nil {
		return nil, err
	}

	return newNode, nil
}

// Rotate left and return the new node and orphan.
func (tree *Tree) rotateLeft(node *Node) (*Node, error) {
	//log.Debug().Msgf("rotateLeft: %s", node.key)

	var err error
	tree.addOrphan(node)
	tree.mutateNode(node)

	tree.addOrphan(node.right(tree))
	newNode := node.right(tree)
	tree.mutateNode(newNode)

	node.setRight(newNode.left(tree))
	newNode.setLeft(node)

	err = node.calcHeightAndSize(tree)
	if err != nil {
		return nil, err
	}

	err = newNode.calcHeightAndSize(tree)
	if err != nil {
		return nil, err
	}

	return newNode, nil
}

var hashPool = &sync.Pool{
	New: func() any {
		return sha256.New()
	},
}

// Computes the hash of the node without computing its descendants. Must be
// called on nodes which have descendant node hashes already computed.
func (node *Node) _hash() []byte {
	if node.hash != nil {
		return node.hash
	}

	h := hashPool.Get().(hash.Hash)
	if err := node.writeHashBytes(h); err != nil {
		return nil
	}
	node.hash = h.Sum(nil)
	h.Reset()
	hashPool.Put(h)

	return node.hash
}

func (node *Node) writeHashBytes(w io.Writer) error {
	var (
		n   int
		buf [binary.MaxVarintLen64]byte
	)

	n = binary.PutVarint(buf[:], int64(node.subtreeHeight))
	if _, err := w.Write(buf[0:n]); err != nil {
		return fmt.Errorf("writing height, %w", err)
	}
	n = binary.PutVarint(buf[:], node.size)
	if _, err := w.Write(buf[0:n]); err != nil {
		return fmt.Errorf("writing size, %w", err)
	}
	n = binary.PutVarint(buf[:], node.version)
	if _, err := w.Write(buf[0:n]); err != nil {
		return fmt.Errorf("writing version, %w", err)
	}

	if node.isLeaf() {
		if err := EncodeBytes(w, node.key); err != nil {
			return fmt.Errorf("writing key, %w", err)
		}

		// Indirection needed to provide proofs without values.
		// (e.g. ProofLeafNode.ValueHash)
		valueHash := sha256.Sum256(node.value)

		if err := EncodeBytes(w, valueHash[:]); err != nil {
			return fmt.Errorf("writing value, %w", err)
		}
	} else {
		if err := EncodeBytes(w, node.leftNode.hash); err != nil {
			return fmt.Errorf("writing left hash, %w", err)
		}
		if err := EncodeBytes(w, node.rightNode.hash); err != nil {
			return fmt.Errorf("writing right hash, %w", err)
		}
	}

	return nil
}

func EncodeBytes(w io.Writer, bz []byte) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(len(bz)))
	if _, err := w.Write(buf[0:n]); err != nil {
		return err
	}
	_, err := w.Write(bz)
	return err
}

// MakeNode constructs a *Node from an encoded byte slice.
func MakeNode(pool *NodePool, nodeKey NodeKey, buf []byte) (*Node, error) {
	panic("not implemented")

	// Read node header (height, size, version, key).
	height, n, err := encoding.DecodeVarint(buf)
	if err != nil {
		return nil, fmt.Errorf("decoding node.height, %w", err)
	}
	buf = buf[n:]
	if height < int64(math.MinInt8) || height > int64(math.MaxInt8) {
		return nil, errors.New("invalid height, must be int8")
	}

	size, n, err := encoding.DecodeVarint(buf)
	if err != nil {
		return nil, fmt.Errorf("decoding node.size, %w", err)
	}
	buf = buf[n:]

	key, n, err := encoding.DecodeBytes(buf)
	if err != nil {
		return nil, fmt.Errorf("decoding node.key, %w", err)
	}
	buf = buf[n:]

	hash, n, err := encoding.DecodeBytes(buf)
	if err != nil {
		return nil, fmt.Errorf("decoding node.hash, %w", err)
	}
	buf = buf[n:]

	node := pool.Get()
	node.subtreeHeight = int8(height)
	//node.nodeKey = nodeKey
	node.size = size
	node.key = key
	node.hash = hash

	if !node.isLeaf() {
		leftNodeKey, n, err := encoding.DecodeBytes(buf)
		if err != nil {
			return nil, fmt.Errorf("decoding node.leftKey, %w", err)
		}
		buf = buf[n:]

		rightNodeKey, _, err := encoding.DecodeBytes(buf)
		if err != nil {
			return nil, fmt.Errorf("decoding node.rightKey, %w", err)
		}

		var leftNk, rightNk NodeKey
		copy(leftNk[:], leftNodeKey)
		copy(rightNk[:], rightNodeKey)
		//node.leftNodeKey = leftNk
		//node.rightNodeKey = rightNk
	}
	return node, nil
}

func (node *Node) WriteBytes(w io.Writer) error {
	panic("not implemented")
	if node == nil {
		return errors.New("cannot write nil node")
	}
	cause := encoding.EncodeVarint(w, int64(node.subtreeHeight))
	if cause != nil {
		return fmt.Errorf("writing height; %w", cause)
	}
	cause = encoding.EncodeVarint(w, node.size)
	if cause != nil {
		return fmt.Errorf("writing size; %w", cause)
	}

	cause = encoding.EncodeBytes(w, node.key)
	if cause != nil {
		return fmt.Errorf("writing key; %w", cause)
	}

	if len(node.hash) != 32 {
		return fmt.Errorf("hash has unexpected length: %d", len(node.hash))
	}
	cause = encoding.EncodeBytes(w, node.hash)
	if cause != nil {
		return fmt.Errorf("writing hash; %w", cause)
	}

	//if !node.isLeaf() {
	//	if node.leftNodeKey.IsEmpty() {
	//		return fmt.Errorf("left node key is nil")
	//	}
	//	cause = encoding.EncodeBytes(w, node.leftNodeKey[:])
	//	if cause != nil {
	//		return fmt.Errorf("writing left node key; %w", cause)
	//	}
	//
	//	if node.rightNodeKey.IsEmpty() {
	//		return fmt.Errorf("right node key is nil")
	//	}
	//	cause = encoding.EncodeBytes(w, node.rightNodeKey[:])
	//	if cause != nil {
	//		return fmt.Errorf("writing right node key; %w", cause)
	//	}
	//}
	return nil
}

func (node *Node) Bytes() ([]byte, error) {
	buf := &bytes.Buffer{}
	err := node.WriteBytes(buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

var nodeSize = uint64(unsafe.Sizeof(Node{})) + 32

func (node *Node) varSize() uint64 {
	return uint64(len(node.key))
}

func (node *Node) sizeBytes() uint64 {
	return nodeSize + node.varSize()
}

func MinRightToken(a []byte, b []byte) []byte {
	maxLength := len(a)
	if len(b) > maxLength {
		maxLength = len(b)
	}
	token := make([]byte, 0, maxLength)

	for i := 0; i < maxLength; i++ {
		if i == len(a) {
			token = append(token, b[i])
			return token
		}
		if i == len(b) {
			token = append(token, a[i])
			return token
		}

		if a[i] != b[i] {
			if b[i] > a[i] {
				token = append(token, b[i])
				return token
			}
			token = append(token, a[i])
			return token
		}
		token = append(token, a[i])
	}
	return token
}

func MinLeftToken(a []byte, b []byte) []byte {
	maxLength := len(a)
	if len(b) > maxLength {
		maxLength = len(b)
	}
	token := make([]byte, 0, maxLength)

	for i := 0; i < maxLength; i++ {
		if i == len(a) {
			return token
		}
		if i == len(b) {
			return token
		}

		if a[i] != b[i] {
			if b[i] < a[i] {
				token = append(token, b[i])
				return token
			}
			token = append(token, a[i])
			return token
		}
		token = append(token, a[i])
	}
	return token
}
