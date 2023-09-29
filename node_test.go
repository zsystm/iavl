package iavl

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"testing"

	"github.com/cosmos/iavl-bench/bench"
	"github.com/emicklei/dot"
	api "github.com/kocubinski/costor-api"
	"github.com/stretchr/testify/require"
)

func TestMinRightToken(t *testing.T) {
	cases := []struct {
		name string
		a    string
		b    string
		want string
	}{
		{name: "naive", a: "alphabet", b: "elephant", want: "e"},
		{name: "trivial substring", a: "bird", b: "bingo", want: "bir"},
		{name: "longer", a: "bird", b: "birdy", want: "birdy"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got1 := MinRightToken([]byte(tc.a), []byte(tc.b))
			got2 := MinRightToken([]byte(tc.b), []byte(tc.a))
			if string(got1) != tc.want {
				t.Errorf("MinRightToken(%q, %q) = %q, want %q", tc.a, tc.b, got1, tc.want)
			}
			require.Equal(t, got1, got2)
		})
	}
}

func TestMinRightToken_Gen(t *testing.T) {
	seed := rand.Int()
	t.Logf("seed: %d", seed)
	r := rand.New(rand.NewSource(int64(seed)))
	for i := 0; i < 1_000_000; i++ {
		lenA := r.Intn(20)
		lenB := r.Intn(20)
		a := make([]byte, lenA)
		b := make([]byte, lenB)
		r.Read(a)
		r.Read(b)
		res := MinRightToken(a, b)
		resSwap := MinRightToken(b, a)
		require.Equal(t, res, resSwap)

		compare := bytes.Compare(a, b)
		switch {
		case compare < 0:
			require.Less(t, a, res)
			require.GreaterOrEqual(t, b, res)
		case compare > 0:
			require.Less(t, b, res)
			require.GreaterOrEqual(t, a, res)
		default:
			require.Equal(t, a, res)
			require.Equal(t, b, res)
		}
	}
}

func writeDotGraph(w io.Writer, root *Node) {
	graph := dot.NewGraph(dot.Directed)

	var traverse func(node *Node) dot.Node
	var i int
	traverse = func(node *Node) dot.Node {
		if node == nil {
			return dot.Node{}
		}
		i++
		n := graph.Node(fmt.Sprintf("%s - %s - %d", string(node.key), string(node.sortKey), node.subtreeHeight))
		if node.isLeaf() {
			return n
		}
		leftNode := traverse(node.leftNode)
		rightNode := traverse(node.rightNode)

		n.Edge(leftNode, "l")
		n.Edge(rightNode, "r")

		return n
	}

	traverse(root)
	fmt.Println("i", i)
	_, err := w.Write([]byte(graph.String()))
	if err != nil {
		panic(err)
	}
}

func TestTokenizedTree(t *testing.T) {
	// can a total order of (sortKey, height) be built to satisfy a traversal order of the tree?
	// in-order seems the most possible.

	outDir := "/tmp/tree_viz"

	var inOrder func(node *Node) []*Node
	inOrder = func(node *Node) (nodes []*Node) {
		if node == nil {
			return nil
		}
		nodes = append(nodes, inOrder(node.leftNode)...)
		nodes = append(nodes, node)
		nodes = append(nodes, inOrder(node.rightNode)...)
		return nodes
	}

	var preOrder func(node *Node) []*Node
	preOrder = func(node *Node) (nodes []*Node) {
		if node == nil {
			return nil
		}

		nodes = append(nodes, node)
		nodes = append(nodes, preOrder(node.leftNode)...)
		nodes = append(nodes, preOrder(node.rightNode)...)
		return nodes
	}

	nodesByKey := make(map[string]*api.Node)

	gen := bench.ChangesetGenerator{
		Seed:             77,
		KeyMean:          4,
		KeyStdDev:        1,
		ValueMean:        10,
		ValueStdDev:      1,
		InitialSize:      10,
		FinalSize:        50,
		Versions:         5,
		ChangePerVersion: 5,
		DeleteFraction:   0.2,
	}
	itr, err := gen.Iterator()
	require.NoError(t, err)
	tree := NewTree(nil, NewNodePool())
	i := 0
	for ; itr.Valid(); err = itr.Next() {
		if itr.Version() > 1 {
			break
		}
		require.NoError(t, err)
		nodes := itr.Nodes()
		for ; nodes.Valid(); err = nodes.Next() {
			require.NoError(t, err)
			node := nodes.GetNode()
			strKey := hex.EncodeToString(node.Key)
			nodesByKey[strKey] = node
			bzKey := []byte(strKey)

			if node.Delete {
				_, _, err := tree.Remove(bzKey)
				require.NoError(t, err)
			} else {
				_, err := tree.Set(bzKey, node.Value)
				require.NoError(t, err)
			}
			i++
			f, err := os.Create(fmt.Sprintf("%s/step_%d.dot", outDir, i))
			require.NoError(t, err)
			writeDotGraph(f, tree.root)
		}
	}

	orderedNodes := preOrder(tree.root)
	sort.Slice(orderedNodes, func(i, j int) bool {
		res := bytes.Compare(orderedNodes[i].sortKey, orderedNodes[j].sortKey)
		// order by (key ASC, height DESC)
		if res != 0 {
			return res < 0
		} else {
			// height DESC
			return orderedNodes[i].subtreeHeight > orderedNodes[j].subtreeHeight
		}
	})

	inOrderNodes := inOrder(tree.root)
	var lastNode *Node
	for i, node := range inOrderNodes {
		fmt.Printf("node: %s, %s, %d\n", string(node.key), string(node.sortKey), node.subtreeHeight)
		if lastNode != nil {
			require.LessOrEqual(t, lastNode.key, node.key)
			if bytes.Equal(lastNode.key, node.key) {
				// in-order assertion
				require.Greater(t, lastNode.subtreeHeight, node.subtreeHeight)
			}
			require.Equal(t, node.key, orderedNodes[i].key)
			require.Equalf(t, orderedNodes[i].subtreeHeight, node.subtreeHeight,
				"heights don't match, node.key: %s", node.key)
			require.Equal(t, node, orderedNodes[i])
		}
		lastNode = node
	}

	require.NoError(t, os.RemoveAll(outDir))
	require.NoError(t, os.Mkdir(outDir, 0755))
	f, err := os.Create(fmt.Sprintf("%s/tree.dot", outDir))
	require.NoError(t, err)
	writeDotGraph(f, tree.root)
	require.NoError(t, f.Close())
}
