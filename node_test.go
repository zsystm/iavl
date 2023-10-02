package iavl

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"testing"

	"github.com/cosmos/iavl-bench/bench"
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

func TestTokenizedTree(t *testing.T) {
	// can a total order of (sortKey, height) be built to satisfy a traversal order of the tree?
	// in-order seems the most possible.

	outDir := "/tmp/tree_viz"
	require.NoError(t, os.RemoveAll(outDir))
	require.NoError(t, os.Mkdir(outDir, 0755))

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
		InitialSize:      100,
		FinalSize:        150,
		Versions:         5,
		ChangePerVersion: 5,
		DeleteFraction:   0.2,
	}
	itr, err := gen.Iterator()
	require.NoError(t, err)
	tree := NewTree(nil, NewNodePool())
	tree.emitDotGraphs = true

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
			for j, dg := range tree.dotGraphs {
				f, err := os.Create(fmt.Sprintf("%s/step_%02d_%d.dot", outDir, i, j))
				require.NoError(t, err)
				_, err = f.Write([]byte(dg.String()))
				require.NoError(t, err)
				require.NoError(t, f.Close())
			}
			tree.dotGraphs = nil

			// verify the tree at every step

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
			for i, n := range inOrderNodes {
				//fmt.Printf("node: %s, %s, %d\n", string(n.key), string(n.sortKey), n.subtreeHeight)
				if lastNode != nil {
					require.LessOrEqual(t, lastNode.key, n.key)
					if bytes.Equal(lastNode.key, n.key) {
						// in-order assertion
						require.Greater(t, lastNode.subtreeHeight, n.subtreeHeight)
					}
					require.Equal(t, n.key, orderedNodes[i].key)
					require.Equalf(t, n.subtreeHeight, orderedNodes[i].subtreeHeight,
						"heights don't match, node.key: %s", n.key)
					require.Equal(t, n, orderedNodes[i])
				}
				lastNode = n
			}
		}
	}
}
