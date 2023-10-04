package iavl

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
	"github.com/cosmos/iavl/v2/testutil"
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

func TestTreeSanity(t *testing.T) {
	cases := []struct {
		name   string
		treeFn func() *Tree
		hashFn func(*Tree) []byte
	}{
		{
			name: "sqlite",
			treeFn: func() *Tree {
				dir := "/tmp/tree_sanity"
				require.NoError(t, os.RemoveAll(dir))
				require.NoError(t, os.MkdirAll(dir, 0755))
				pool := NewNodePool()

				sql := &SqliteDb{
					shards:       make(map[int64]*sqlite3.Stmt),
					versionShard: make(map[int64]int64),
					connString:   "file::memory:?cache=shared",
					pool:         pool,
				}
				require.NoError(t, sql.init(true))

				//sql, err := NewSqliteDb(pool, dir, true)
				//require.NoError(t, err)

				require.NoError(t, sql.resetReadConn())
				return NewTree(sql, pool)
			},
			hashFn: func(tree *Tree) []byte {
				hash, _, err := tree.SaveVersionV2()
				require.NoError(t, err)
				return hash
			},
		},
		{
			name: "no_db",
			treeFn: func() *Tree {
				pool := NewNodePool()
				return NewTree(nil, pool)
			},
			hashFn: func(tree *Tree) []byte {
				rehashTree(tree.root)
				tree.version++
				return tree.root.hash
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tree := tc.treeFn()
			opts := testutil.NewTreeBuildOptions()
			itr := opts.Iterator
			var err error
			for ; itr.Valid(); err = itr.Next() {
				if itr.Version() > 150 {
					break
				}
				require.NoError(t, err)
				nodes := itr.Nodes()
				for ; nodes.Valid(); err = nodes.Next() {
					require.NoError(t, err)
					node := nodes.GetNode()
					if node.Delete {
						_, _, err := tree.Remove(node.Key)
						require.NoError(t, err)
					} else {
						_, err := tree.Set(node.Key, node.Value)
						require.NoError(t, err)
					}
				}
				switch itr.Version() {
				case 1:
					h := tc.hashFn(tree)
					require.Equal(t, "48c3113b8ba523d3d539d8aea6fce28814e5688340ba7334935c1248b6c11c7a", hex.EncodeToString(h))
					require.Equal(t, int64(104938), tree.root.size)
					fmt.Printf("version=%d, hash=%x size=%d\n", itr.Version(), h, tree.root.size)
				case 150:
					h := tc.hashFn(tree)
					require.Equal(t, "04c42dd1cec683cbbd4974027e4b003b848e389a33d03d7a9105183e6d108dd9", hex.EncodeToString(h))
					require.Equal(t, int64(105030), tree.root.size)
					fmt.Printf("version=%d, hash=%x size=%d\n", itr.Version(), h, tree.root.size)
				}
			}
		})
	}
}
