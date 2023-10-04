// TODO move to package iavl_test
// this means an audit of exported fields and types.
package iavl

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/cosmos/iavl/v2/metrics"
	"github.com/cosmos/iavl/v2/testutil"
	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

func MemUsage() string {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	s := fmt.Sprintf("alloc=%s gc=%d",
		humanize.Bytes(m.Alloc),
		//humanize.Bytes(m.TotalAlloc),
		//humanize.Bytes(m.Sys),
		m.NumGC)
	return s
}

func testTreeBuild(t *testing.T, tree *Tree, opts testutil.TreeBuildOptions) (cnt int64) {
	var (
		hash    []byte
		version int64
		err     error
	)
	cnt = 1

	// generator
	itr := opts.Iterator
	fmt.Printf("Initial memory usage from generators:\n%s\n", MemUsage())

	if opts.LoadVersion != 0 {
		require.NoError(t, tree.LoadVersion(opts.LoadVersion))
		log.Info().Msgf("fast forwarding changesets to version %d...", opts.LoadVersion+1)
		i := 1
		for ; itr.Valid(); err = itr.Next() {
			if itr.Version() > opts.LoadVersion {
				break
			}
			require.NoError(t, err)
			nodes := itr.Nodes()
			for ; nodes.Valid(); err = nodes.Next() {
				require.NoError(t, err)
				if i%5_000_000 == 0 {
					fmt.Printf("fast forward %s nodes\n", humanize.Comma(int64(i)))
				}
				i++
			}
		}
		log.Info().Msgf("fast forward complete")
	}

	sampleRate := int64(100_000)
	if opts.SampleRate != 0 {
		sampleRate = opts.SampleRate
	}

	since := time.Now()
	itrStart := time.Now()
	for ; itr.Valid(); err = itr.Next() {
		require.NoError(t, err)
		changeset := itr.Nodes()
		for ; changeset.Valid(); err = changeset.Next() {
			cnt++
			require.NoError(t, err)
			node := changeset.GetNode()

			//var keyBz bytes.Buffer
			//keyBz.Write([]byte(node.StoreKey))
			//keyBz.Write(node.Key)
			//key := keyBz.Bytes()
			key := node.Key

			if !node.Delete {
				//fmt.Printf("set key=%x\n", key)
				_, err = tree.Set(key, node.Value)
				require.NoError(t, err)
			} else {
				_, _, err := tree.Remove(key)
				require.NoError(t, err)
			}

			if cnt%sampleRate == 0 {
				dur := time.Since(since)

				fmt.Printf("leaves=%s time=%s last=%s μ=%s version=%d orphans=%s key_reads=%s %s\n",
					humanize.Comma(cnt),
					dur.Round(time.Millisecond),
					humanize.Comma(int64(float64(sampleRate)/time.Since(since).Seconds())),
					humanize.Comma(int64(float64(cnt)/time.Since(itrStart).Seconds())),
					version,
					humanize.Comma(tree.metrics.TreeOrphans),
					humanize.Comma(tree.metrics.TreeKeyReads),
					MemUsage())

				if tree.metrics.WriteTime > 0 && tree.metrics.WriteLeaves > 0 {
					fmt.Printf("writes: cnt=%s wr/s=%s dur/wr=%s dur=%s\n",
						humanize.Comma(tree.metrics.WriteLeaves),
						humanize.Comma(int64(float64(tree.metrics.WriteLeaves)/tree.metrics.WriteTime.Seconds())),
						time.Duration(int64(tree.metrics.WriteTime)/tree.metrics.WriteLeaves),
						tree.metrics.WriteTime.Round(time.Millisecond),
					)
				}

				if err := tree.sql.queryReport(0); err != nil {
					t.Fatalf("query report err %v", err)
				}

				fmt.Println()

				since = time.Now()

				tree.metrics.WriteDurations = nil
				tree.metrics.WriteLeaves = 0
				tree.metrics.WriteTime = 0
				tree.metrics.TreeOrphans = 0
				tree.metrics.TreeKeyReads = 0
			}

			//if cnt%(sampleRate*4) == 0 {
			//}
		}
		hash, version, err = tree.SaveVersionV2()

		//fmt.Printf("version: %d, hash: %x, size: %d\n", version, hash, tree.root.size)

		require.NoError(t, err)
		if version == opts.Until {
			break
		}
	}
	fmt.Printf("final version: %d, hash: %x\n", version, hash)
	fmt.Printf("height: %d, size: %d\n", tree.Height(), tree.Size())
	fmt.Printf("mean leaves/ms %s\n", humanize.Comma(cnt/time.Since(itrStart).Milliseconds()))
	if opts.Report != nil {
		opts.Report()
	}
	require.Equal(t, version, opts.Until)
	return cnt
}

func TestTree_Build(t *testing.T) {
	var err error
	tmpDir := t.TempDir()
	//tmpDir := "/tmp/leaves"
	t.Logf("levelDb tmpDir: %s\n", tmpDir)

	pool := NewNodePool()
	sql, err := NewSqliteDb(pool, tmpDir, true)
	//sql, err := NewInMemorySqliteDb(pool)
	tree := NewTree(sql, pool)
	require.NoError(t, err)

	opts := testutil.NewTreeBuildOptions()
	opts.Report = func() {
		tree.metrics.Report()
	}

	require.NoError(t, tree.sql.resetReadConn())

	testStart := time.Now()
	leaves := testTreeBuild(t, tree, opts)

	treeDuration := time.Since(testStart)
	workingSetCount := 0 // offset the dirty root above.

	fmt.Printf("mean leaves/s: %s\n", humanize.Comma(int64(float64(leaves)/treeDuration.Seconds())))
	fmt.Printf("workingSetCount: %d\n", workingSetCount)

	ts := &treeStat{}

	fmt.Printf("tree size: %s\n", humanize.Bytes(ts.size))

	require.Equal(t, opts.UntilHash, fmt.Sprintf("%x", tree.root.hash))
}

func treeCount(tree *Tree, node Node) int {
	if node.isLeaf() {
		return 1
	}
	left := node.left(tree)
	right := node.right(tree)
	return 1 + treeCount(tree, *left) + treeCount(tree, *right)
}

func treeHeight(tree *Tree, node Node) int8 {
	if node.isLeaf() {
		return 1
	}
	left := node.left(tree)
	right := node.right(tree)
	return 1 + maxInt8(treeHeight(tree, *left), treeHeight(tree, *right))
}

type treeStat struct {
	size uint64
}

var osmoScalePath = fmt.Sprintf("%s/src/scratch/sqlite-osmo", os.Getenv("HOME"))

func TestBuild_OsmoScale(t *testing.T) {
	tmpDir := osmoScalePath

	pool := NewNodePool()
	sql, err := NewSqliteDb(pool, tmpDir, true)
	require.NoError(t, err)

	tree := &Tree{
		pool:           pool,
		metrics:        &metrics.TreeMetrics{},
		sql:            sql,
		maxWorkingSize: 1024 * 1024 * 1024,
	}

	opts := testutil.OsmoLike()
	opts.Report = func() {
		tree.metrics.Report()
	}
	version1 := opts.Iterator.Nodes()
	i := 0
	start := time.Now()
	for ; version1.Valid(); err = version1.Next() {
		require.NoError(t, err)
		node := version1.GetNode()
		var keyBz bytes.Buffer
		keyBz.Write([]byte(node.StoreKey))
		keyBz.Write(node.Key)
		key := keyBz.Bytes()
		require.NoError(t, err)

		if node.Delete {
			t.Fatalf("unexpected delete in version 1")
		}

		_, err = tree.Set(key, node.Value)
		require.NoError(t, err)

		i++
		if i%500_000 == 0 {
			log.Info().Msgf("leaves=%s dur=%s; rate=%s",
				humanize.Comma(int64(i)),
				time.Since(start),
				humanize.Comma(int64(500_000/time.Since(start).Seconds())))
			start = time.Now()
		}
	}

	hash, _, err := tree.SaveVersion()
	require.NoError(t, err)
	fmt.Printf("version 1 hash: %x\n", hash)

	err = tree.LoadVersion(1)
	require.NoError(t, err)
	require.NoError(t, sql.Close())

	require.Equal(t, "bc4bc22437cc71b4ff8e6735ca27757b1bd6a6285c872bbf8d77007e864b5877",
		fmt.Sprintf("%x", hash))
}

func TestOsmoLike_HotStart(t *testing.T) {
	tmpDir := "/tmp/iavl-init"

	pool := NewNodePool()
	sql, err := NewSqliteDb(pool, tmpDir, false)
	require.NoError(t, err)
	tree := NewTree(sql, pool)

	opts := testutil.CompactedChangelogs("/Users/mattk/src/scratch/osmo-like/v2")
	root, err := sql.ImportSnapshot(1, false)

	require.NoError(t, tree.LoadVersion(1))
	require.NoError(t, err)
	tree.root = root

	require.NoError(t, sql.WarmLeaves())
	testTreeBuild(t, tree, opts)
	require.NoError(t, sql.Close())
}

func TestOsmoLike_ColdStart(t *testing.T) {
	tmpDir := "/tmp/sortkey"

	pool := NewNodePool()
	sql, err := NewSqliteDb(pool, tmpDir, false)
	require.NoError(t, err)
	tree := NewTree(sql, pool)
	require.NoError(t, tree.LoadVersion(1))

	opts := testutil.CompactedChangelogs("/Users/mattk/src/scratch/osmo-like/v2")

	require.NoError(t, err)

	testTreeBuild(t, tree, opts)
	require.NoError(t, sql.Close())
}

func TestOsmoLike_InMemory(t *testing.T) {
	tmpDir := "/tmp/sortkey"

	pool := NewNodePool()
	sql, err := NewInMemorySqliteDb(pool)
	require.NoError(t, err)
	require.NoError(t, sql.LoadIntoMemory(tmpDir))

	tree := NewTree(sql, pool)
	require.NoError(t, tree.LoadVersion(1))

	opts := testutil.CompactedChangelogs("/Users/mattk/src/scratch/osmo-like/v2")

	require.NoError(t, err)

	testTreeBuild(t, tree, opts)
	require.NoError(t, sql.Close())
}

func TestTree_Import(t *testing.T) {
	tmpDir := "/Users/mattk/src/scratch/sqlite/height-zero"

	pool := NewNodePool()
	sql, err := NewSqliteDb(pool, tmpDir, false)
	require.NoError(t, err)

	root, err := sql.ImportSnapshot(1, true)
	require.NoError(t, err)
	require.NotNil(t, root)
}

func TestTree_Rehash(t *testing.T) {
	pool := NewNodePool()
	sql, err := NewSqliteDb(pool, "/Users/mattk/src/scratch/sqlite/height-zero", false)
	require.NoError(t, err)
	tree := NewTree(sql, pool)
	require.NoError(t, tree.LoadVersion(1))

	savedHash := make([]byte, 32)
	n := copy(savedHash, tree.root.hash)
	require.Equal(t, 32, n)
	var step func(node *Node)
	step = func(node *Node) {
		if node.isLeaf() {
			return
		}
		node.hash = nil
		step(node.left(tree))
		step(node.right(tree))
		node._hash()
	}
	step(tree.root)
	require.Equal(t, savedHash, tree.root.hash)
}
