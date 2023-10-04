package iavl

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aybabtme/uniplot/histogram"
	"github.com/bvinc/go-sqlite-lite/sqlite3"
	"github.com/cosmos/iavl/v2/metrics"
	"github.com/dustin/go-humanize"
)

type SqliteDb struct {
	connString string
	write      *sqlite3.Conn
	read       *sqlite3.Conn

	pool *NodePool

	shardId      int64
	shards       map[int64]*sqlite3.Stmt
	versionShard map[int64]int64

	queryLeaf   *sqlite3.Stmt
	queryBranch *sqlite3.Stmt

	queryDurations   []time.Duration
	queryTime        time.Duration
	queryCount       int64
	queryLeafMiss    int64
	queryLeafCount   int64
	queryBranchCount int64

	metrics *metrics.TreeMetrics
}

func (sql *SqliteDb) init(newDb bool) error {
	var err error
	sql.write, err = sqlite3.Open(sql.connString)
	if err != nil {
		return err
	}

	err = sql.write.Exec("PRAGMA synchronous=OFF;")
	if err != nil {
		return err
	}

	// wal_autocheckpoint is in pages, so we need to convert maxWalSizeBytes to pages
	maxWalSizeBytes := 1024 * 1024 * 500
	if err = sql.write.Exec(fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", maxWalSizeBytes/os.Getpagesize())); err != nil {
		return err
	}

	if err = sql.write.Exec(fmt.Sprintf("PRAGMA cache_size=%d;", -8*1024*1024*1024)); err != nil {
		return err
	}

	if newDb {
		if err = sql.initNewDb(); err != nil {
			return err
		}
	}

	return nil
}

func NewInMemorySqliteDb(pool *NodePool) (*SqliteDb, error) {
	sql := &SqliteDb{
		shards:       make(map[int64]*sqlite3.Stmt),
		versionShard: make(map[int64]int64),
		connString:   "file::memory:?cache=shared",
		pool:         pool,
	}
	if err := sql.init(true); err != nil {
		return nil, err
	}
	return sql, nil
}

func (sql *SqliteDb) LoadIntoMemory(path string) error {
	connString := fmt.Sprintf("file:%s/iavl-v2.db", path)
	if err := sql.write.Exec("ATTACH DATABASE ? AS disk", connString); err != nil {
		return err
	}
	if err := sql.write.Exec("INSERT INTO main.leaf SELECT * FROM disk.leaf;"); err != nil {
		return err
	}
	if err := sql.resetReadConn(); err != nil {
		return err
	}

	q, err := sql.read.Prepare("SELECT count(*) FROM leaf")
	if err != nil {
		return err
	}
	if _, err = q.Step(); err != nil {
		return err
	}
	count, ok, err := q.ColumnInt64(0)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("count not found")
	}
	log.Info().Msgf("loaded leaf count: %s", humanize.Comma(count))
	if err = q.Close(); err != nil {
		return err
	}

	err = sql.write.Exec("INSERT INTO main.branch SELECT * FROM disk.branch;")
	if err != nil {
		return err
	}
	q, err = sql.read.Prepare("SELECT count(*) FROM branch")
	if err != nil {
		return err
	}
	if _, err = q.Step(); err != nil {
		return err
	}
	count, ok, err = q.ColumnInt64(0)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("count not found")
	}
	log.Info().Msgf("loaded branch count: %s", humanize.Comma(count))
	if err = q.Close(); err != nil {
		return err
	}

	if err = sql.write.Exec("INSERT INTO main.root SELECT * FROM disk.root;"); err != nil {
		return err
	}

	since := time.Now()
	log.Info().Msgf("creating indexes")
	if err = sql.write.Exec("CREATE INDEX leaf_idx ON leaf (seq); CREATE INDEX branch_idx ON branch (sort_key);"); err != nil {
		return err
	}
	log.Info().Msgf("indexes creation took %s", time.Since(since).Round(time.Millisecond))

	if err = sql.write.Exec("DETACH DATABASE disk;"); err != nil {
		return err
	}

	return nil
}

func NewSqliteDb(pool *NodePool, path string, newDb bool) (*SqliteDb, error) {
	sql := &SqliteDb{
		shards:       make(map[int64]*sqlite3.Stmt),
		versionShard: make(map[int64]int64),
		connString:   fmt.Sprintf("file:%s/iavl-v2.db", path),
		pool:         pool,
	}
	if err := sql.init(newDb); err != nil {
		return nil, err
	}

	return sql, nil
}

func (sql *SqliteDb) newReadConn() (*sqlite3.Conn, error) {
	conn, err := sqlite3.Open(sql.connString)
	if strings.Contains(sql.connString, "file::memory") {
		return conn, nil
	}

	if err != nil {
		return nil, err
	}
	err = conn.Exec(fmt.Sprintf("PRAGMA mmap_size=%d;", 8*1024*1024*1024))
	if err != nil {
		return nil, err
	}
	err = conn.Exec(fmt.Sprintf("PRAGMA cache_size=%d;", -8*1024*1024*1024))
	return conn, nil
}

func (sql *SqliteDb) resetReadConn() (err error) {
	if sql.read != nil {
		if sql.queryBranch != nil {
			if err = sql.queryBranch.Close(); err != nil {
				return err
			}
			sql.queryBranch = nil
		}
		if sql.queryLeaf != nil {
			if err = sql.queryLeaf.Close(); err != nil {
				return err
			}
			sql.queryLeaf = nil
		}

		err = sql.read.Close()
		if err != nil {
			return fmt.Errorf("failed to close read conn: %w", err)
		}
	}
	sql.read, err = sql.newReadConn()
	return err
}

func (sql *SqliteDb) getReadConn() (*sqlite3.Conn, error) {
	var err error
	if sql.read == nil {
		sql.read, err = sql.newReadConn()
	}
	return sql.read, err
}

func (sql *SqliteDb) initNewDb() error {
	err := sql.write.Exec(`
CREATE TABLE root (version int, sort_key blob, s_height int, PRIMARY KEY (version));
CREATE TABLE changelog (version int, sequence int, bytes blob);
CREATE TABLE leaf (seq int, version int, key blob, hash blob);
CREATE TABLE branch (
    sort_key blob, 
    s_height int, 
    version int, 
    size int, 
    key blob, 
    hash blob, 
    left_sort_key blob, 
    right_sort_key blob, 
    left_s_height int, 
    right_s_height int,
    left_leaf_seq int,
    right_leaf_seq int
);
CREATE TABLE shard (version int, id int, PRIMARY KEY (version, id));`)
	if err != nil {
		return err
	}

	pageSize := os.Getpagesize()
	log.Info().Msgf("setting page size to %s", humanize.Bytes(uint64(pageSize)))
	err = sql.write.Exec(fmt.Sprintf("PRAGMA page_size=%d; VACUUM;", pageSize))
	if err != nil {
		return err
	}
	err = sql.write.Exec("PRAGMA journal_mode=WAL;")
	if err != nil {
		return err
	}

	return nil
}

func (sql *SqliteDb) BatchNodeDiffs(diffs []*nodeDiff) error {
	batchSize := 200_000
	var deleteStmt, updateStmt *sqlite3.Stmt
	newBatch := func() error {
		err := sql.write.Begin()
		if err != nil {
			return err
		}
		deleteStmt, err = sql.write.Prepare("DELETE FROM latest WHERE sort_key = ? AND height = ?")
		if err != nil {
			return err
		}
		updateStmt, err = sql.write.Prepare("UPDATE latest SET sort_key = ?, height = ?, bytes = ? WHERE sort_key = ? AND height = ?")
		if err != nil {
			return err
		}
		return nil
	}
	since := time.Now()
	for i, diff := range diffs {
		if diff.delete {
			err := deleteStmt.Exec(diff.prevSortKey, int(diff.prevHeight))
			if err != nil {
				return err
			}
		} else {
			bz, err := diff.new.Bytes()
			if err != nil {
				return err
			}
			err = updateStmt.Exec(diff.new.sortKey, int(diff.new.subtreeHeight), bz, diff.prevSortKey, int(diff.prevHeight))
			if err != nil {
				return err
			}
		}
		if i != 0 && i%batchSize == 0 {
			err := sql.write.Commit()
			if err != nil {
				return err
			}
			err = deleteStmt.Close()
			if err != nil {
				return err
			}
			err = updateStmt.Close()
			if err != nil {
				return err
			}
			err = newBatch()
			if err != nil {
				return err
			}

			log.Info().Msgf("i=%s dur=%s rate=%s",
				humanize.Comma(int64(i)),
				time.Since(since).Round(time.Millisecond),
				humanize.Comma(int64(float64(batchSize)/time.Since(since).Seconds())))
			since = time.Now()
		}
	}
	err := sql.write.Commit()
	if err != nil {
		return err
	}
	return nil
}

func (sql *SqliteDb) BatchSet(nodes []*Node, leaves bool) (n int64, versions []int64, err error) {
	batchSize := 200_000
	var byteCount int64
	versionMap := make(map[int64]bool)

	logger := log.With().Str("op", "batch-set").Logger()

	newBatch := func() (*sqlite3.Stmt, error) {
		err = sql.write.Begin()
		if err != nil {
			return nil, err
		}

		var stmt *sqlite3.Stmt
		stmt, err = sql.write.Prepare("INSERT INTO latest (sort_key, height, bytes) VALUES (?, ?, ?)")

		if err != nil {
			return nil, err
		}
		return stmt, nil
	}
	stmt, err := newBatch()
	if err != nil {
		return 0, versions, err
	}
	since := time.Now()
	for i, node := range nodes {
		versionMap[node.version] = true
		bz, err := node.Bytes()
		byteCount += int64(len(bz))
		if err != nil {
			return 0, versions, err
		}
		err = stmt.Exec(node.sortKey, int(node.subtreeHeight), bz)
		if err != nil {
			return 0, versions, err
		}
		if i != 0 && i%batchSize == 0 {
			err := sql.write.Commit()
			if err != nil {
				return 0, versions, err
			}
			err = stmt.Close()
			if err != nil {
				return 0, versions, err
			}
			stmt, err = newBatch()
			if err != nil {
				return 0, versions, err
			}

			logger.Info().Msgf("i=%s dur=%s rate=%s",
				humanize.Comma(int64(i)),
				time.Since(since).Round(time.Millisecond),
				humanize.Comma(int64(float64(batchSize)/time.Since(since).Seconds())))
			since = time.Now()
		}
		if leaves {
			sql.pool.Put(node)
		}
	}
	err = sql.write.Commit()
	if err != nil {
		return 0, versions, err
	}
	err = stmt.Close()
	if err != nil {
		return 0, versions, err
	}

	if !leaves {
		err = sql.write.Exec(fmt.Sprintf(
			"CREATE INDEX IF NOT EXISTS tree_idx_%d ON tree_%d (version, sequence);", sql.shardId, sql.shardId))
		if err != nil {
			return 0, versions, err
		}
	}

	err = sql.write.Exec("PRAGMA wal_checkpoint(RESTART);")
	if err != nil {
		return 0, versions, err
	}

	for version := range versionMap {
		versions = append(versions, version)
	}
	return byteCount, versions, nil
}

func (sql *SqliteDb) GetShardQuery(version int64) (*sqlite3.Stmt, error) {
	id, ok := sql.versionShard[version]
	if !ok {
		return nil, fmt.Errorf("shard not found for version %d", version)
	}
	q, ok := sql.shards[id]
	if !ok {
		return nil, fmt.Errorf("shard query not found for id %d", id)
	}
	return q, nil
}

func (sql *SqliteDb) getLeaf(seq leafSeq) (*Node, error) {
	start := time.Now()

	var err error
	if sql.queryLeaf == nil {
		log.Info().Msgf("preparing leaf query")
		sql.queryLeaf, err = sql.read.Prepare("SELECT version, key, hash FROM leaf WHERE seq = ?")
		if err != nil {
			return nil, err
		}
	}

	if err = sql.queryLeaf.Bind(int(seq)); err != nil {
		return nil, err
	}
	hasRow, err := sql.queryLeaf.Step()
	if err != nil {
		return nil, err
	}
	if !hasRow {
		return nil, fmt.Errorf("leaf not found: %d", seq)
	}

	//log.Info().Msgf("leaf found: %d", seq)

	leaf := sql.pool.Get()
	err = sql.queryLeaf.Scan(&leaf.version, &leaf.key, &leaf.hash)
	if err != nil {
		return nil, err
	}
	leaf.leafSeq = seq
	leaf.size = 1

	err = sql.queryLeaf.Reset()
	if err != nil {
		return nil, err
	}

	dur := time.Since(start)
	sql.queryDurations = append(sql.queryDurations, dur)
	sql.queryTime += dur
	sql.queryCount++
	sql.queryLeafCount++

	return leaf, nil
}

func (sql *SqliteDb) getNode(nodeKey NodeKey, q *sqlite3.Stmt) (*Node, error) {
	start := time.Now()

	if err := q.Bind(nodeKey.Version(), int(nodeKey.Sequence())); err != nil {
		return nil, err
	}
	hasRow, err := q.Step()
	if !hasRow {
		return nil, fmt.Errorf("node not found: %v; shard=%d", nodeKey, sql.versionShard[nodeKey.Version()])
	}
	if err != nil {
		return nil, err
	}
	//nodeBz, err := q.ColumnBlob(0)
	var nodeBz sqlite3.RawBytes
	err = q.Scan(&nodeBz)
	node, err := MakeNode(sql.pool, nodeKey, nodeBz)
	if err != nil {
		return nil, err
	}
	err = q.Reset()
	if err != nil {
		return nil, err
	}

	dur := time.Since(start)
	sql.queryDurations = append(sql.queryDurations, dur)
	sql.queryTime += dur
	sql.queryCount++
	sql.queryBranchCount++

	return node, nil
}

func (sql *SqliteDb) getBranch(bk *branchKey) (*Node, error) {
	start := time.Now()

	if bk == nil {
		panic("branchKey is nil")
	}
	if sql.queryBranch == nil {
		var err error
		sql.queryBranch, err = sql.read.Prepare("" +
			"SELECT sort_key, s_height, version, size, key, hash, " +
			"left_sort_key, right_sort_key, left_leaf_seq, right_leaf_seq " +
			"FROM branch WHERE sort_key = ?")
		if err != nil {
			return nil, err
		}
	}
	if err := sql.queryBranch.Bind(bk.sortKey); err != nil {
		return nil, err
	}
	hasRow, err := sql.queryBranch.Step()
	if !hasRow {
		return nil, fmt.Errorf("branch not found: %v; hex=%x", bk, bk.sortKey)
	}
	if err != nil {
		return nil, err
	}
	var (
		leftSortKey, rightSortKey          []byte
		leftLeafSeq, rightLeafSeq, sHeight int
	)
	node := sql.pool.Get()
	err = sql.queryBranch.Scan(
		&node.sortKey,
		&sHeight,
		&node.version,
		&node.size,
		&node.key,
		&node.hash,
		&leftSortKey,
		&rightSortKey,
		&leftLeafSeq,
		&rightLeafSeq,
	)
	if err != nil {
		return nil, err
	}

	node.subtreeHeight = int8(sHeight)
	node.leftLeaf = leafSeq(leftLeafSeq)
	node.rightLeaf = leafSeq(rightLeafSeq)

	if leftLeafSeq == 0 {
		node.leftBranch = &branchKey{
			sortKey: leftSortKey,
		}
	}
	if rightLeafSeq == 0 {
		node.rightBranch = &branchKey{
			sortKey: rightSortKey,
		}
	}

	if fmt.Sprintf("%x", leftSortKey) == "a9bf8d1e" {
		fmt.Println("leftSortKey", leftSortKey)
	}

	if err = sql.queryBranch.Reset(); err != nil {
		return nil, err
	}

	dur := time.Since(start)
	sql.queryDurations = append(sql.queryDurations, dur)
	sql.queryTime += dur
	sql.queryCount++
	sql.queryBranchCount++

	return node, nil
}

func (sql *SqliteDb) Delete(nodeKey []byte) error {
	return nil
}

func (sql *SqliteDb) Close() error {
	for _, q := range sql.shards {
		err := q.Close()
		if err != nil {
			return err
		}
	}
	if sql.read != nil {
		if sql.queryLeaf != nil {
			if err := sql.queryLeaf.Close(); err != nil {
				return err
			}
		}
		if err := sql.read.Close(); err != nil {
			return err
		}
	}
	if err := sql.write.Close(); err != nil {
		return err
	}
	return nil
}

func (sql *SqliteDb) MapVersions(versions []int64, shardId int64) error {
	err := sql.write.Begin()
	if err != nil {
		return err
	}
	stmt, err := sql.write.Prepare("INSERT INTO shard(version, id) VALUES (?, ?)")
	if err != nil {
		return err
	}

	defer stmt.Close()
	for _, version := range versions {
		err := stmt.Exec(version, shardId)
		if err != nil {
			return err
		}
		sql.versionShard[version] = shardId
	}
	return sql.write.Commit()
}

func (sql *SqliteDb) NextShard() error {
	// initialize shardId if not done so. done with a new connection.
	if sql.shardId == 0 {
		conn, err := sqlite3.Open(sql.connString)
		if err != nil {
			return err
		}
		q, err := conn.Prepare("SELECT MAX(id) FROM shard")
		if err != nil {
			return err
		}
		_, err = q.Step()
		if err != nil {
			return err
		}

		// if table is empty MAX query will bind sql.shardId to zero
		err = q.Scan(&sql.shardId)
		if err != nil {
			return err
		}

		if err := q.Close(); err != nil {
			return err
		}
		if err := conn.Close(); err != nil {
			return err
		}
	}

	sql.shardId++

	// hack to maintain 1 shard for testing
	if sql.shardId > 1 {
		sql.shardId = 1
		return nil
	}

	log.Info().Msgf("creating shard %d", sql.shardId)

	err := sql.write.Exec(fmt.Sprintf("CREATE TABLE tree_%d (version int, sequence int, bytes blob);",
		sql.shardId))
	if err != nil {
		return err
	}
	return err
}

func (sql *SqliteDb) IndexShard(shardId int64) error {
	err := sql.write.Exec(fmt.Sprintf("CREATE INDEX tree_%d_node_key_idx ON tree_%d (node_key);", shardId, shardId))
	return err
}

func (sql *SqliteDb) SaveRoot(version int64, node *Node) error {
	err := sql.write.Exec("INSERT INTO root(version, sort_key, s_height) VALUES (?, ?, ?)",
		version, node.sortKey, int(node.subtreeHeight))
	return err
}

func (sql *SqliteDb) LoadRoot(version int64) (*Node, error) {
	conn, err := sql.newReadConn()
	if err != nil {
		return nil, err
	}
	rootQuery, err := conn.Prepare("SELECT sort_key, s_height FROM root WHERE version = ?", version)
	if err != nil {
		return nil, err
	}

	hasRow, err := rootQuery.Step()
	if !hasRow {
		return nil, fmt.Errorf("root not found for version %d", version)
	}
	if err != nil {
		return nil, err
	}

	var (
		sortKey []byte
		sHeight int
	)
	err = rootQuery.Scan(&sortKey, &sHeight)
	if err != nil {
		return nil, err
	}

	if err = rootQuery.Close(); err != nil {
		return nil, err
	}

	// TODO this placement seems wrong?
	//if err := sql.resetShardQueries(); err != nil {
	//	return nil, err
	//}
	if err = sql.resetReadConn(); err != nil {
		return nil, err
	}

	rootNode, err := sql.getBranch(&branchKey{sortKey, int8(sHeight)})
	if err != nil {
		return nil, err
	}
	if err = conn.Close(); err != nil {
		return nil, err
	}
	return rootNode, nil
}

func (sql *SqliteDb) addShardQuery() error {
	panic("not supported")
	if _, ok := sql.shards[sql.shardId]; ok {
		return nil
	}
	if sql.read == nil {
		if err := sql.resetReadConn(); err != nil {
			return err
		}
	}

	q, err := sql.read.Prepare(fmt.Sprintf(
		"SELECT bytes FROM tree_%d WHERE version = ? AND sequence = ?", sql.shardId))
	if err != nil {
		return err
	}
	sql.shards[sql.shardId] = q
	return nil
}

func (sql *SqliteDb) resetShardQueries() error {
	for _, q := range sql.shards {
		err := q.Close()
		if err != nil {
			return err
		}
	}

	if sql.read == nil {
		// single reader conn for all shards. keep open to fill mmap, but reset queries periodically to flush WAL
		if err := sql.resetReadConn(); err != nil {
			return err
		}
	}

	q, err := sql.read.Prepare("SELECT DISTINCT id FROM shard")
	if err != nil {
		return err
	}
	for {
		ok, err := q.Step()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		var shardId int64
		err = q.Scan(&shardId)
		if err != nil {
			return err
		}
		sql.shards[shardId], err = sql.read.Prepare(
			fmt.Sprintf("SELECT bytes FROM tree_%d WHERE version = ? AND sequence = ?", shardId))
		if err != nil {
			return err
		}
	}
	err = q.Close()
	if err != nil {
		return err
	}

	q, err = sql.read.Prepare("SELECT version, id FROM shard")
	for {
		ok, err := q.Step()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		var version, shardId int64
		err = q.Scan(&version, &shardId)
		if err != nil {
			return err
		}
		sql.versionShard[version] = shardId
	}

	return q.Close()
}

func (sql *SqliteDb) queryReport(bins int) error {
	if sql.queryCount == 0 {
		return nil
	}

	fmt.Printf("queries=%s q/s=%s dur/q=%s dur=%s leaf-q=%s branch-q=%s leaf-miss=%s\n",
		humanize.Comma(sql.queryCount),
		humanize.Comma(int64(float64(sql.queryCount)/sql.queryTime.Seconds())),
		time.Duration(int64(sql.queryTime)/sql.queryCount),
		sql.queryTime.Round(time.Millisecond),
		humanize.Comma(sql.queryLeafCount),
		humanize.Comma(sql.queryBranchCount),
		humanize.Comma(sql.queryLeafMiss),
	)

	if bins > 0 {
		var histData []float64
		for _, d := range sql.queryDurations {
			if d > 50*time.Microsecond {
				continue
			}
			histData = append(histData, float64(d))
		}
		hist := histogram.Hist(bins, histData)
		err := histogram.Fprintf(os.Stdout, hist, histogram.Linear(10), func(v float64) string {
			return time.Duration(v).String()
		})
		if err != nil {
			return err
		}
	}

	sql.queryDurations = nil
	sql.queryTime = 0
	sql.queryCount = 0
	sql.queryLeafMiss = 0
	sql.queryLeafCount = 0
	sql.queryBranchCount = 0

	return nil
}

func (sql *SqliteDb) WarmLeaves() error {
	start := time.Now()
	read, err := sql.getReadConn()
	if err != nil {
		return err
	}
	stmt, err := read.Prepare("SELECT version, sequence, bytes FROM leaf")
	if err != nil {
		return err
	}
	var cnt int64
	for {
		ok, err := stmt.Step()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		cnt++
		var version, seq int64
		var bz sqlite3.RawBytes
		err = stmt.Scan(&version, &seq, &bz)
		if err != nil {
			return err
		}
		if cnt%5_000_000 == 0 {
			log.Info().Msgf("warmed %s leaves", humanize.Comma(cnt))
		}
	}

	log.Info().Msgf("warmed %s leaves in %s", humanize.Comma(cnt), time.Since(start))
	return stmt.Close()
}

type sqliteNode struct {
	ordinal  int
	version  int
	sequence int
	bz       []byte
}

func (sql *SqliteDb) Snapshot(ctx context.Context, tree *Tree, version int64) error {
	err := sql.write.Exec(
		fmt.Sprintf("CREATE TABLE snapshot_%d (ordinal int, version int, sequence int, bytes blob);", version))
	if err != nil {
		return err
	}
	err = tree.LoadVersion(version)
	if err != nil {
		return err
	}

	snapshot := &sqliteSnapshot{
		ctx:       ctx,
		conn:      sql.write,
		batchSize: 200_000,
		version:   version,
		getLeft: func(node *Node) *Node {
			return node.left(tree)
		},
		getRight: func(node *Node) *Node {
			return node.right(tree)
		},
	}
	if err = snapshot.prepareWrite(); err != nil {
		return err
	}
	if err = snapshot.writeStep(tree.root); err != nil {
		return err
	}
	if err = snapshot.flush(); err != nil {
		return err
	}
	log.Info().Msgf("creating index on snapshot_%d", version)
	err = sql.write.Exec(fmt.Sprintf("CREATE INDEX snapshot_%d_idx ON snapshot_%d (ordinal);", version, version))
	return err
}

type sqliteSnapshot struct {
	ctx       context.Context
	conn      *sqlite3.Conn
	batch     *sqlite3.Stmt
	lastWrite time.Time
	ordinal   int
	batchSize int
	version   int64
	getLeft   func(*Node) *Node
	getRight  func(*Node) *Node
}

func (snap *sqliteSnapshot) writeStep(node *Node) error {
	snap.ordinal++
	// Pre-order, NLR traversal
	// Visit this node
	nodeBz, err := node.Bytes()
	if err != nil {
		return err
	}
	// TODO sequence?
	err = snap.batch.Exec(snap.ordinal, node.version, 0, nodeBz)
	if err != nil {
		return err
	}

	if snap.ordinal%snap.batchSize == 0 {
		if err = snap.flush(); err != nil {
			return err
		}
		if err = snap.prepareWrite(); err != nil {
			return err
		}
	}

	if node.isLeaf() {
		return nil
	}

	// traverse left
	err = snap.writeStep(snap.getLeft(node))
	if err != nil {
		return err
	}

	// traverse right
	return snap.writeStep(snap.getRight(node))
}

func (snap *sqliteSnapshot) flush() error {
	select {
	case <-snap.ctx.Done():
		log.Info().Msgf("snapshot cancelled at ordinal=%s", humanize.Comma(int64(snap.ordinal)))
		return errors.Join(
			snap.batch.Reset(),
			snap.batch.Close(),
			snap.conn.Rollback(),
			snap.conn.Close())
	default:
	}

	log.Info().Msgf("flush total=%s, batch=%s, dur=%s, wr/s=%s",
		humanize.Comma(int64(snap.ordinal)),
		humanize.Comma(int64(snap.batchSize)),
		time.Since(snap.lastWrite).Round(time.Millisecond),
		humanize.Comma(int64(float64(snap.batchSize)/time.Since(snap.lastWrite).Seconds())),
	)

	err := snap.conn.Commit()
	if err != nil {
		return err
	}
	err = snap.batch.Close()
	if err != nil {
		return err
	}
	snap.lastWrite = time.Now()
	return nil
}

func (snap *sqliteSnapshot) prepareWrite() error {
	err := snap.conn.Begin()
	if err != nil {
		return err
	}
	insert := fmt.Sprintf("INSERT INTO snapshot_%d (ordinal, version, sequence, bytes) VALUES (?, ?, ?, ?);",
		snap.version)
	snap.batch, err = snap.conn.Prepare(insert)
	return err
}

func (sql *SqliteDb) ImportSnapshot(version int64, loadLeaves bool) (*Node, error) {
	read, err := sql.getReadConn()
	if err != nil {
		return nil, err
	}
	q, err := read.Prepare(fmt.Sprintf("SELECT version, sequence, bytes FROM snapshot_%d ORDER BY ordinal", version))
	if err != nil {
		return nil, err
	}
	defer func(q *sqlite3.Stmt) {
		err = q.Close()
		if err != nil {
			log.Error().Err(err).Msg("error closing import query")
		}
	}(q)

	imp := &sqliteImport{
		query:      q,
		pool:       sql.pool,
		loadLeaves: loadLeaves,
		since:      time.Now(),
	}
	root, err := imp.step()
	if err != nil {
		return nil, err
	}

	if !loadLeaves {
		return root, nil
	}

	// if full tree was loaded then rehash the full tree to validate integrity of the snapshot
	tree := NewTree(sql, sql.pool)
	if err = tree.LoadVersion(version); err != nil {
		return nil, err
	}

	rehashTree(root)
	if !bytes.Equal(root.hash, tree.root.hash) {
		return nil, fmt.Errorf("root hash mismatch: %x != %x", root.hash, tree.root.hash)
	}

	return root, nil
}

func rehashTree(node *Node) {
	if node.isLeaf() {
		return
	}
	node.hash = nil

	rehashTree(node.leftNode)
	rehashTree(node.rightNode)

	node._hash()
}

type sqliteImport struct {
	query      *sqlite3.Stmt
	pool       *NodePool
	loadLeaves bool

	i     int64
	since time.Time
}

func (sqlImport *sqliteImport) step() (node *Node, err error) {
	sqlImport.i++
	if sqlImport.i%1_000_000 == 0 {
		log.Debug().Msgf("import: nodes=%s, node/s=%s",
			humanize.Comma(sqlImport.i),
			humanize.Comma(int64(float64(1_000_000)/time.Since(sqlImport.since).Seconds())),
		)
		sqlImport.since = time.Now()
	}

	hasRow, err := sqlImport.query.Step()
	if !hasRow {
		return nil, sqlImport.query.Reset()
	}
	if err != nil {
		return nil, err
	}
	var bz sqlite3.RawBytes
	var version, seq int
	err = sqlImport.query.Scan(&version, &seq, &bz)
	if err != nil {
		return nil, err
	}
	nodeKey := NewNodeKey(int64(version), uint32(seq))
	node, err = MakeNode(sqlImport.pool, nodeKey, bz)
	if err != nil {
		return nil, err
	}

	if node.isLeaf() {
		if sqlImport.loadLeaves {
			return node, nil
		}
		sqlImport.pool.Put(node)
		return nil, nil
	}

	node.leftNode, err = sqlImport.step()
	if err != nil {
		return nil, err
	}
	node.rightNode, err = sqlImport.step()
	if err != nil {
		return nil, err
	}
	return node, nil
}

func (sql *SqliteDb) getRightNode(node *Node) (*Node, error) {
	var err error
	if node.rightLeaf != 0 {
		node.rightNode, err = sql.getLeaf(node.rightLeaf)
		if err != nil {
			return nil, err
		}
	} else {
		node.rightNode, err = sql.getBranch(node.rightBranch)
		if err != nil {
			return nil, err
		}
	}

	return node.rightNode, nil
}

func (sql *SqliteDb) getLeftNode(node *Node) (*Node, error) {
	if sql == nil {
		panic("what")
	}
	var err error
	if node.leftLeaf != 0 {
		node.leftNode, err = sql.getLeaf(node.leftLeaf)
		if err != nil {
			return nil, err
		}
	} else {
		if node.leftBranch == nil {
			return nil, fmt.Errorf("leftBranch is nil: node.sortKey=%x, height=%d", node.sortKey, node.subtreeHeight)
		}
		node.leftNode, err = sql.getBranch(node.leftBranch)
		if err != nil {
			return nil, err
		}
	}

	return node.leftNode, nil
}

func (sql *SqliteDb) getLastLeafSeq() (leafSeq, error) {
	q, err := sql.read.Prepare("SELECT MAX(seq) FROM leaf")
	if err != nil {
		return 0, err
	}
	hasRow, err := q.Step()
	if err != nil {
		return 0, err
	}
	if !hasRow {
		return 0, nil
	}
	var seq int
	err = q.Scan(&seq)
	if err != nil {
		return 0, err
	}
	if err = q.Close(); err != nil {
		return 0, err
	}
	return leafSeq(seq), nil
}
