package iavl

import (
	"fmt"
	"time"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
	"github.com/dustin/go-humanize"
	"github.com/rs/zerolog"
)

type SqliteBatch struct {
	sql           *SqliteDb
	version       int64
	size          int64
	logger        zerolog.Logger
	treeOpts      *TreeOptions
	branches      []*Node
	leaves        []*Node
	deletes       []*NodeDelete
	branchOrphans []NodeKey
	leafOrphans   []NodeKey

	treeCount int64
	treeSince time.Time
	leafCount int64
	leafSince time.Time

	leafInsert   *sqlite3.Stmt
	deleteInsert *sqlite3.Stmt
	latestInsert *sqlite3.Stmt
	latestDelete *sqlite3.Stmt
	treeInsert   *sqlite3.Stmt
	leafOrphan   *sqlite3.Stmt
	treeOrphan   *sqlite3.Stmt
}

func NewSQLiteBatch(
	sql *SqliteDb,
	version int64,
	size int64,
	logger zerolog.Logger) *SqliteBatch {
	return &SqliteBatch{
		sql:     sql,
		version: version,
		size:    size,
		logger:  logger,
	}
}

func (b *SqliteBatch) newChangeLogBatch() (err error) {
	if err = b.sql.leafWrite.Begin(); err != nil {
		return err
	}
	b.leafInsert, err = b.sql.leafWrite.Prepare("INSERT OR REPLACE INTO leaf (version, sequence, bytes) VALUES (?, ?, ?)")
	if err != nil {
		return err
	}
	b.deleteInsert, err = b.sql.leafWrite.Prepare("INSERT OR REPLACE INTO leaf_delete (version, sequence, key) VALUES (?, ?, ?)")
	if err != nil {
		return err
	}
	b.latestInsert, err = b.sql.leafWrite.Prepare("INSERT OR REPLACE INTO latest (key, value) VALUES (?, ?)")
	if err != nil {
		return err
	}
	b.latestDelete, err = b.sql.leafWrite.Prepare("DELETE FROM latest WHERE key = ?")
	if err != nil {
		return err
	}
	b.leafOrphan, err = b.sql.leafWrite.Prepare("INSERT INTO leaf_orphan (version, sequence, at) VALUES (?, ?, ?)")
	if err != nil {
		return err
	}
	b.leafSince = time.Now()
	return nil
}

func (b *SqliteBatch) changelogMaybeCommit() (err error) {
	if b.leafCount%b.size == 0 {
		if err = b.changelogBatchCommit(); err != nil {
			return err
		}
		if err = b.newChangeLogBatch(); err != nil {
			return err
		}
	}
	return nil
}

func (b *SqliteBatch) changelogBatchCommit() error {
	if err := b.sql.leafWrite.Commit(); err != nil {
		return err
	}
	if err := b.leafInsert.Close(); err != nil {
		return err
	}
	if err := b.deleteInsert.Close(); err != nil {
		return err
	}
	if err := b.latestInsert.Close(); err != nil {
		return err
	}
	if err := b.latestDelete.Close(); err != nil {
		return err
	}
	if err := b.leafOrphan.Close(); err != nil {
		return err
	}

	return nil
}

func (b *SqliteBatch) execBranchOrphan(nodeKey NodeKey) error {
	return b.treeOrphan.Exec(nodeKey.Version(), int(nodeKey.Sequence()), b.version)
}

func (b *SqliteBatch) newTreeBatch(checkpointVersion int64) (err error) {
	if err = b.sql.treeWrite.Begin(); err != nil {
		return err
	}
	b.treeInsert, err = b.sql.treeWrite.Prepare(fmt.Sprintf(
		"INSERT INTO tree_%d (version, sequence, bytes) VALUES (?, ?, ?)", checkpointVersion))
	if err != nil {
		return err
	}
	b.treeOrphan, err = b.sql.treeWrite.Prepare("INSERT INTO orphan (version, sequence, at) VALUES (?, ?, ?)")
	b.treeSince = time.Now()
	return err
}

func (b *SqliteBatch) treeBatchCommit() error {
	if err := b.sql.treeWrite.Commit(); err != nil {
		return err
	}
	if err := b.treeInsert.Close(); err != nil {
		return err
	}
	if err := b.treeOrphan.Close(); err != nil {
		return err
	}

	if b.treeCount >= b.size {
		batchSize := b.treeCount % b.size
		if batchSize == 0 {
			batchSize = b.size
		}
		b.logger.Debug().Msgf("db=tree count=%s dur=%s batch=%d rate=%s",
			humanize.Comma(int64(b.treeCount)),
			time.Since(b.treeSince).Round(time.Millisecond),
			batchSize,
			humanize.Comma(int64(float64(batchSize)/time.Since(b.treeSince).Seconds())))
	}
	return nil
}

func (b *SqliteBatch) treeMaybeCommit(checkpointVersion int64) (err error) {
	if b.treeCount%b.size == 0 {
		if err = b.treeBatchCommit(); err != nil {
			return err
		}
		if err = b.newTreeBatch(checkpointVersion); err != nil {
			return err
		}
	}
	return nil
}

func (b *SqliteBatch) saveLeaves() (int64, error) {
	var byteCount int64

	err := b.newChangeLogBatch()
	if err != nil {
		return 0, err
	}

	var (
		bz  []byte
		val []byte
	)
	for i, leaf := range b.leaves {
		b.leafCount++
		if b.tree.storeLatestLeaves {
			val = leaf.value
			leaf.value = nil
		}
		bz, err = leaf.Bytes()
		if err != nil {
			return 0, err
		}
		byteCount += int64(len(bz))
		if err = b.leafInsert.Exec(leaf.nodeKey.Version(), int(leaf.nodeKey.Sequence()), bz); err != nil {
			return 0, err
		}
		if b.tree.storeLatestLeaves {
			if err = b.latestInsert.Exec(leaf.key, val); err != nil {
				return 0, err
			}
		}
		if err = b.changelogMaybeCommit(); err != nil {
			return 0, err
		}
		if b.tree.heightFilter > 0 {
			if i != 0 {
				// evict leaf
				b.tree.returnNode(leaf)
			} else if leaf.nodeKey != b.tree.root.nodeKey {
				// never evict the root if it's a leaf
				b.tree.returnNode(leaf)
			}
		}
	}

	for _, leafDelete := range tree.deletes {
		b.leafCount++
		err = b.deleteInsert.Exec(leafDelete.deleteKey.Version(), int(leafDelete.deleteKey.Sequence()), leafDelete.leafKey)
		if err != nil {
			return 0, err
		}
		if tree.storeLatestLeaves {
			if err = b.latestDelete.Exec(leafDelete.leafKey); err != nil {
				return 0, err
			}
		}
		if err = b.changelogMaybeCommit(); err != nil {
			return 0, err
		}
	}

	for _, orphan := range tree.leafOrphans {
		b.leafCount++
		err = b.leafOrphan.Exec(orphan.Version(), int(orphan.Sequence()), b.tree.version)
		if err != nil {
			return 0, err
		}
		if err = b.changelogMaybeCommit(); err != nil {
			return 0, err
		}
	}

	if err = b.changelogBatchCommit(); err != nil {
		return 0, err
	}

	err = tree.sql.leafWrite.Exec("CREATE INDEX IF NOT EXISTS leaf_idx ON leaf (version, sequence)")
	if err != nil {
		return byteCount, err
	}

	return byteCount, nil
}

func (b *SqliteBatch) isCheckpoint() bool {
	return len(b.tree.branches) > 0
}

func (b *SqliteBatch) saveBranches() (n int64, err error) {
	if b.isCheckpoint() {
		tree := b.tree
		b.treeCount = 0

		log.Info().Msgf("checkpointing version=%d path=%s", tree.version, tree.sql.opts.Path)
		if err := tree.sql.NextShard(tree.version); err != nil {
			return 0, err
		}

		if err = b.newTreeBatch(tree.version); err != nil {
			return 0, err
		}

		for _, node := range tree.branches {
			b.treeCount++
			bz, err := node.Bytes()
			if err != nil {
				return 0, err
			}
			if err = b.treeInsert.Exec(node.nodeKey.Version(), int(node.nodeKey.Sequence()), bz); err != nil {
				return 0, err
			}
			if err = b.treeMaybeCommit(tree.version); err != nil {
				return 0, err
			}
			if node.evict {
				tree.returnNode(node)
			}
		}

		b.logger.Debug().Msgf("db=tree orphans=%s", humanize.Comma(int64(len(tree.branchOrphans))))

		for _, orphan := range tree.branchOrphans {
			b.treeCount++
			err = b.execBranchOrphan(orphan)
			if err != nil {
				return 0, err
			}
			if err = b.treeMaybeCommit(tree.version); err != nil {
				return 0, err
			}
		}

		if err = b.treeBatchCommit(); err != nil {
			return 0, err
		}
		err = b.sql.treeWrite.Exec(fmt.Sprintf(
			"CREATE INDEX IF NOT EXISTS tree_idx_%d ON tree_%d (version, sequence);", tree.version, tree.version))
		if err != nil {
			return 0, err
		}
	}

	return b.treeCount, nil
}
