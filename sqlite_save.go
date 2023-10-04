package iavl

import (
	"fmt"
	"time"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
	"github.com/dustin/go-humanize"
)

type sqlSaveVersion struct {
	sql          *SqliteDb
	tree         *Tree
	saveBranches bool
	batchSize    int
	currentBatch int
	lastCommit   time.Time
	branchInsert *sqlite3.Stmt
	branchUpdate *sqlite3.Stmt
	branchDelete *sqlite3.Stmt
	leafInsert   *sqlite3.Stmt
	leafUpdate   *sqlite3.Stmt
	leafDelete   *sqlite3.Stmt

	leafInserts   []*Node
	leafUpdates   []*Node
	branchInserts []*Node
	evictions     []*Node
}

func newSqlSaveVersion(sql *SqliteDb, tree *Tree) (*sqlSaveVersion, error) {
	sv := &sqlSaveVersion{
		sql:        sql,
		tree:       tree,
		batchSize:  200_000,
		lastCommit: time.Now(),
	}
	return sv, nil
}

func (sv *sqlSaveVersion) deepSaveV2(node *Node) (err error) {
	// invariants
	if node.isLeaf() {
		if node.hash == nil {
			panic("leaf hash is nil")
		}
		if node.leftNode != nil || node.rightNode != nil {
			panic("leaf has children")
		}
		if node.leafSeq == 0 {
			panic("leaf has leafSeq 0")
		}
	} else {
		if node.leafSeq != 0 {
			panic("branch has leafSeq != 0")
		}
	}

	if node.version != sv.tree.version {
		return
	}

	if !node.isLeaf() {
		if err = sv.deepSaveV2(node.left(sv.tree)); err != nil {
			return err
		}
		if err = sv.deepSaveV2(node.right(sv.tree)); err != nil {
			return err
		}
	}
	node._hash()

	if node.isLeaf() && node.version == sv.tree.version {
		if node.leafSeq > sv.tree.lastLeafSequence {
			sv.leafInserts = append(sv.leafInserts, node)
		} else {
			sv.leafUpdates = append(sv.leafUpdates, node)
		}
	} else {
		// branch invariants
		if node.leftLeaf != node.leftNode.leafSeq {
			panic("leftLeaf != leftNode.leafSeq")
		}
		if node.rightLeaf != node.rightNode.leafSeq {
			panic("rightLeaf != rightNode.leafSeq")
		}

		if node.version == sv.tree.version && sv.saveBranches {
			sv.branchInserts = append(sv.branchInserts, node)
		}

		if node.leftLeaf != 0 || node.rightLeaf != 0 {
			sv.evictions = append(sv.evictions, node)
		}
	}

	return nil
}

func (sv *sqlSaveVersion) upsert() error {
	if err := sv.prepare(); err != nil {
		return err
	}

	for _, node := range sv.leafInserts {
		if err := sv.leafInsert.Exec(int(node.leafSeq), node.key, node.version, node.hash); err != nil {
			return fmt.Errorf("leaf seq id %d failed: %w", node.leafSeq, err)
		}
		if err := sv.step(); err != nil {
			return err
		}
	}
	for _, node := range sv.leafUpdates {
		if err := sv.leafUpdate.Exec(node.key, node.version, node.hash, int(node.leafSeq)); err != nil {
			return err
		}
		if err := sv.step(); err != nil {
			return err
		}
	}
	for _, node := range sv.branchInserts {
		var leftSortKey, rightSortKey []byte
		if node.leftLeaf == 0 {
			leftSortKey = node.leftNode.sortKey
		}
		if node.rightLeaf == 0 {
			rightSortKey = node.rightNode.sortKey
		}

		if err := sv.branchInsert.Exec(
			node.sortKey,
			int(node.subtreeHeight),
			node.version,
			node.size,
			node.key,
			node.hash,
			leftSortKey,
			rightSortKey,
			int(node.leftLeaf),
			int(node.rightLeaf),
		); err != nil {
			return err
		}
		if err := sv.step(); err != nil {
			return err
		}
	}

	return nil
}

func (sv *sqlSaveVersion) deepSave(node *Node) (err error) {
	if node.sortKey == nil && node.leafSeq == 0 {
		panic("found a node with nil sortKey and leafSeq")
	}
	// abort traversal on clean nodes
	if node.version < sv.tree.version {
		return nil
	}
	// post-order, LRN traversal for non-leafs
	if !node.isLeaf() {
		if err = sv.deepSave(node.left(sv.tree)); err != nil {
			return err
		}
		if err = sv.deepSave(node.right(sv.tree)); err != nil {
			return err
		}
	}

	// hash & save node
	node._hash()
	switch {
	case node.isLeaf() && node.version >= sv.tree.version:
		if node.leafSeq > sv.tree.lastLeafSequence {
			if err = sv.leafInsert.Exec(int(node.leafSeq), node.version, node.key, node.hash); err != nil {
				return fmt.Errorf("leaf seq id %d failed: %w", node.leafSeq, err)
			}
		} else {
			// i may be able to skip storing/writing version to save some writes.
			if err = sv.leafUpdate.Exec(node.key, node.version, node.hash, int(node.leafSeq)); err != nil {
				return err
			}
		}
		sv.sql.metrics.WriteLeaves++
		sv.sql.pool.Put(node)
	case sv.saveBranches && (node.lastBranchKey == nil || !node.lastBranchKey.Equals(node)):
		var leftSortKey, rightSortKey []byte
		if node.leftLeaf == 0 {
			leftSortKey = node.leftNode.sortKey
		}
		if node.rightLeaf == 0 {
			rightSortKey = node.rightNode.sortKey
		}

		if err = sv.branchInsert.Exec(
			node.sortKey,
			int(node.subtreeHeight),
			node.version,
			node.size,
			node.key,
			node.hash,
			leftSortKey,
			rightSortKey,
			int(node.leftLeaf),
			int(node.rightLeaf),
		); err != nil {
			return err
		}
	case sv.saveBranches:
		// TODO
		panic("not implemented")

	default:
		//log.Warn().Msgf("nothing to do for node isLeaf=%t, saveBranches=%t", node.isLeaf(), sv.saveBranches)
		// not saving branches
	}
	sv.currentBatch++
	if sv.currentBatch%sv.batchSize == 0 {
		log.Info().Msgf("i=%s dur=%s rate=%s",
			humanize.Comma(int64(sv.currentBatch)),
			time.Since(sv.lastCommit).Round(time.Millisecond),
			humanize.Comma(int64(float64(sv.batchSize)/time.Since(sv.lastCommit).Seconds())))

		if err = sv.commit(); err != nil {
			return err
		}
		if err = sv.prepare(); err != nil {
			return err
		}
		sv.lastCommit = time.Now()
	}

	if node.leftLeaf != 0 {
		node.leftNode = nil
	}
	if node.rightLeaf != 0 {
		node.rightNode = nil
	}

	return nil
}

func (sv *sqlSaveVersion) prepare() (err error) {
	if err = sv.sql.write.Begin(); err != nil {
		return err
	}

	sv.branchInsert, err = sv.sql.write.Prepare("INSERT INTO branch (sort_key, s_height, version, size, key, hash, left_sort_key, right_sort_key, left_leaf_seq, right_leaf_seq) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	sv.branchUpdate, err = sv.sql.write.Prepare("UPDATE branch SET sort_key = ?, s_height = ?, version = ?, key = ?, hash = ?, left_sort_key = ?, right_sort_key = ?, left_s_height = ?, right_s_height = ?, left_leaf_seq = ?, right_leaf_seq = ? WHERE sort_key = ? AND s_height = ?")
	if err != nil {
		return err
	}
	sv.branchDelete, err = sv.sql.write.Prepare("DELETE FROM branch WHERE sort_key = ? AND s_height = ?")
	if err != nil {
		return err
	}

	sv.leafInsert, err = sv.sql.write.Prepare("INSERT INTO leaf (seq, key, version, hash) VALUES (?, ?, ?, ?)")
	if err != nil {
		return err
	}
	sv.leafUpdate, err = sv.sql.write.Prepare("UPDATE leaf SET key = ?, version = ?, hash = ? WHERE seq = ?")
	if err != nil {
		return err
	}
	sv.leafDelete, err = sv.sql.write.Prepare("DELETE FROM leaf WHERE seq = ?")
	if err != nil {
		return err
	}
	return nil
}

func (sv *sqlSaveVersion) step() error {
	sv.currentBatch++
	if sv.currentBatch%sv.batchSize == 0 {
		log.Info().Msgf("i=%s dur=%s rate=%s",
			humanize.Comma(int64(sv.currentBatch)),
			time.Since(sv.lastCommit).Round(time.Millisecond),
			humanize.Comma(int64(float64(sv.batchSize)/time.Since(sv.lastCommit).Seconds())))

		if err := sv.commit(); err != nil {
			return err
		}
		if err := sv.prepare(); err != nil {
			return err
		}
		sv.lastCommit = time.Now()
	}
	return nil
}

func (sv *sqlSaveVersion) commit() error {
	if err := sv.sql.write.Commit(); err != nil {
		return err
	}
	if err := sv.branchInsert.Close(); err != nil {
		return err
	}
	if err := sv.branchUpdate.Close(); err != nil {
		return err
	}
	if err := sv.branchDelete.Close(); err != nil {
		return err
	}
	if err := sv.leafInsert.Close(); err != nil {
		return err
	}
	if err := sv.leafUpdate.Close(); err != nil {
		return err
	}
	if err := sv.leafDelete.Close(); err != nil {
		return err
	}
	return nil
}

func (sv *sqlSaveVersion) finish() (err error) {
	if err = sv.commit(); err != nil {
		return err
	}
	if err = sv.sql.write.Exec("PRAGMA wal_checkpoint(RESTART);"); err != nil {
		return err
	}

	if sv.tree.version == 1 {
		// TODO this is a hack for tests
		if err = sv.tree.sql.write.Exec("CREATE INDEX leaf_idx on leaf (seq);"); err != nil {
			return err
		}
		if err = sv.tree.sql.write.Exec("CREATE UNIQUE INDEX branch_idx on branch (sort_key);"); err != nil {
			return err
		}
	}

	for _, node := range sv.evictions {
		if node.leftLeaf != 0 {
			sv.sql.pool.Put(node.leftNode)
			node.leftNode = nil
		}
		if node.rightLeaf != 0 {
			sv.sql.pool.Put(node.rightNode)
			node.rightNode = nil
		}
	}

	return nil
}
