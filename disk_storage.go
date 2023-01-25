package iavl

import (
	"bytes"
	"encoding/hex"
	"errors"
	"os"
	"path"
	"sort"

	dbm "github.com/cosmos/cosmos-db"
)

type DiskStorage struct {
	path string
}

func (d DiskStorage) filePath(key []byte) string {
	return path.Join(d.path, hex.EncodeToString(key))
}

func (d DiskStorage) Get(key []byte) ([]byte, error) {
	return os.ReadFile(d.filePath(key))
}

func (d DiskStorage) Has(key []byte) (bool, error) {
	_, err := os.Stat(d.filePath(key))
	if err == nil {
		return true, nil
	} else if errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else {
		return false, err
	}
}

func (d DiskStorage) Set(key []byte, value []byte) error {
	return os.WriteFile(d.filePath(key), value, 0644)
}

func (d DiskStorage) SetSync(key []byte, value []byte) error {
	return d.Set(key, value)
}

func (d DiskStorage) Delete(bytes []byte) error {
	return os.Remove(d.filePath(bytes))
}

func (d DiskStorage) DeleteSync(bytes []byte) error {
	return d.Delete(bytes)
}

func (d DiskStorage) Iterator(start, end []byte) (dbm.Iterator, error) {
	return d.iterator(start, end, false)
}

func (d DiskStorage) ReverseIterator(start, end []byte) (dbm.Iterator, error) {
	return d.iterator(start, end, true)
}

func (d DiskStorage) iterator(start, end []byte, reverse bool) (dbm.Iterator, error) {
	files, err := os.ReadDir(d.path)
	if err != nil {
		return nil, err
	}

	if reverse {
		sortFileNameDescend(files)
	}

	it := &diskStoreIterator{
		db:    d,
		start: start,
		end:   end,
		files: files,
	}

	for ; it.Valid(); it.Next() {
		if bytes.Compare(it.Key(), start) >= 0 {
			break
		}
	}

	return it, err
}

func (d DiskStorage) Close() error {
	return nil
}

func (d DiskStorage) NewBatch() dbm.Batch {
	return &diskStoreBatch{
		db: d,
	}
}

func (d DiskStorage) Print() error {
	return nil
}

func (d DiskStorage) Stats() map[string]string {
	return map[string]string{}
}

type diskStoreBatch struct {
	db  DiskStorage
	ops []*diskStorageOp
}

type diskStorageOp struct {
	key, value []byte
	delete     bool
}

func (d *diskStoreBatch) Set(key, value []byte) error {
	d.ops = append(d.ops, &diskStorageOp{key: key, value: value})
	return nil
}

func (d *diskStoreBatch) Delete(key []byte) error {
	d.ops = append(d.ops, &diskStorageOp{key: key, delete: true})
	return nil
}

func (d *diskStoreBatch) Write() error {
	for _, op := range d.ops {
		if op.delete {
			err := d.db.Delete(op.key)
			if err != nil {
				return err
			}
		} else {
			err := d.db.Set(op.key, op.value)
			if err != nil {
				return err
			}
		}
	}
	d.ops = nil
	return nil
}

func (d *diskStoreBatch) WriteSync() error {
	return d.Write()
}

func (d *diskStoreBatch) Close() error {
	d.ops = nil
	return nil
}

var _ dbm.DB = &DiskStorage{}

func sortFileNameDescend(files []os.DirEntry) {
	sort.Slice(files, func(i, j int) bool {
		return files[i].Name() > files[j].Name()
	})
}

type diskStoreIterator struct {
	db         DiskStorage
	start, end []byte
	files      []os.DirEntry
	err        error
}

func (d diskStoreIterator) Domain() (start []byte, end []byte) {
	return d.start, d.end
}

func (d diskStoreIterator) Valid() bool {
	return len(d.files) > 0 && bytes.Compare(d.Key(), d.end) < 0
}

func (d *diskStoreIterator) Next() {
	d.files = d.files[1:]
}

func (d diskStoreIterator) Key() (key []byte) {
	key, d.err = hex.DecodeString(d.files[0].Name())
	return key
}

func (d *diskStoreIterator) Value() (value []byte) {
	value, d.err = os.ReadFile(d.files[0].Name())
	return value
}

func (d diskStoreIterator) Error() error {
	return d.err
}

func (d diskStoreIterator) Close() error {
	d.files = nil
	return nil
}
