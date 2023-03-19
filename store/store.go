package store

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"

	"github.com/dgraph-io/badger/v3"

	"github.com/przebro/badgerstore/bcollection"
	"github.com/przebro/databazaar/collection"
	"github.com/przebro/databazaar/store"
)

var storeName = "internal"

type badgerStore struct {
	basePath    string
	collections map[string]*badger.DB
}

func init() {
	store.RegisterStoreFactory(storeName, initInternalstore)
}
func initInternalstore(opt store.ConnectionOptions) (store.DataStore, error) {

	if opt.Path == "" {
		return nil, errors.New("empty path")
	}

	dir, err := os.Stat(opt.Path)
	if err != nil || !dir.IsDir() {
		return nil, errors.New("invalid path")
	}

	return &badgerStore{basePath: opt.Path, collections: make(map[string]*badger.DB)}, nil
}

func (store *badgerStore) CreateCollection(ctx context.Context, name string) (collection.DataCollection, error) {

	if ok, _ := regexp.Match(`^[A-Za-z][\d\w]{0,31}$`, []byte(name)); !ok {
		return nil, errors.New("invalid collection name")
	}

	pth := filepath.Join(store.basePath, name)

	if err := os.Mkdir(pth, os.ModePerm); err != nil {
		return nil, err
	}

	options := badger.DefaultOptions(pth)
	db, err := badger.Open(options)
	if err != nil {
		return nil, err
	}

	store.collections[name] = db

	return bcollection.NewCollection(db), nil
}

func (store *badgerStore) Collection(ctx context.Context, name string) (collection.DataCollection, error) {

	db, ok := store.collections[name]
	if !ok {

		pth := filepath.Join(store.basePath, name)
		fs, err := os.Stat(pth)
		if err != nil || !fs.IsDir() {
			return nil, errors.New("collection not found")
		}

		options := badger.DefaultOptions(pth)
		db, err = badger.Open(options)

		if err != nil {
			return nil, err
		}

		store.collections[name] = db

	}

	return bcollection.NewCollection(db), nil
}

func (store *badgerStore) Status(context.Context) (string, error) {

	return "", nil
}

func (store *badgerStore) Close(ctx context.Context) {
	for _, db := range store.collections {
		db.Close()
	}
}

func (store *badgerStore) CollectionExists(ctx context.Context, name string) bool {

	path := filepath.Join(store.basePath, name)
	if _, err := os.Stat(path); err != nil {
		return false
	}

	f, err := os.Open(path)
	if err != nil {
		return false
	}

	defer f.Close()

	_, err = f.Readdirnames(1)
	return err == io.EOF

}
