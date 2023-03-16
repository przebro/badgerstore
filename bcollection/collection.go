package bcollection

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v3"
	"github.com/przebro/badgerstore/format"
	"github.com/przebro/databazaar/collection"
	"github.com/przebro/databazaar/result"
	"github.com/przebro/databazaar/selector"
)

type BadgerCollection struct {
	db *badger.DB
}

func NewCollection(db *badger.DB) *BadgerCollection {
	return &BadgerCollection{db: db}
}

func (m *BadgerCollection) Create(ctx context.Context, document interface{}) (*result.BazaarResult, error) {

	id, _, err := collection.RequiredFields(document)
	if err != nil {
		return nil, err
	}

	if id == "" {
		return nil, collection.ErrEmptyOrInvalidID
	}

	bf := new(bytes.Buffer)

	enc := gob.NewEncoder(bf)
	if err := enc.Encode(document); err != nil {
		return nil, err
	}

	err = m.db.Update(func(txn *badger.Txn) error {
		defer txn.Discard()
		if err := txn.Set([]byte(id), bf.Bytes()); err != nil {

			return err
		}
		txn.Commit()
		return nil

	})

	if err != nil {
		return nil, err
	}

	return &result.BazaarResult{ID: id}, nil
}
func (m *BadgerCollection) Get(ctx context.Context, id string, result interface{}) error {

	err := m.db.View(func(txn *badger.Txn) error {
		defer txn.Discard()
		item, err := txn.Get([]byte(id))
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			bf := bytes.NewBuffer(val)
			dec := gob.NewDecoder(bf)
			if err := dec.Decode(result); err != nil {
				return err
			}

			return nil
		})
	})

	return err
}
func (m *BadgerCollection) Update(ctx context.Context, doc interface{}) error {
	_, err := m.Create(ctx, doc)
	return err
}
func (m *BadgerCollection) Delete(ctx context.Context, id string) error {
	tx := m.db.NewTransaction(true)

	defer tx.Discard()
	if err := tx.Delete([]byte(id)); err != nil {
		return err
	}
	tx.Commit()
	return nil
}
func (m *BadgerCollection) CreateMany(ctx context.Context, docs []interface{}) ([]result.BazaarResult, error) {

	return nil, nil
}
func (m *BadgerCollection) BulkUpdate(ctx context.Context, docs []interface{}) error {
	err := m.db.Update(func(txn *badger.Txn) error {

		defer txn.Discard()

		for _, doc := range docs {
			id, _, err := collection.RequiredFields(doc)
			if err != nil {

				return err
			}

			if id == "" {
				return collection.ErrEmptyOrInvalidID
			}

			bf := new(bytes.Buffer)

			enc := gob.NewEncoder(bf)
			if err := enc.Encode(doc); err != nil {
				return err
			}

			if err := txn.Set([]byte(id), bf.Bytes()); err != nil {
				return err
			}
		}

		txn.Commit()
		return nil
	})

	return err

}
func (m *BadgerCollection) All(ctx context.Context) (collection.BazaarCursor, error) {

	crsr := newCursor(m.db, nil)

	return crsr, nil
}
func (m *BadgerCollection) Count(ctx context.Context) (int64, error) {

	return 0, nil
}
func (m *BadgerCollection) AsQuerable() (collection.QuerableCollection, error) {
	return m, nil
}
func (m *BadgerCollection) Select(ctx context.Context, s selector.Expr, fld selector.Fields) (collection.BazaarCursor, error) {

	ex, ok := s.(*selector.CmpExpr)
	if !ok {
		return nil, fmt.Errorf("unsupported selector type: %T", s)
	}

	prefix := []byte{}

	if ex.Op == "$prefix" {
		prefix = []byte(strings.Trim(ex.Ex.Expand(&format.PrefixFormatter{}), "\""))
	}

	crsr := newCursor(m.db, prefix)

	return crsr, nil
}

func (m *BadgerCollection) Type() string {
	return "badger"
}
