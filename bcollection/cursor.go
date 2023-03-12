package bcollection

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"reflect"

	"github.com/dgraph-io/badger/v3"
)

type badgerCursor struct {
	crsr        *badger.Iterator
	tx          *badger.Txn
	prefix      []byte
	beforeFirst bool
}

func newCursor(db *badger.DB, prefix []byte) *badgerCursor {

	txn := db.NewTransaction(false)
	opts := badger.DefaultIteratorOptions

	opts.PrefetchSize = 10

	it := txn.NewIterator(opts)

	if prefix != nil {
		it.Seek(prefix)
	} else {
		it.Rewind()
	}

	return &badgerCursor{crsr: it, prefix: prefix, tx: txn, beforeFirst: true}

}

func (c *badgerCursor) All(ctx context.Context, v interface{}) error {

	rval := reflect.ValueOf(v)
	if rval.Kind() != reflect.Ptr {
		return fmt.Errorf("not a pointer")
	}

	sval := rval.Elem()
	if sval.Kind() == reflect.Interface {
		sval = sval.Elem()
	}

	if sval.Kind() != reflect.Slice {
		return fmt.Errorf("not a slice")
	}

	etype := sval.Type().Elem()

	if c.prefix != nil {
		for c.crsr.ValidForPrefix(c.prefix) {
			newElem := reflect.New(etype)
			ife := newElem.Interface()

			if err := c.Decode(ife); err != nil {
				return err
			}
			sval.Set(reflect.Append(sval, newElem.Elem()))
			c.crsr.Next()
		}
	} else {

		for c.crsr.Valid() {

			newElem := reflect.New(etype)
			ife := newElem.Interface()

			if err := c.Decode(ife); err != nil {
				return err
			}
			sval.Set(reflect.Append(sval, newElem.Elem()))
			c.crsr.Next()
		}
	}

	return nil
}
func (c *badgerCursor) Next(ctx context.Context) bool {

	isValid := false

	if c.beforeFirst {
		c.beforeFirst = false
		if c.prefix != nil {
			isValid = c.crsr.ValidForPrefix(c.prefix)
		} else {
			isValid = c.crsr.Valid()
		}

		return isValid
	}

	c.crsr.Next()

	if c.prefix != nil {
		isValid = c.crsr.ValidForPrefix(c.prefix)
	} else {
		isValid = c.crsr.Valid()
	}

	return isValid
}
func (c *badgerCursor) Decode(v interface{}) error {

	itm := c.crsr.Item()
	sz := itm.ValueSize()
	buf := make([]byte, sz)
	buf, err := itm.ValueCopy(buf)
	if err != nil {
		return err
	}

	rdr := bytes.NewReader(buf)

	dec := gob.NewDecoder(rdr)
	return dec.Decode(v)
}
func (c *badgerCursor) Close() error {
	c.crsr.Close()
	return nil
}
