package datastore

import (
	"cloud.google.com/go/datastore"
	"context"
	"fmt"
	"github.com/uxland/go-cqrs-firestore/shared"
	"google.golang.org/api/iterator"
)

type readDB struct {
	kind        string
	itemFactory func() interface{}
	client      *datastore.Client
}

func NewGenericDBImpl(kind string, itemFactory func() interface{}, client *datastore.Client) shared.GenericReadDB {
	return &readDB{kind, itemFactory, client}
}

func (db *readDB) SaveItem(tx interface{}, id string, item interface{}) error {
	transaction := tx.(*datastore.Transaction)
	key := datastore.NameKey(db.kind, id, nil)
	_, err := transaction.Put(key, item)
	return err
}

func (db *readDB) readIterator(it *datastore.Iterator) (interface{}, error) {
	docs := make([]interface{}, 0)
	for {
		item := db.itemFactory()
		_, err := it.Next(item)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		docs = append(docs, item)
	}
	return docs, nil
}

func (db *readDB) LoadAllItems() (interface{}, error) {
	query := datastore.NewQuery(db.kind).
		Limit(1000)
	it := db.client.Run(context.Background(), query)
	return db.readIterator(it)
}

func (db *readDB) LoadItem(id string) (interface{}, error) {
	key := datastore.NameKey(db.kind, id, nil)
	item := db.itemFactory()
	err := db.client.Get(context.Background(), key, item)
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (db *readDB) DeleteItem(tx interface{}, id string) error {
	transaction := tx.(*datastore.Transaction)
	key := datastore.NameKey(db.kind, id, nil)
	return transaction.Delete(key)
}

func (db *readDB) UpdateItem(transaction interface{}, id string, updates interface{}) error {
	panic("implement me")
}

func (db *readDB) ListItems(filter []shared.Filter, limit int) (interface{}, error) {
	query := datastore.NewQuery(db.kind)
	for _, s := range filter {
		query = query.Filter(fmt.Sprintf("%s %s", s.Path, s.Op), s.Value)
	}
	it := db.client.Run(context.Background(), query)
	return db.readIterator(it)
}
