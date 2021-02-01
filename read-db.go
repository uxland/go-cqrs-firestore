package go_cqrs_firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"google.golang.org/api/iterator"
)

type Filter struct {
	Path  string
	Op    string
	Value interface{}
}

type GenericReadDB interface {
	SaveItem(transaction *firestore.Transaction, id string, item interface{}) error
	LoadAllItems() (interface{}, error)
	LoadItem(id string) (interface{}, error)
	DeleteItem(transaction *firestore.Transaction, id string) error
	UpdateItem(transaction *firestore.Transaction, id string, updates []firestore.Update) error
	ListItems(filter []Filter, limit int) (interface{}, error)
}
type genericDBImpl struct {
	collectionName string
	itemFactory    func() interface{}
	client         *firestore.Client
}

func NewGenericDBImpl(collectionName string, itemFactory func() interface{}, client *firestore.Client) GenericReadDB {
	return &genericDBImpl{collectionName: collectionName, itemFactory: itemFactory, client: client}
}

func (g *genericDBImpl) ListItems(filter []Filter, limit int) (interface{}, error) {
	col := g.client.Collection(g.collectionName)
	if limit == 0 {
		limit = 100
	}
	q := col.Query
	for _, f := range filter {
		q = q.Where(f.Path, f.Op, f.Value)
	}
	q = q.Limit(limit)
	documentIterator := q.Documents(context.Background())
	return g.readIterator(documentIterator)
}

func (g *genericDBImpl) DeleteItem(transaction *firestore.Transaction, id string) error {
	collection := g.client.Collection(g.collectionName)
	docRef := collection.Doc(id)
	return transaction.Delete(docRef)
}

func (g *genericDBImpl) SaveItem(transaction *firestore.Transaction, id string, item interface{}) error {
	collection := g.client.Collection(g.collectionName)
	docRef := collection.Doc(id)
	return transaction.Set(docRef, item)
}

func (g *genericDBImpl) UpdateItem(transaction *firestore.Transaction, id string, updates []firestore.Update) error {
	collection := g.client.Collection(g.collectionName)
	docRef := collection.Doc(id)
	return transaction.Update(docRef, updates)
}

func (g *genericDBImpl) readIterator(documentIterator *firestore.DocumentIterator) (interface{}, error) {
	docs := make([]interface{}, 0)
	for {
		doc, err := documentIterator.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		item := g.itemFactory()
		err = doc.DataTo(item)
		if err != nil {
			return nil, err
		}
		docs = append(docs, item)
	}
	return docs, nil
}

func (g *genericDBImpl) LoadAllItems() (interface{}, error) {
	collection := g.client.Collection(g.collectionName)
	documentIterator := collection.Limit(1000).Documents(context.Background())
	return g.readIterator(documentIterator)
}

func (g *genericDBImpl) LoadItem(id string) (interface{}, error) {
	collection := g.client.Collection(g.collectionName)
	docRef := collection.Doc(id)
	snapshot, err := docRef.Get(context.Background())
	if err != nil || !snapshot.Exists() {
		return nil, nil
	}
	item := g.itemFactory()
	err = snapshot.DataTo(item)
	return item, err
}
