package firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"github.com/uxland/go-cqrs-firestore/shared"
	"google.golang.org/api/iterator"
)

type genericDBImpl struct {
	collectionName string
	itemFactory    func() interface{}
	client         *firestore.Client
}

func NewGenericDBImpl(collectionName string, itemFactory func() interface{}, client *firestore.Client) shared.GenericReadDB {
	return &genericDBImpl{collectionName: collectionName, itemFactory: itemFactory, client: client}
}

func (g *genericDBImpl) SaveItem(transaction interface{}, id string, item interface{}) error {
	tx := transaction.(*firestore.Transaction)
	collection := g.client.Collection(g.collectionName)
	docRef := collection.Doc(id)
	return tx.Set(docRef, item)
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

func (g *genericDBImpl) DeleteItem(transaction interface{}, id string) error {
	tx := transaction.(*firestore.Transaction)
	collection := g.client.Collection(g.collectionName)
	docRef := collection.Doc(id)
	return tx.Delete(docRef)
}

func (g *genericDBImpl) UpdateItem(transaction interface{}, id string, updates interface{}) error {
	tx := transaction.(*firestore.Transaction)
	collection := g.client.Collection(g.collectionName)
	docRef := collection.Doc(id)
	return tx.Update(docRef, updates.([]firestore.Update))
}

func (g *genericDBImpl) ListItems(filter []shared.Filter, limit int) (interface{}, error) {
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
