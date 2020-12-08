package go_cqrs_firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"errors"
	"github.com/google/uuid"
	ycq "github.com/jetbasrawi/go.cqrs"
	"google.golang.org/api/iterator"
)

const collectionName = "events"

type Repository interface {
	Load(id string) (ycq.AggregateRoot, error)
	Save(aggregate ycq.AggregateRoot, expectedVersion *int) error
	CommitChanges(ctx context.Context, transaction *firestore.Transaction) error
	AcceptChanges()
}

type impl struct {
	aggregateType string
	constructor   func(id string) ycq.AggregateRoot
	cache         map[string]ycq.AggregateRoot
	toSave        map[string]*int
	eventFactory  ycq.EventFactory
	bus           ycq.EventBus
	client        *firestore.Client
}

func NewRepository(aggregateType string, constructor func(id string) ycq.AggregateRoot, factory ycq.EventFactory, bus ycq.EventBus, client *firestore.Client) Repository {
	return &impl{
		aggregateType: aggregateType,
		constructor:   constructor,
		cache:         make(map[string]ycq.AggregateRoot),
		toSave:        make(map[string]*int),
		eventFactory:  factory,
		bus:           bus,
		client:        client,
	}
}

func (repo *impl) Load(id string) (ycq.AggregateRoot, error) {
	var aggregate ycq.AggregateRoot
	if aggregate = repo.cache[id]; aggregate != nil {
		return aggregate, nil
	}
	aggregate = repo.constructor(id)
	ctx := context.Background()
	collection := repo.client.Collection(collectionName)
	messages, err := repo.loadEvents(collection, ctx, aggregate.AggregateID())
	if err != nil {
		return nil, err
	}
	for _, message := range messages {
		aggregate.Apply(message, false)
		aggregate.IncrementVersion()
	}
	repo.cache[id] = aggregate
	return aggregate, nil
}

type eventDocument struct {
	AggregateID   string            `firestore:"aggregateID"`
	AggregateType string            `firestore:"aggregateType"`
	Event         interface{}       `firestore:"event"`
	Headers       map[string]string `firestore:"headers"`
	Version       *int              `firestore:"version"`
}

func (repo *impl) loadEvents(collection *firestore.CollectionRef, ctx context.Context, id string) ([]ycq.EventMessage, error) {
	iter := collection.
		Where("aggregateID", "==", id).
		Where("aggregateType", "==", repo.aggregateType).
		OrderBy("version", firestore.Asc).
		Documents(ctx)
	result := make([]ycq.EventMessage, 0)
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			return result, nil
		}
		if err != nil {
			return nil, err
		}
		eventType, err := doc.DataAt("eventType")
		if err != nil {
			return nil, err
		}
		event := repo.eventFactory.GetEvent(eventType.(string))
		message := &eventDocument{Event: event}
		err = doc.DataTo(message)

		if err != nil {
			return nil, err
		}
		result = append(result, ycq.NewEventMessage(id, message.Event, message.Version))
	}
}

func (repo *impl) assertExpectedVersion(collection *firestore.CollectionRef, ctx context.Context, aggregateID string, expectedVersion int) error {
	if expectedVersion == -1 {
		return nil
	}
	persisted, err := repo.loadEvents(collection, ctx, aggregateID)
	if err != nil {
		return err
	}
	if len(persisted) == 0 {
		return nil
	}
	if last := persisted[len(persisted)-1]; *last.Version() != expectedVersion {
		return errors.New("concurrence exception")
	}
	return nil

}

func (repo *impl) save(transaction *firestore.Transaction, ctx context.Context, aggregate ycq.AggregateRoot, expectedVersion *int) error {
	collection := repo.client.Collection(collectionName)

	err := repo.assertExpectedVersion(collection, ctx, aggregate.AggregateID(), *expectedVersion)
	if err != nil {
		return err
	}
	for _, message := range aggregate.GetChanges() {

		id := uuid.New().String()
		docRef := collection.Doc(id)
		props := map[string]interface{}{
			"aggregateID":   message.AggregateID(),
			"aggregateType": repo.aggregateType,
			"event":         message.Event(),
			"eventType":     message.EventType(),
			"headers":       message.GetHeaders(),
			"version":       *message.Version(),
		}
		err = transaction.Set(docRef, props)
		if err != nil {
			return err
		}
		repo.bus.PublishEvent(message)
	}
	return nil
}

func (repo *impl) Save(aggregate ycq.AggregateRoot, expectedVersion *int) error {
	repo.toSave[aggregate.AggregateID()] = expectedVersion
	return nil
}

func (repo *impl) CommitChanges(ctx context.Context, transaction *firestore.Transaction) error {
	for id, expectedVersion := range repo.toSave {
		aggregate := repo.cache[id]
		if aggregate == nil {
			continue
		}
		err := repo.save(transaction, ctx, aggregate, expectedVersion)
		if err != nil {
			return err
		}
	}
	return nil
}
func (repo *impl) AcceptChanges() {
	for id := range repo.toSave {
		aggregate := repo.cache[id]
		if aggregate == nil {
			continue
		}
		aggregate.ClearChanges()
	}
	repo.toSave = make(map[string]*int)
}
