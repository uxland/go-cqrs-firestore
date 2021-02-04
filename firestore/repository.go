package firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"errors"
	"github.com/google/uuid"
	ycq "github.com/jetbasrawi/go.cqrs"
	"github.com/uxland/go-cqrs-firestore/shared"
	"google.golang.org/api/iterator"
)

//const collectionName = "events"

type eventDocument struct {
	AggregateID   string            `firestore:"aggregateID"`
	AggregateType string            `firestore:"aggregateType"`
	Event         interface{}       `firestore:"event"`
	Headers       map[string]string `firestore:"headers"`
	Version       *int              `firestore:"version"`
}

type repositorySettings struct {
	AggregateType        string
	AggregateFactory     func(id string) ycq.AggregateRoot
	EventFactory         ycq.EventFactory
	Bus                  ycq.EventBus
	Client               *firestore.Client
	EventsCollectionName string
}

type repo struct {
	cache  map[string]ycq.AggregateRoot
	toSave map[string]*int
	repositorySettings
}

const defaultCollectionName = "events"

func NewRepository(settings repositorySettings) shared.Repository {

	if settings.EventsCollectionName == "" {
		settings.EventsCollectionName = defaultCollectionName
	}
	return &repo{
		cache:              make(map[string]ycq.AggregateRoot),
		toSave:             make(map[string]*int),
		repositorySettings: settings,
	}
}

func (r *repo) loadEvents(collection *firestore.CollectionRef, ctx context.Context, id string) ([]ycq.EventMessage, error) {
	iter := collection.
		Where("aggregateID", "==", id).
		Where("aggregateType", "==", r.AggregateType).
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
		event := r.EventFactory.GetEvent(eventType.(string))
		message := &eventDocument{Event: event}
		err = doc.DataTo(message)

		if err != nil {
			return nil, err
		}
		result = append(result, ycq.NewEventMessage(id, message.Event, message.Version))
	}
}
func (r *repo) assertExpectedVersion(collection *firestore.CollectionRef, ctx context.Context, aggregateID string, expectedVersion int) error {
	if expectedVersion == -1 {
		return nil
	}
	persisted, err := r.loadEvents(collection, ctx, aggregateID)
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
func (r *repo) Load(id string) (ycq.AggregateRoot, error) {
	var aggregate ycq.AggregateRoot
	if aggregate = r.cache[id]; aggregate != nil {
		return aggregate, nil
	}
	aggregate = r.AggregateFactory(id)
	ctx := context.Background()
	collection := r.Client.Collection(r.EventsCollectionName)
	messages, err := r.loadEvents(collection, ctx, aggregate.AggregateID())
	if err != nil {
		return nil, err
	}
	for _, message := range messages {
		aggregate.Apply(message, false)
		aggregate.IncrementVersion()
	}
	r.cache[id] = aggregate
	return aggregate, nil
}

func (r *repo) save(transaction *firestore.Transaction, ctx context.Context, aggregate ycq.AggregateRoot, expectedVersion *int) error {
	collection := r.Client.Collection(r.EventsCollectionName)

	err := r.assertExpectedVersion(collection, ctx, aggregate.AggregateID(), *expectedVersion)
	if err != nil {
		return err
	}
	for _, message := range aggregate.GetChanges() {

		id := uuid.New().String()
		docRef := collection.Doc(id)
		props := map[string]interface{}{
			"aggregateID":   message.AggregateID(),
			"aggregateType": r.AggregateType,
			"event":         message.Event(),
			"eventType":     message.EventType(),
			"headers":       message.GetHeaders(),
			"version":       *message.Version(),
		}
		err = transaction.Set(docRef, props)
		if err != nil {
			return err
		}
		r.Bus.PublishEvent(message)
	}
	return nil
}
func (r *repo) Save(aggregate ycq.AggregateRoot, expectedVersion *int) error {
	r.toSave[aggregate.AggregateID()] = expectedVersion
	return nil
}

func (r *repo) CommitChanges(ctx context.Context, tx interface{}) error {
	transaction := tx.(*firestore.Transaction)
	for id, expectedVersion := range r.toSave {
		aggregate := r.cache[id]
		if aggregate == nil {
			continue
		}
		err := r.save(transaction, ctx, aggregate, expectedVersion)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *repo) AcceptChanges() {
	for id := range r.toSave {
		aggregate := r.cache[id]
		if aggregate == nil {
			continue
		}
		aggregate.ClearChanges()
	}
	r.toSave = make(map[string]*int)
}
