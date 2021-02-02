package datastore

import (
	"cloud.google.com/go/datastore"
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	ycq "github.com/jetbasrawi/go.cqrs"
	"github.com/uxland/go-cqrs-firestore/shared"
	"google.golang.org/api/iterator"
)

type repo struct {
	aggregateType string
	constructor   func(id string) ycq.AggregateRoot
	cache         map[string]ycq.AggregateRoot
	toSave        map[string]*int
	eventFactory  ycq.EventFactory
	bus           ycq.EventBus
	client        *datastore.Client
}

const eventsKind = "event"

func NewRepository(aggregateType string, constructor func(id string) ycq.AggregateRoot, factory ycq.EventFactory, bus ycq.EventBus, client *datastore.Client) shared.Repository {
	return &repo{
		aggregateType: aggregateType,
		constructor:   constructor,
		cache:         make(map[string]ycq.AggregateRoot),
		toSave:        make(map[string]*int),
		eventFactory:  factory,
		bus:           bus,
		client:        client,
	}
}

func (r *repo) Load(id string) (ycq.AggregateRoot, error) {
	panic("implement me")
}

func (r *repo) Save(aggregate ycq.AggregateRoot, expectedVersion *int) error {
	r.toSave[aggregate.AggregateID()] = expectedVersion
	return nil
}

func (r *repo) CommitChanges(ctx context.Context, tx interface{}) error {
	transaction := tx.(*datastore.Transaction)
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

type eventDocument struct {
	AggregateID   string                 `datastore:"aggregateID"`
	AggregateType string                 `datastore:"aggregateType"`
	Event         interface{}            `datastore:"event"`
	Headers       map[string]interface{} `datastore:"headers"`
	Version       *int                   `datastore:"version"`
	EventType     string                 `datastore:"version"`
}

func (r *repo) loadEvents(ctx context.Context, id string) ([]ycq.EventMessage, error) {
	query := datastore.NewQuery(eventsKind).
		Filter("aggregateID=", id).
		Filter("aggregateType=", r.aggregateType).
		Order("version")
	it := r.client.Run(ctx, query)
	result := make([]ycq.EventMessage, 0)

	for {
		doc := &eventDocument{}
		_, err := it.Next(doc)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		bytes, err := json.Marshal(doc.Event)
		if err != nil {
			return nil, err
		}
		event := r.eventFactory.GetEvent(doc.EventType)
		err = json.Unmarshal(bytes, event)
		if err != nil {
			return nil, err
		}
		result = append(result, ycq.NewEventMessage(id, event, doc.Version))
	}
	return result, nil

}

func (r *repo) assertExpectedVersion(ctx context.Context, aggregateID string, expectedVersion int) error {
	if expectedVersion == -1 {
		return nil
	}
	persisted, err := r.loadEvents(ctx, aggregateID)
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
func (r *repo) save(transaction *datastore.Transaction, ctx context.Context, aggregate ycq.AggregateRoot, expectedVersion *int) error {
	err := r.assertExpectedVersion(ctx, aggregate.AggregateID(), *expectedVersion)
	if err != nil {
		return err
	}
	for _, message := range aggregate.GetChanges() {

		id := uuid.New().String()
		props := &eventDocument{
			AggregateID:   message.AggregateID(),
			AggregateType: r.aggregateType,
			Event:         message.Event(),
			EventType:     message.EventType(),
			Headers:       message.GetHeaders(),
			Version:       message.Version(),
		}
		key := datastore.NameKey(eventsKind, id, nil)
		_, err = transaction.Put(key, props)
		if err != nil {
			return err
		}
		r.bus.PublishEvent(message)
	}
	return nil
}
