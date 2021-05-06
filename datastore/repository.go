package datastore

import (
	"cloud.google.com/go/datastore"
	"context"
	"errors"
	"github.com/google/uuid"
	ycq "github.com/jetbasrawi/go.cqrs"
	"github.com/uxland/go-cqrs-firestore/shared"
	"google.golang.org/api/iterator"
	"reflect"
)

type repo struct {
	cache  map[string]ycq.AggregateRoot
	toSave map[string]*int
	repositorySettings
}

type repositorySettings struct {
	AggregateType    string
	AggregateFactory func(id string) ycq.AggregateRoot
	EventFactory     ycq.EventFactory
	Bus              ycq.EventBus
	Client           *datastore.Client
	EventsKind       string
	Namespace        string
}

func NewRepository(settings repositorySettings) shared.Repository {
	const defaultEventsKind = "event"
	if reflect.ValueOf(settings).FieldByName("EventsKind").IsZero() {
		settings.EventsKind = defaultEventsKind
	}
	return &repo{
		cache:              make(map[string]ycq.AggregateRoot),
		toSave:             make(map[string]*int),
		repositorySettings: settings,
	}
}

func (r *repo) Load(id string) (ycq.AggregateRoot, error) {
	var aggregate ycq.AggregateRoot
	if aggregate = r.cache[id]; aggregate != nil {
		return aggregate, nil
	}
	aggregate = r.AggregateFactory(id)
	ctx := context.Background()
	messages, err := r.loadEvents(ctx, aggregate.AggregateID())
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
	r.cache = make(map[string]ycq.AggregateRoot)
	r.toSave = make(map[string]*int)
}

type eventDocument struct {
	AggregateID   string      `datastore:"aggregateID"`
	AggregateType string      `datastore:"aggregateType"`
	Event         interface{} `datastore:"event,noindex"`
	Version       int         `datastore:"version"`
	EventType     string      `datastore:"eventType"`
}

func (e *eventDocument) Load(properties []datastore.Property) error {
	return datastore.LoadStruct(e, properties)
}

func (e *eventDocument) Save() ([]datastore.Property, error) {
	pls, ok := e.Event.(datastore.PropertyLoadSaver)
	if !ok {
		return datastore.SaveStruct(e)
	}
	properties, err := pls.Save()
	if err != nil {
		return nil, err
	}
	type tmp struct {
		AggregateID   string `datastore:"aggregateID"`
		AggregateType string `datastore:"aggregateType"`
		Version       int    `datastore:"version"`
		EventType     string `datastore:"eventType"`
	}
	aux := &tmp{
		AggregateID:   e.AggregateID,
		AggregateType: e.AggregateType,
		Version:       e.Version,
		EventType:     e.EventType,
	}
	p, err := datastore.SaveStruct(aux)
	p = append(p, datastore.Property{Name: "Event", NoIndex: true, Value: &datastore.Entity{Properties: properties}})
	return p, nil
}

func (r *repo) loadEvents(ctx context.Context, id string) ([]ycq.EventMessage, error) {
	query := r.createQuery().
		Filter("aggregateID=", id).
		Filter("aggregateType=", r.AggregateType).
		Order("version")
	it := r.Client.Run(ctx, query)
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
		event := r.EventFactory.GetEvent(doc.EventType)
		entity := doc.Event.(*datastore.Entity)

		if pls, ok := event.(datastore.PropertyLoadSaver); ok {
			err = pls.Load(entity.Properties)
		} else {
			err = datastore.LoadStruct(event, entity.Properties)
		}

		if err != nil {
			return nil, err
		}
		msg := ycq.NewEventMessage(id, event, &doc.Version)
		result = append(result, msg)
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
			AggregateType: r.AggregateType,
			Event:         message.Event(),
			EventType:     message.EventType(),
			Version:       *message.Version(),
		}

		key := newKey(r.Namespace, r.EventsKind, id, nil)
		_, err = transaction.Put(key, props)
		if err != nil {
			return err
		}
		r.Bus.PublishEvent(message)
	}
	return nil
}

func (r *repo) createQuery() *datastore.Query {
	query := datastore.NewQuery(r.EventsKind)
	if r.Namespace != "" {
		query = query.Namespace(r.Namespace)
	}
	return query
}
