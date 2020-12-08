package go_cqrs_firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	ycq "github.com/jetbasrawi/go.cqrs"
	"reflect"
)

type GenericUnitOfWork interface {
	GetAggregateRepo(aggregateType reflect.Type) Repository
	Load(typ reflect.Type, aggregateID string) (ycq.AggregateRoot, error)
	Save(aggregate ycq.AggregateRoot, expectedVersion *int) error
	CommitAllChanges(ctx context.Context, transaction *firestore.Transaction) error
	AcceptAllChanges()
}
type AggregateDefinition struct {
	Constructor func(id string) ycq.AggregateRoot
	Type        reflect.Type
}
type unitOfWorkImpl struct {
	repos map[string]Repository
}

func (uow *unitOfWorkImpl) GetAggregateRepo(aggregateType reflect.Type) Repository {
	typeString := aggregateType.String()
	return uow.repos[typeString]
}

func NewUnitOfWork(aggregates []AggregateDefinition, factory ycq.EventFactory, bus ycq.EventBus, client *firestore.Client) GenericUnitOfWork {
	uow := &unitOfWorkImpl{repos: map[string]Repository{}}
	for _, aggregateDefinition := range aggregates {
		aggregateType := aggregateDefinition.Type.String()
		uow.repos[aggregateType] = NewRepository(aggregateType, aggregateDefinition.Constructor, factory, bus, client)
	}
	return uow
}

func (uow *unitOfWorkImpl) Load(typ reflect.Type, aggregateID string) (ycq.AggregateRoot, error) {
	return uow.repos[typ.String()].Load(aggregateID)
}

func (uow *unitOfWorkImpl) Save(aggregate ycq.AggregateRoot, expectedVersion *int) error {
	aggregateType := reflect.TypeOf(aggregate).String()
	repo := uow.repos[aggregateType]
	return repo.Save(aggregate, expectedVersion)
}
func (uow *unitOfWorkImpl) AcceptAllChanges() {
	for _, repository := range uow.repos {
		repository.AcceptChanges()
	}
}

func (uow *unitOfWorkImpl) CommitAllChanges(ctx context.Context, transaction *firestore.Transaction) error {
	var err error = nil
	for _, repository := range uow.repos {
		err = repository.CommitChanges(ctx, transaction)
		if err != nil {
			break
		}
	}
	return err
}
