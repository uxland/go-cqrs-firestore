package shared

import (
	"context"
	ycq "github.com/jetbasrawi/go.cqrs"
	"reflect"
	"sync"
)

type GenericUnitOfWork interface {
	GetAggregateRepo(aggregateType reflect.Type) Repository
	Load(typ reflect.Type, aggregateID string) (ycq.AggregateRoot, error)
	Save(aggregate ycq.AggregateRoot, expectedVersion *int) error
	CommitAllChanges(ctx context.Context, transaction interface{}) error
	AcceptAllChanges()
}
type AggregateDefinition struct {
	Constructor func(id string) ycq.AggregateRoot
	Type        reflect.Type
}

type BaseGenericUnitOfWork struct {
	Repos map[string]Repository
	sync.Mutex
}

func NewBaseGenericUnitOfWork() *BaseGenericUnitOfWork {
	return &BaseGenericUnitOfWork{Repos: map[string]Repository{}, Mutex: sync.Mutex{}}
}

func (uow *BaseGenericUnitOfWork) GetAggregateRepo(aggregateType reflect.Type) Repository {
	typeString := aggregateType.String()
	return uow.Repos[typeString]
}

func (uow *BaseGenericUnitOfWork) Load(typ reflect.Type, aggregateID string) (ycq.AggregateRoot, error) {
	return uow.Repos[typ.String()].Load(aggregateID)
}

func (uow *BaseGenericUnitOfWork) Save(aggregate ycq.AggregateRoot, expectedVersion *int) error {
	aggregateType := reflect.TypeOf(aggregate).String()
	repo := uow.Repos[aggregateType]
	return repo.Save(aggregate, expectedVersion)
}

func (uow *BaseGenericUnitOfWork) CommitAllChanges(ctx context.Context, transaction interface{}) error {
	var err error = nil
	uow.Lock()
	for _, repository := range uow.Repos {
		err = repository.CommitChanges(ctx, transaction)
		if err != nil {
			break
		}
	}
	uow.Unlock()
	return err
}

func (uow *BaseGenericUnitOfWork) AcceptAllChanges() {
	for _, repository := range uow.Repos {
		repository.AcceptChanges()
	}
}
