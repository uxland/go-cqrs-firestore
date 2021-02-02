package datastore

import (
	"cloud.google.com/go/datastore"
	ycq "github.com/jetbasrawi/go.cqrs"
	"github.com/uxland/go-cqrs-firestore/shared"
)

type unitOfWork struct {
	*shared.BaseGenericUnitOfWork
}

func NewGenericUoW(aggregates []shared.AggregateDefinition, factory ycq.EventFactory, bus ycq.EventBus, client *datastore.Client) shared.GenericUnitOfWork {
	uow := shared.NewBaseGenericUnitOfWork()
	for _, aggregateDefinition := range aggregates {
		aggregateType := aggregateDefinition.Type.String()
		uow.Repos[aggregateType] = NewRepository(aggregateType, aggregateDefinition.Constructor, factory, bus, client)
	}
	return uow
}
