package datastore

import (
	"cloud.google.com/go/datastore"
	ycq "github.com/jetbasrawi/go.cqrs"
	"github.com/uxland/go-cqrs-firestore/shared"
)

type unitOfWork struct {
	*shared.BaseGenericUnitOfWork
}

type UoWSettings struct {
	Aggregates []shared.AggregateDefinition
	Factory    ycq.EventFactory
	Bus        ycq.EventBus
	Client     *datastore.Client
	EventKind  string
}

func NewGenericUoW(settings UoWSettings) shared.GenericUnitOfWork {
	uow := shared.NewBaseGenericUnitOfWork()
	for _, aggregateDefinition := range settings.Aggregates {
		rs := repositorySettings{
			AggregateType:    aggregateDefinition.Type.String(),
			AggregateFactory: aggregateDefinition.Constructor,
			EventFactory:     settings.Factory,
			Bus:              settings.Bus,
			Client:           settings.Client,
			EventsKind:       settings.EventKind,
		}
		uow.Repos[rs.AggregateType] = NewRepository(rs)
	}
	return uow
}
