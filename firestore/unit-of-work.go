package firestore

import (
	"cloud.google.com/go/firestore"
	ycq "github.com/jetbasrawi/go.cqrs"
	"github.com/uxland/go-cqrs-firestore/shared"
)

type unitOfWork struct {
	*shared.BaseGenericUnitOfWork
}

type UoWSettings struct {
	Aggregates           []shared.AggregateDefinition
	Factory              ycq.EventFactory
	Bus                  ycq.EventBus
	Client               *firestore.Client
	EventsCollectionName string
}

func NewGenericUoW(settings UoWSettings) shared.GenericUnitOfWork {
	uow := shared.NewBaseGenericUnitOfWork()
	for _, aggregateDefinition := range settings.Aggregates {
		rs := repositorySettings{
			AggregateType:        aggregateDefinition.Type.String(),
			AggregateFactory:     aggregateDefinition.Constructor,
			EventFactory:         settings.Factory,
			Bus:                  settings.Bus,
			Client:               settings.Client,
			EventsCollectionName: settings.EventsCollectionName,
		}
		uow.Repos[rs.AggregateType] = NewRepository(rs)
	}
	return uow
}
