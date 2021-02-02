package shared

import (
	"context"
	ycq "github.com/jetbasrawi/go.cqrs"
)

type Repository interface {
	Load(id string) (ycq.AggregateRoot, error)
	Save(aggregate ycq.AggregateRoot, expectedVersion *int) error
	CommitChanges(ctx context.Context, transaction interface{}) error
	AcceptChanges()
}
