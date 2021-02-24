package datastore

import (
	"cloud.google.com/go/datastore"
	"context"
	"github.com/uxland/go-cqrs-firestore/shared"
	"time"
)

const leasingKind = "leasing"

type datastoreLease struct {
	srvc *service

	id      string
	Expires time.Time `datastore:"expires"`
	Locked  bool      `datastore:"locked"`
}

type service struct {
	client *datastore.Client
}

func NewLeasingService(client *datastore.Client) shared.LeasingService {
	return &service{client: client}
}

func (s *service) lock(lease *datastoreLease) error {
	lease.Locked = true
	key := newKey("", leasingKind, lease.id, nil)
	_, err := s.client.Put(context.Background(), key, lease)
	return err
}

func (s *service) delete(id string) error {
	key := newKey("", leasingKind, id, nil)
	return s.client.Delete(context.Background(), key)
}

func (s *service) GetLease(resourceID string, duration time.Duration) (shared.Lease, error) {
	ctx := context.Background()
	var lease *datastoreLease
	_, err := s.client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		persisted := &datastoreLease{}
		key := newKey("", leasingKind, resourceID, nil)
		err := s.client.Get(ctx, key, persisted)
		if err != nil && err != datastore.ErrNoSuchEntity {
			return err
		}
		if err == datastore.ErrNoSuchEntity || persisted.canLease() {
			lease = newLease(s, resourceID, duration)
			err = nil
		}
		if lease != nil {
			_, err = tx.Put(key, lease)
		}
		return err
	})
	if err != nil || lease == nil {
		return nil, err
	}
	return lease, nil

}

func newLease(srvc *service, ID string, duration time.Duration) *datastoreLease {
	return &datastoreLease{
		srvc:    srvc,
		id:      ID,
		Locked:  false,
		Expires: time.Now().Add(duration),
	}
}

func (l *datastoreLease) Lock() (bool, error) {
	l.Locked = true
	err := l.srvc.lock(l)
	return err != nil, err
}

func (l *datastoreLease) Release() (bool, error) {
	err := l.srvc.delete(l.id)
	return err != nil, err
}

func (l *datastoreLease) expired() bool {
	return l.Expires.Before(time.Now())
}
func (l *datastoreLease) canLease() bool {
	return !l.Locked && l.expired()
}
