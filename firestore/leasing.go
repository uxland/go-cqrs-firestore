package firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"github.com/uxland/go-cqrs-firestore/shared"
	"time"
)

type service struct {
	client *firestore.Client
}

const leasingCollectionName = "leasing"

func NewService(client *firestore.Client) shared.LeasingService {
	return &service{client: client}
}

func (srvc *service) GetLease(resourceID string, duration time.Duration) (shared.Lease, error) {
	ctx := context.Background()
	var lease *firestoreLease
	err := srvc.client.RunTransaction(ctx, func(ctx context.Context, transaction *firestore.Transaction) error {
		var err error
		docRef := srvc.client.Collection(leasingCollectionName).Doc(resourceID)
		snapshot, _ := transaction.Get(docRef)

		if !snapshot.Exists() {
			lease = newLease(srvc, resourceID, duration)
		} else {
			persisted := &firestoreLease{}
			err := snapshot.DataTo(persisted)
			if err != nil {
				return err
			}
			if persisted.canLease() {
				lease = newLease(srvc, resourceID, duration)
			}
		}
		if lease != nil {
			err = transaction.Set(docRef, lease)
		}
		return err
	})
	if lease == nil {
		return nil, nil
	}
	return lease, err
}

func (srvc *service) lock(id string) error {
	docRef := srvc.client.Collection(leasingCollectionName).Doc(id)
	_, err := docRef.Update(context.Background(), []firestore.Update{
		{
			Path:  "locked",
			Value: true,
		},
	})
	return err
}

func (srvc *service) delete(id string) error {
	docRef := srvc.client.Collection(leasingCollectionName).Doc(id)
	_, err := docRef.Delete(context.Background())
	return err
}

type firestoreLease struct {
	srvc *service

	id      string
	Expires time.Time `firestore:"expires"`
	Locked  bool      `firestore:"locked"`
}

func newLease(srvc *service, ID string, duration time.Duration) *firestoreLease {
	return &firestoreLease{srvc: srvc,
		id:      ID,
		Locked:  false,
		Expires: time.Now().Add(duration),
	}
}

func (l firestoreLease) Lock() (bool, error) {
	l.Locked = true
	err := l.srvc.lock(l.id)
	return err != nil, err
}

func (l firestoreLease) Release() (bool, error) {
	err := l.srvc.delete(l.id)
	return err != nil, err
}

func (l *firestoreLease) expired() bool {
	return l.Expires.Before(time.Now())
}
func (l *firestoreLease) canLease() bool {
	return !l.Locked && l.expired()
}
