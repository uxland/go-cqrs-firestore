package shared

import "time"

type LeasingService interface {
	GetLease(resourceID string, duration time.Duration) (Lease, error)
}
type Lease interface {
	Lock() (bool, error)
	Release() (bool, error)
}
