package go_cqrs_firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"log"
	"testing"
	"time"
)

func TestLeasingService(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Leasing service Suite")
}

var _ = Describe("Leasing service test suite", func() {
	var sut LeasingService
	var resourceID = "my-resource"
	var leaseDuration = time.Minute * 5
	var lease Lease
	var err error
	var client *firestore.Client
	const projectID = ""
	JustBeforeEach(func() {
		ctx := context.Background()
		var err error
		client, err = firestore.NewClient(ctx, projectID)
		if err != nil {
			log.Panic(err)
		}
		sut = NewService(client)
	})

	AfterEach(func() {
		collection := client.Collection(leasingCollectionName)
		iterator := collection.Limit(100).Documents(context.Background())
		for {
			next, err := iterator.Next()
			if err != nil {
				break
			}
			_, _ = next.Ref.Delete(context.Background())
		}
	})

	Describe("Given a resource id", func() {
		Describe("Given resource is not leased in system", func() {
			Describe("When getting a lease for the resource id", func() {
				JustBeforeEach(func() {
					lease, err = sut.GetLease(resourceID, leaseDuration)
				})
				It("should get the lease", func() {
					Expect(lease).NotTo(BeZero())
					Expect(err).To(BeZero())
				})
				It("should create the document in db", func() {
					docRef := client.Collection(leasingCollectionName).Doc(resourceID)
					snapshot, _ := docRef.Get(context.Background())
					Expect(snapshot.Exists()).To(BeTrue())

				})
			})
		})
		Describe("Given the resource is leased", func() {
			JustBeforeEach(func() {
				lease, err = sut.GetLease(resourceID, leaseDuration)
			})
			Describe("When trying to get a lease for the resource", func() {
				It("Shouldn't return any lease", func() {
					newLease, newErr := sut.GetLease(resourceID, leaseDuration)
					Expect(newLease).To(BeNil())
					Expect(newErr).To(BeNil())
				})
			})
			Describe("When releasing the lease", func() {
				JustBeforeEach(func() {
					_, err = lease.Release()
				})
				It("Should remove the lease from DB", func() {
					docRef := client.Collection(leasingCollectionName).Doc(resourceID)
					snapshot, _ := docRef.Get(context.Background())
					Expect(snapshot.Exists()).To(BeFalse())
				})
				It("should allow to retrieve a new lease for the resource", func() {
					newLease, newErr := sut.GetLease(resourceID, leaseDuration)
					Expect(newLease).ToNot(BeNil())
					Expect(newErr).To(BeNil())
				})
			})
			Describe("When locking the release", func() {
				It("should not retrieve any lease even the lease is expired", func() {
					expiredLease, _ := sut.GetLease("another-resource", time.Minute*-5)
					_, _ = expiredLease.Lock()
					otherLease, _ := sut.GetLease("another-resource", 10*time.Minute)
					Expect(otherLease).To(BeNil())
				})
			})
		})
		Describe("Given the resource is leased but expired", func() {
			It("Should allow to retrieve a new lease", func() {
				expireResourceID := "expired-resource"
				_, _ = sut.GetLease(expireResourceID, -5*time.Minute)
				newLease, newErr := sut.GetLease(expireResourceID, time.Minute*5)
				Expect(newLease).NotTo(BeNil())
				Expect(newErr).To(BeNil())
			})
		})
		//Describe("Given the ")
	})
})
