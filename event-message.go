package go_cqrs_firestore

import ycq "github.com/jetbasrawi/go.cqrs"

type EventMessage interface {
	ycq.EventMessage
	OrderingKey() string
}

func NewEventMessage(aggregateID string, event interface{}, version *int, orderingKey string) ycq.EventMessage {
	message := ycq.NewEventMessage(aggregateID, event, version)
	message.SetHeader("orderingKey", orderingKey)
	return message
}
