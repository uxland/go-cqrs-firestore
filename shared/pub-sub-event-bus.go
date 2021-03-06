package shared

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	ycq "github.com/jetbasrawi/go.cqrs"
	"google.golang.org/api/option"
	"log"
	"reflect"
)

type pubsubBus struct {
	client      *pubsub.Client
	topic       string
	internalBus ycq.EventBus
}

func NewPubsubEventBus(projectID, topic string) ycq.EventBus {
	client, err := pubsub.NewClient(context.Background(), projectID, option.WithEndpoint("europe-west3-pubsub.googleapis.com:443"))
	if err != nil {
		panic(err)
	}
	log.Printf("creating pub sub event bus on project %s for topic %s\n", projectID, topic)
	return &pubsubBus{internalBus: ycq.NewInternalEventBus(), client: client, topic: topic}
}

type pubsubMessage struct {
	AggregateID   string                 `json:"aggregateID"`
	AggregateType string                 `json:"aggregateType"`
	Event         interface{}            `json:"event"`
	EventType     string                 `json:"eventType"`
	Headers       map[string]interface{} `json:"headers"`
	Version       int                    `json:"version"`
}

func toPubsubMessage(message ycq.EventMessage) (*pubsub.Message, error) {
	psMsg := &pubsubMessage{
		AggregateID:   message.AggregateID(),
		AggregateType: reflect.TypeOf(message.Event()).String(),
		Event:         message.Event(),
		EventType:     message.EventType(),
		Headers:       message.GetHeaders(),
		Version:       *message.Version(),
	}
	orderingKey := message.AggregateID()
	headers := message.GetHeaders()
	if headers != nil {
		if key := headers["orderingKey"]; key != nil {
			orderingKey = key.(string)
		}
	}
	buffer, err := json.Marshal(psMsg)
	if err != nil {
		return nil, err
	}
	return &pubsub.Message{
		Data:        buffer,
		OrderingKey: orderingKey,
	}, nil
}

func (p *pubsubBus) PublishEvent(message ycq.EventMessage) {
	p.internalBus.PublishEvent(message)
	psMsg, err := toPubsubMessage(message)
	if err != nil {
		return
	}
	log.Printf("publishing message to topic %s\n", p.topic)
	topic := p.client.Topic(p.topic)
	topic.EnableMessageOrdering = true
	publish := topic.Publish(context.Background(), psMsg)
	serverID, err := publish.Get(context.Background())
	if err != nil {
		log.Printf("error publishing event %s \n", err.Error())
	} else {
		log.Printf("msg with serverID: %s published\n", serverID)
	}
}

func (p *pubsubBus) AddHandler(handler ycq.EventHandler, i ...interface{}) {
	for _, event := range i {
		p.internalBus.AddHandler(handler, event)
	}
}
