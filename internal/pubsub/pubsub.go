package pubsub

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/pubsub"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/daadaamed/go-cqrs-pubsub/internal/event"
)

// Client holds the Pub/Sub client and the resolved topic.
type Client struct {
	client *pubsub.Client
	topic  *pubsub.Topic
}

// Config controls bootstrap. PushEndpoint is the HTTP URL the emulator/Pub/Sub
// will POST events to (the projection handler).
type Config struct {
	ProjectID    string
	TopicID      string
	Subscription string
	PushEndpoint string
}

// New creates the client and ensures the topic and push subscription exist.
// Bootstrap is idempotent: "already exists" is treated as success.
func New(ctx context.Context, cfg Config) (*Client, error) {
	cl, err := pubsub.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("pubsub client: %w", err)
	}

	topic, err := ensureTopic(ctx, cl, cfg.TopicID)
	if err != nil {
		cl.Close()
		return nil, err
	}

	if err := ensurePushSubscription(ctx, cl, topic, cfg.Subscription, cfg.PushEndpoint); err != nil {
		cl.Close()
		return nil, err
	}

	return &Client{client: cl, topic: topic}, nil
}

// Close flushes and releases resources.
func (c *Client) Close() {
	c.topic.Stop()
	_ = c.client.Close()
}

// Publish sends one event to the topic. Blocks until the broker acks,
// surfacing publish errors to the caller.
func (c *Client) Publish(ctx context.Context, e *event.Event) error {
	msg := &pubsub.Message{
		Data: e.Payload,
		Attributes: map[string]string{
			"type":         string(e.Type),
			"aggregate_id": e.AggregateID.String(),
		},
	}
	res := c.topic.Publish(ctx, msg)
	if _, err := res.Get(ctx); err != nil {
		return fmt.Errorf("publish event: %w", err)
	}
	return nil
}

func ensureTopic(ctx context.Context, cl *pubsub.Client, id string) (*pubsub.Topic, error) {
	t, err := cl.CreateTopic(ctx, id)
	if err == nil {
		return t, nil
	}
	if status.Code(err) == codes.AlreadyExists {
		return cl.Topic(id), nil
	}
	return nil, fmt.Errorf("ensure topic %q: %w", id, err)
}

func ensurePushSubscription(ctx context.Context, cl *pubsub.Client, topic *pubsub.Topic, subID, pushEndpoint string) error {
	_, err := cl.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{
		Topic:      topic,
		PushConfig: pubsub.PushConfig{Endpoint: pushEndpoint},
	})
	if err == nil {
		return nil
	}
	if status.Code(err) == codes.AlreadyExists {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return err
	}
	return fmt.Errorf("ensure subscription %q: %w", subID, err)
}
