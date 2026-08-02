package main

import (
	"context"
	"fmt"
	"log"
	"time"

	rabbitjetstream "github.com/chennqqi/rabbit-jetstream-go"
	"github.com/nats-io/nats.go"
)

func main() {
	client, err := rabbitjetstream.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	publisher, err := client.Publisher("orders", "queue-plan-revision", 9)
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ack, err := publisher.Publish(ctx, rabbitjetstream.PublishMessage{
		ID: "order-42", Priority: 9, Data: []byte(`{"order_id":42}`),
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("confirmed stream=%s sequence=%d duplicate=%t\n", ack.Stream, ack.Sequence, ack.Duplicate)
}
