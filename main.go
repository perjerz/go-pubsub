package main

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, "serene-circlet-276403")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	now := time.Now().Format(time.RFC3339)

	topic := client.Topic("perjerz-topic")
	go func() {
		for {
			time.Sleep(5 * time.Second)
			topic.Publish(ctx, &pubsub.Message{Data: []byte(now), Attributes: map[string]string{"hi": "hello"}})
		}
	}()

	cctx, cancel := context.WithCancel(ctx)

	go func() {
		stop := make(chan os.Signal)
		// os.Interrupt cross-platform
		signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

		<-stop

		fmt.Println("Are you sure to exit?")

		cancel()
	}()

	err = client.Subscription("perjerz-subscription").Receive(cctx, handleMessage)

	if err != nil {
		log.Fatal(err)
	}
}

type data struct {
	Event string `json:"event"`
}

func handleMessage(ctx context.Context, message *pubsub.Message) {
	defer message.Nack()
	//var d data
	//err := json.Unmarshal(message.Data, &d)
	//if err != nil {
	//	log.Println(err)
	//	return
	//}

	log.Println("receive", string(message.Data))
	message.Ack()
}