package main

import (
	"fmt"
	"log"
	"time"
)

func main()  {
	topic := &Topic{}

	subscription1 := topic.CreateSubscription()
	subscription1.AddConsumer(func(msg *Message) {
		log.Println("worker 1-1:", msg.Data)
		msg.Ack = true
	})
	subscription1.AddConsumer(func(msg *Message) {
		log.Println("worker 1-2:", msg.Data, "Nack")

	})
	subscription2 := topic.CreateSubscription()
	subscription2.AddConsumer(func(msg *Message) {
		log.Println("worker 2-1:", msg.Data)
		msg.Ack = true
	})

	for i := 0; i < 10; i++ {
		topic.Publish(Message {
			Data: fmt.Sprintf("Hello %d", i),
		})
	}

	time.Sleep(2 * time.Second)
}

func (t *Topic) CreateSubscription() *Subscription {
	sub := Subscription{}

	t.subscriptions = append(t.subscriptions, &sub)
	return &sub
}

func (t *Topic) Publish(msg Message) {
	for _, sub := range t.subscriptions {
		go sub.receiveFromTopic(msg)
	}
}

type Topic struct{
	subscriptions []*Subscription
}

type Subscription struct{
	index     uint8
	consumers []func(msg *Message)
}

func (sub *Subscription) receiveFromTopic(msg Message) {
	// TODO: Save to DB
	sub.index++
	if len(sub.consumers) == 0 {
		return
	}

	msg.Ack = false
	for !msg.Ack {
		sub.consumers[int(sub.index)%len(sub.consumers)](&msg)
		if !msg.Ack {
			time.Sleep(time.Second)
		}
	}

	// TODO: remove from db
}

func (sub *Subscription) AddConsumer(callback func(msg *Message)) {
	sub.consumers = append(sub.consumers, callback)
}

type Message struct{
	Ack bool
	Data string
}