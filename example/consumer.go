package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
	"github.com/hongjunxin/jaegersarama"
)

func main() {
	_, closer, err := JaegerInit("consumer")
	if err != nil {
		panic((err))
	}
	defer closer.Close()

	consumer, err := sarama.NewConsumer([]string{"127.0.0.1:9092"}, sarama.NewConfig())
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	partitionConsumer, err := consumer.ConsumePartition("trace-kafka-topic", 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	consumed := 0
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			// must do Extract() from msg so consumer will report it's span
			ctx, _ := jaegersarama.Extract(context.Background(), msg)
			fmt.Printf("[consumer] span info: %v\n", GetSpanInfoFromContext(ctx))
			consumed++
		case <-signals:
			break
		}
	}
	log.Printf("Consumed: %d\n", consumed)
}
