package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Shopify/sarama"
	"github.com/hongjunxin/jaegersarama"
	"github.com/opentracing/opentracing-go"
)

var (
	producer                          sarama.AsyncProducer
	client                            sarama.Client
	producerSuccesses, producerErrors int
)

func init() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewRandomPartitioner

	// config.Net.SASL.Enable = true
	// config.Net.SASL.User = "kafkaclient1"
	// config.Net.SASL.Password = "kafkaclient1pwd"

	var err error

	broker := []string{"127.0.0.1:9092"}
	client, err = sarama.NewClient(broker, config)
	if err != nil {
		log.Panic("can't init  NewClient for %s, error:%s", broker, err)
	}
	producer, err = sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		log.Panic("can't init  NewClient for %s, error:%s", broker, err)
	}
}

func main() {
	tracer, closer, err := JaegerInit("producer")
	if err != nil {
		panic(err)
	}
	defer closer.Close()
	span := tracer.StartSpan("main")
	defer span.Finish()
	ctx := opentracing.ContextWithSpan(context.Background(), span)

	msg := &sarama.ProducerMessage{Topic: "trace-kafka-topic", Value: sarama.StringEncoder("testing")}
	// must do Inject() into msg so producer will report it's span
	ctx, _ = jaegersarama.Inject(ctx, msg)
	fmt.Printf("[producer] span info: %v\n", GetSpanInfoFromContext(ctx))
	producer.Input() <- msg

	select {
	case <-producer.Successes():
		producerSuccesses++
	case e := <-producer.Errors():
		fmt.Printf("kafka produce msg failed, err='%v'\n", e.Err)
		producerErrors++
	case <-time.After(3000 * time.Millisecond):
		fmt.Println("kafka produce msg failed, timeout.")
	}
	fmt.Printf("[main] produce msg, success=%v, error=%v\n", producerSuccesses, producerErrors)
	defer client.Close()
	defer producer.AsyncClose()
}
