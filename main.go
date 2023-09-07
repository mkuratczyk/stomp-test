package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/go-stomp/stomp/v3"
)

const defaultPort = ":61613"

var serverAddr = flag.String("server", "localhost:61613", "STOMP server endpoint")
var messageCount = flag.Int("count", 10, "Number of messages to send/receive")
var queueName = flag.String("queue", "/queue/stomp_test", "Destination queue")
var publishOnly = flag.Bool("publishOnly", false, "If true, only publish messages, don't subscribe")
var consumeOnly = flag.Bool("consumeOnly", false, "If true, only consume messages, don't publish")
var helpFlag = flag.Bool("help", false, "Print help text")
var stop = make(chan bool)

// these are the default options that work with RabbitMQ
var options []func(*stomp.Conn) error = []func(*stomp.Conn) error{
	stomp.ConnOpt.Login("guest", "guest"),
	stomp.ConnOpt.Host("/"),
}

func main() {
	flag.Parse()
	if *helpFlag {
		fmt.Fprintf(os.Stderr, "Usage of %s\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	if !*publishOnly {
		fmt.Fprintf(os.Stderr, "Subscribing..\n")
		subscribed := make(chan bool)
		go recvMessages(subscribed)

		// wait until we know the receiver has subscribed
		<-subscribed
	}

	if !*consumeOnly {
		go sendMessages()
	}

	if !*publishOnly && !*consumeOnly {
		<-stop
	}
	<-stop
}

func sendMessages() {
	defer func() {
		stop <- true
	}()

	conn, err := stomp.Dial("tcp", *serverAddr, options...)
	if err != nil {
		println("cannot connect to server", err.Error())
		return
	}

	for i := 1; i <= *messageCount; i++ {
		text := fmt.Sprintf("Message #%d", i)
		err = conn.Send(*queueName, "text/plain",
			[]byte(text), nil)
		if err != nil {
			println("failed to send to server", err)
			return
		}
	}
	time.Sleep(1 * time.Second)
	println("sender finished")
}

func recvMessages(subscribed chan bool) {
	defer func() {
		stop <- true
	}()

	conn, err := stomp.Dial("tcp", *serverAddr, options...)

	if err != nil {
		println("cannot connect to server", err.Error())
		return
	}
	println("Connected...")

	sub, err := conn.Subscribe(*queueName, stomp.AckAuto)
	if err != nil {
		println("cannot subscribe to", *queueName, err.Error())
		return
	}
	println("Subscribed...")
	close(subscribed)

	for i := 1; i <= *messageCount; i++ {
		msg := <-sub.C
		expectedText := fmt.Sprintf("Message #%d", i)
		actualText := string(msg.Body)
		if expectedText != actualText {
			println("Expected:", expectedText)
			println("Actual:", actualText)
		}
	}
	println("receiver finished")

}
