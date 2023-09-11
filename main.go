package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/go-stomp/stomp/v3"
)

var serverAddr = flag.String("server", "localhost:61613", "STOMP server endpoint")
var messageCount = flag.Int("count", 10, "Number of messages to send/receive")
var pubisherCount = flag.Int("publisherCount", 1, "Number of publishers")
var consumerCount = flag.Int("consumerCount", 1, "Number of consumers")
var interval = flag.Int("interval", 0, "Interval between messages (ms)")
var queueName = flag.String("queue", "/queue/stomp_test", "Destination queue")
var publishOnly = flag.Bool("publishOnly", false, "If true, only publish messages, don't subscribe")
var separateQueues = flag.Bool("separateQueues", false, "If true, each publisher uses a separate queue")
var consumeOnly = flag.Bool("consumeOnly", false, "If true, only consume messages, don't publish")
var timestampBody = flag.Bool("timestampBody", false, "If true, message body is perf-test compatible")
var helpFlag = flag.Bool("help", false, "Print help text")

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

	var wg sync.WaitGroup

	if !*publishOnly {
		for i := 1; i <= *consumerCount; i++ {
			subscribed := make(chan bool)
			n := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				recvMessages(subscribed, n)
			}()

			// wait until we know the receiver has subscribed
			<-subscribed
		}
	}

	if !*consumeOnly {
		for i := 1; i <= *pubisherCount; i++ {
			n := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				sendMessages(n)
			}()
		}
	}

	wg.Wait()
}

func sendMessages(n int) {
	// sleep random interval to avoid all senders connecting at the same time
	s := rand.Intn(n)
	time.Sleep(time.Duration(s) * time.Millisecond)

	conn, err := stomp.Dial("tcp", *serverAddr, options...)
	if err != nil {
		println("cannot connect to server", err.Error())
		return
	}

	var queue string
	if *separateQueues {
		queue = fmt.Sprintf("%s-%d", *queueName, n)
	} else {
		queue = *queueName
	}
	for i := 1; i <= *messageCount; i++ {
		var text string
		if *timestampBody {
			b := make([]byte, 12)
			binary.BigEndian.PutUint32(b[0:], uint32(1234))
			binary.BigEndian.PutUint64(b[4:], uint64(time.Now().UnixMilli()))
			err = conn.Send(queue, "", b, nil)
		} else {
			text = fmt.Sprintf("Message #%d", i)
			err = conn.Send(queue, "text/plain", []byte(text), nil)
		}
		if err != nil {
			println("failed to send to server", err)
			return
		}
		time.Sleep(time.Duration(*interval) * time.Millisecond)
	}
	time.Sleep(1 * time.Second)
	println("sender finished")
}

func recvMessages(subscribed chan bool, n int) {
	conn, err := stomp.Dial("tcp", *serverAddr, options...)

	if err != nil {
		println("cannot connect to server", err.Error())
		return
	}
	println("Connected...")

	var queue string
	if *separateQueues {
		queue = fmt.Sprintf("%s-%d", *queueName, n)
	} else {
		queue = *queueName
	}
	sub, err := conn.Subscribe(queue, stomp.AckAuto)
	if err != nil {
		println("cannot subscribe to", queue, err.Error())
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
