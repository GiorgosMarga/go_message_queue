package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/GiorgosMarga/ibmmq/internal/server"
)

func main() {
	var (
		messagesToSend  int
		delay           int
		numOfPublishers int
		withPriority    int
	)

	flag.IntVar(&messagesToSend, "messagesToSend", 1, "messages to send per publisher")
	flag.IntVar(&delay, "delay", 0, "delay in seconds between message. Set to -1 for random.")
	flag.IntVar(&numOfPublishers, "numOfPublishers", 1, "Total publishers. Each publisher sends 'messageToSend' messages.")
	flag.IntVar(&withPriority, "withPriority", 0, "Is set to 1, publishers send messages with random priorities. If set to 0, messages have no priority.")

	flag.Parse()

	wg := &sync.WaitGroup{}
	for i := range numOfPublishers {
		wg.Add(1)
		go func() {
			publisher := server.NewPublisherServer(":5000")

			if err := publisher.CreateConn(); err != nil {
				log.Fatal(err)
			}
			var priority uint16
			if withPriority == 1 {
				priority = uint16(rand.Intn(100))
			}

			for j := range messagesToSend {
				if err := publisher.PublishMessage([]byte("Hello from publisher"), uint16(priority)); err != nil {
					log.Fatal(err)
				}
				fmt.Printf("[%d]: Sent %d/%d\n", i, j+1, messagesToSend)
				var del int
				if delay == -1 {
					del = rand.Intn(10)
				} else {
					del = delay
				}
				time.Sleep(time.Duration(del) * time.Second)
			}
			fmt.Printf("[%d]: Finished\n", i)
			wg.Done()
		}()
	}

	wg.Wait()
}
