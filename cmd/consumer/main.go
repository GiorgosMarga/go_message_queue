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
		messagesToConsume int
		delay             int
		numOfConsumers    int
	)

	flag.IntVar(&messagesToConsume, "messagesToConsume", 1, "messages to consume per consumer")
	flag.IntVar(&delay, "delay", 0, "delay in seconds to ack. Set to -1 for random.")
	flag.IntVar(&numOfConsumers, "numOfConsumers", 2, "Total consumers. Each consumer consumes 'messagesToConsume' messages.")

	flag.Parse()
	wg := &sync.WaitGroup{}
	for i := range numOfConsumers {
		wg.Go(func() {
			publisher := server.NewConsumerServer(":5000")

			if err := publisher.CreateConn(); err != nil {
				log.Fatal(err)
			}

			for j := range messagesToConsume {
				_, err := publisher.ConsumeMessage()
				if err != nil {
					fmt.Println(err)
				}
				fmt.Printf("[%d]: consumed %d/%d\n", i, j+1, messagesToConsume)

				if delay == -1 {
					time.Sleep(time.Duration(rand.Intn(100)) * time.Second)
				} else {
					time.Sleep(time.Duration(delay) * time.Second)
				}
			}
		})
	}

	wg.Wait()
}
