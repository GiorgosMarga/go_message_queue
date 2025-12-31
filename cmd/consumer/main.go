package main

import (
	"fmt"
	"log"
	"sync"

	"github.com/GiorgosMarga/ibmmq/internal/server"
)

func main() {
	wg := &sync.WaitGroup{}
	for range 1 {
		wg.Add(1)
		go func() {
			publisher := server.NewConsumerServer(":8080")

			if err := publisher.CreateConn(); err != nil {
				log.Fatal(err)
			}

			m, err := publisher.ConsumeMessage()
			if err != nil {
				fmt.Println(err)
			}
			fmt.Printf("%+v\n", m)
			wg.Done()
		}()
	}
	wg.Wait()

}
