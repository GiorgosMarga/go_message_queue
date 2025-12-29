package main

import (
	"fmt"
	"log"

	"github.com/GiorgosMarga/ibmmq/internal/server"
)

func main() {
	publisher := server.NewConsumerServer(":8080")

	if err := publisher.CreateConn(); err != nil {
		log.Fatal(err)
	}

	m, err := publisher.ConsumeMessage()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%+v\n", m)
}
