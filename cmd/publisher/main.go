package main

import (
	"log"
	"math/rand"

	"github.com/GiorgosMarga/ibmmq/internal/server"
)

func main() {
	publisher := server.NewPublisherServer(":8080")

	if err := publisher.CreateConn(); err != nil {
		log.Fatal(err)
	}

	priority := rand.Intn(100)
	if err := publisher.PublishMessage([]byte("Hello from publisher"), uint16(priority)); err != nil {
		log.Fatal(err)
	}

}

