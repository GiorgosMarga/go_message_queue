package main

import (
	"log"

	"github.com/GiorgosMarga/ibmmq/internal/server"
)

func main() {
	publisher := server.NewPublisherServer(":8080")

	if err := publisher.CreateConn(); err != nil {
		log.Fatal(err)
	}

	if err := publisher.PublishMessage([]byte("Hello from publisher")); err != nil {
		log.Fatal(err)
	}

}
