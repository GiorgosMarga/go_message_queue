package main

import (
	"flag"
	"fmt"

	"github.com/GiorgosMarga/ibmmq/internal/server"
)

func main() {

	var port string
	flag.StringVar(&port, "port", ":8080", "port")
	flag.Parse()
	s := server.NewQueueServer(port, server.ModePriority|server.ModeAckRequired|server.ModeFileBacked)
	if err := s.Start(); err != nil {
		fmt.Println(err)
	}
}
