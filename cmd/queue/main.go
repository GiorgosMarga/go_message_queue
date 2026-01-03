package main

import (
	"fmt"

	"github.com/GiorgosMarga/ibmmq/internal/server"
)

func main() {

	s := server.NewQueueServer(":8080", server.ModePriority|server.ModeAckRequired|server.ModeFileBacked)
	if err := s.Start(); err != nil {
		fmt.Println(err)
	}
}
