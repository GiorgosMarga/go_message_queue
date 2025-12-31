package main

import (
	"fmt"

	"github.com/GiorgosMarga/ibmmq/internal/server"
)

func main() {
	s := server.NewQueueServer(":8080", server.ModeAckRequired)

	if err := s.Start(); err != nil {
		fmt.Println(err)
	}
}
