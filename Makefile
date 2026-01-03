queue:
	go run ./cmd/queue/main.go

consumer:
	go run ./cmd/consumer/main.go -messagesToConsume=1 -delay=-1 -numOfConsumers=1

publisher:
	go run ./cmd/publisher/main.go -messagesToSend=10 -delay=-1 -withPriority=0 -numOfPublishers=10