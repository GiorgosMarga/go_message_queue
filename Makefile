queue:
	go run ./cmd/queue/main.go -port=":5000"

consumer:
	go run ./cmd/consumer/main.go -messagesToConsume=100 -delay=0 -numOfConsumers=2

publisher:
	go run ./cmd/publisher/main.go -messagesToSend=100 -delay=0 -withPriority=0 -numOfPublishers=10

test/10_000:
	go run ./cmd/queue/main.go &
	sleep 2
	go run ./cmd/publisher/main.go -messagesToSend=10_000 -delay=-1 -withPriority=0 -numOfPublishers= &
	go run ./cmd/consumer/main.go -messagesToConsume=1_000 -delay=-1 -numOfConsumers=10