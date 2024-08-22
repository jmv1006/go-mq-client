package consumer

type Message struct {
	Timestamp string `json:"timestamp"`
	Payload   string `json:"payload"`
}
