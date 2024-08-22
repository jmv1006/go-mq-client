package utils

type Request struct {
	Type  string `json:"type"`
	Body  string `json:"body"`
	Topic string `json:"topic"`
}
