package producer

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"

	"github.com/jmv1006/go-mq-client/utils"
)

type Producer struct {
	Address string
	Port    int
	Topic   string
}

func NewProducer(address string, port int, topic string) *Producer {
	return &Producer{Address: address, Port: port, Topic: topic}
}

func (p *Producer) ProduceMessage(msg string) error {
	rawAddr := fmt.Sprintf("%s:%d", p.Address, p.Port)

	addr, err := net.ResolveTCPAddr("tcp", rawAddr)

	if err != nil {
		return err
	}

	conn, err := net.DialTCP("tcp", nil, addr)

	if err != nil {
		return err
	}

	requestBytes, err := p.getRequestBytes(msg)

	if err != nil {
		return err
	}

	_, err = conn.Write(requestBytes)

	return err
}

func (p *Producer) getRequestBytes(msg string) ([]byte, error) {
	producerRequest := utils.Request{Type: "PRODUCE", Body: msg, Topic: p.Topic}

	marshaledMsg, err := json.Marshal(producerRequest)

	if err != nil {
		return []byte{}, err
	}

	encoded := base64.StdEncoding.EncodeToString(marshaledMsg)

	return []byte(encoded), nil
}
