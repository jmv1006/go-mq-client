package consumer

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"

	"github.com/jmv1006/go-mq-client/utils"
)

type Consumer struct {
	Address  string
	Port     int
	Topic    string
	QuitChan chan int
}

func NewConsumer(address string, port int, topic string, quitChan chan int) *Consumer {
	return &Consumer{Address: address, Port: port, Topic: topic, QuitChan: quitChan}
}

func (c *Consumer) Consume() (chan Message, error) {
	rawAddr := fmt.Sprintf("%s:%d", c.Address, c.Port)

	addr, err := net.ResolveTCPAddr("tcp", rawAddr)

	if err != nil {
		return nil, err
	}

	conn, err := net.DialTCP("tcp", nil, addr)

	if err != nil {
		return nil, err
	}

	encodedRequest, err := c.encodeRequest()

	if err != nil {
		return nil, err
	}

	// Making Request
	_, err = conn.Write([]byte(encodedRequest))

	if err != nil {
		return nil, err
	}

	messagesChan := make(chan Message)

	go c.listen(conn, messagesChan)

	return messagesChan, nil
}

func (c *Consumer) listen(conn *net.TCPConn, messagesChan chan Message) {
	initialBuffer := make([]byte, 1000)

	for {
		select {
		case <-c.QuitChan:
			close(messagesChan)
		default:
			buff := bytes.NewBuffer(initialBuffer)

			written, err := conn.Read(buff.Bytes())

			if err != nil {
				if err != io.EOF {
					fmt.Println("read error:", err)
				}
				break
			} else if written == 1 {
				//  Bypass the heartbeat check
				continue
			}

			writtenBytes := buff.Bytes()[:written]
			msg, err := c.getMessageFromBytes(writtenBytes)

			if err == nil {
				messagesChan <- msg
			}
		}
	}
}

func (c *Consumer) getMessageFromBytes(msg []byte) (Message, error) {
	decoded, err := base64.StdEncoding.DecodeString(string(msg))

	if err != nil {
		return Message{}, err
	}

	var message Message

	err = json.Unmarshal(decoded, &message)

	if err != nil {
		return Message{}, err
	}

	return message, nil
}

func (c *Consumer) encodeRequest() (string, error) {
	consumerRequest := utils.Request{
		Type:  "CONSUME",
		Body:  "",
		Topic: c.Topic,
	}

	marshaled, err := json.Marshal(consumerRequest)

	if err != nil {
		return "", err
	}

	// encode marshaled body
	encoded := base64.StdEncoding.EncodeToString([]byte(marshaled))

	return encoded, nil
}
