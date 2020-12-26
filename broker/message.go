package broker

import (
	"encoding/json"
	"time"
)

type Message struct {
	Header    string
	Body      string
	Key       string
	size      int
	timestamp int64
}

func (m *Message) encodeMessage() []byte {
	bytes, _ := json.Marshal(m)
	return bytes
}

// NewMessage creates a new message
func NewMessage(content string) Message {
	var message Message
	bytemessage := []byte(content)
	json.Unmarshal(bytemessage, &message)
	message.size = len(message.Body)
	message.timestamp = time.Now().UnixNano()
	return message
}
