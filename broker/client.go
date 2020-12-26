package broker

import (
	"bufio"
	"encoding/csv"
	"log"
	"math/rand"
	"strings"
	"time"
)

// Client type
type Client struct {
	ID     *rand.Rand
	Writer *bufio.Writer
	Reader *bufio.Reader
	Disc   *chan bool
}

func (c *Client) read(separator rune) []string {

	line, err := c.Reader.ReadString(byte(separator))
	if err != nil {
		log.Printf("%d %s client read error", c.ID, err.Error())
	}
	// not ideal, but this is the best I could find
	input := csv.NewReader(strings.NewReader(line))
	input.Comma = ' '
	fields, err := input.Read()
	return fields
}

func (c *Client) writeByte(data []byte) {
	c.Writer.Write(data)
	c.Writer.Write([]byte("\n"))
	c.Writer.Flush()
}

func (c *Client) writeString(data string) {
	c.Writer.WriteString(data)
	c.Writer.WriteString("\n")
	c.Writer.Flush()
}

// NewClient factory method
func NewClient(writer *bufio.Writer, reader *bufio.Reader) *Client {
	closeChannel := make(chan bool)
	id := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &Client{
		id,
		writer,
		reader,
		&closeChannel,
	}
}
