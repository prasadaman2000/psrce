package pubsubclientlib

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type PubSubClientInstance struct {
	username         string
	password         string
	psclient_address string
}

type DecodedMessage struct {
	Msg   []byte    `json:"Msg"`
	Time  time.Time `json:"Time"`
	Topic string    `json:"topic"`
}

func NewPubSubClientInstance(
	username string,
	password string,
	psclient_address string,
) (*PubSubClientInstance, error) {
	resp, err := http.Get(
		fmt.Sprintf("http://%s/clientConnect?username=client&password=clientpass", psclient_address))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Status code: %d", resp.StatusCode)
	}
	return &PubSubClientInstance{
		username,
		password,
		psclient_address,
	}, nil
}

func (psci *PubSubClientInstance) Subscribe(topic string) error {
	subscribeUrl := fmt.Sprintf("http://%s/subscribe?username=%s&password=%s&topic=%s",
		psci.username, psci.password, psci.psclient_address, topic)
	resp, err := http.Get(subscribeUrl)
	if err != nil {
		fmt.Printf("Subscribe error: %v\n", err)
		return err
	}
	resp.Body.Close()
	return nil
}

func (psci *PubSubClientInstance) Publish(topic string, payload []byte) error {
	publishURL := fmt.Sprintf("http://%s/publish?username=%s&password=%s&topic=%s",
		psci.username, psci.password, psci.psclient_address, topic)
	resp, err := http.Post(publishURL, "application/octet-stream", bytes.NewReader(payload))
	if err != nil {
		fmt.Printf("Publish error: %v\n", err)
		return err
	}
	defer resp.Body.Close()
	return nil
}

func (psci *PubSubClientInstance) Poll() ([]DecodedMessage, error) {
	resp, err := http.Get(
		fmt.Sprintf("http://%s/poll?username=%s&password=%s",
			psci.username, psci.password, psci.psclient_address))
	if err != nil {
		return nil, err
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("%s\n", err)
		return nil, err
	}
	var messages []DecodedMessage
	err = json.Unmarshal(body, &messages)
	if err != nil {
		fmt.Printf("Unmarshal error: %s, %s\n", err, body)
		return nil, err
	}

	return messages, nil
}
