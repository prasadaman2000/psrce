package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

type DecodedMessage struct {
	Msg   []byte    `json:"Msg"`
	Time  time.Time `json:"Time"`
	Topic string    `json:"topic"`
}

func main() {
	psclient_address := flag.String("psclient_address", "0.0.0.0:8099",
		"--address defines the address of the pubsubclient server")
	executable_path := flag.String("executable_path", "a.out",
		"--executable_path defines the executable path to transfer")
	remote_alias := flag.String("remote_alias", "local",
		"--remote_alias defines the host to send command to")

	flag.Parse()

	resp, err := http.Get(
		fmt.Sprintf("http://%s/clientConnect?username=client&password=clientpass", *psclient_address))
	if err != nil {
		fmt.Printf("%s\n", err)
		return
	} else {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("%s\n", body)
	}

	fileBytes, err := os.ReadFile(*executable_path)
	if err != nil {
		fmt.Printf("File error: %v\n", err)
		return
	}

	fmt.Printf("Publishing binary payload size: %d bytes\n", len(fileBytes))
	if len(fileBytes) == 0 {
		fmt.Println("Error: a.out is 0 bytes on disk!")
		return
	}

	subscribeTopic := fmt.Sprintf("%s_Outputs", *remote_alias)
	subscribeUrl := fmt.Sprintf("http://%s/subscribe?username=client&password=clientpass&topic=%s", *psclient_address, subscribeTopic)
	resp, err = http.Get(subscribeUrl)
	if err != nil {
		fmt.Printf("Subscribe error: %v\n", err)
		return
	}
	defer resp.Body.Close()

	publishTopic := fmt.Sprintf("%s_Commands", *remote_alias)
	publishURL := fmt.Sprintf("http://%s/publish?username=client&password=clientpass&topic=%s", *psclient_address, publishTopic)
	resp, err = http.Post(publishURL, "application/octet-stream", bytes.NewReader(fileBytes))
	if err != nil {
		fmt.Printf("Publish error: %v\n", err)
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("Error reading publish response: %v\n", err)
		return
	}
	fmt.Printf("%s\n", body)

	time.Sleep(5 * time.Second)
	resp, err = http.Get(
		fmt.Sprintf("http://%s/poll?username=client&password=clientpass&topic=%s",
			*psclient_address, subscribeTopic))
	if err != nil {
		fmt.Printf("%s\n", err)
		return
	}
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("%s\n", err)
		return
	}
	var messages []DecodedMessage
	err = json.Unmarshal(body, &messages)
	if err != nil {
		fmt.Printf("Unmarshal error: %s, %s\n", err, body)
		return
	}
	for _, message := range messages {
		fmt.Printf("%s\n", message.Msg)
	}
}
