package main

import (
	"bytes"
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

	fileBytes, err := os.ReadFile("a.out")
	if err != nil {
		fmt.Printf("File error: %v\n", err)
		return
	}

	fmt.Printf("Publishing binary payload size: %d bytes\n", len(fileBytes))
	if len(fileBytes) == 0 {
		fmt.Println("Error: a.out is 0 bytes on disk!")
		return
	}

	publishURL := fmt.Sprintf("http://%s/publish?username=client&password=clientpass&topic=newRCEUser", *psclient_address)
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
}
