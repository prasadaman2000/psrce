package main

import (
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
	resp, err := http.Get(
		fmt.Sprintf("http://%s/clientConnect?username=client&password=clientpass", *psclient_address))
	if err != nil {
		fmt.Printf("%s\n", err)
		return
	} else {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("%s\n", body)
	}

	file, err := os.Open("a.out")
	if err != nil {
		fmt.Printf("%s\n", err)
		return
	}

	resp, err = http.Post(
		fmt.Sprintf("http://%s/publish?username=client&password=clientpass&topic=newRCEUser",
			*psclient_address), "application/octet-stream", file)
	if err != nil {
		fmt.Printf("%s\n", err)
		return
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("%s\n", err)
		return
	}
	fmt.Printf("%s\n", body)
}
