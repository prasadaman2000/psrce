package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"time"
)

type DecodedMessage struct {
	Msg   []byte    `json:"Msg"`
	Time  time.Time `json:"Time"`
	Topic string    `json:"topic"`
}

func main() {
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Printf("Could not get hostname %s", err)
		return
	}

	psclient_address := flag.String("psclient_address", "0.0.0.0:8099", "--address defines the address of the pubsubclient server")
	host_alias := flag.String("host_alias", hostname, "--host_alias defines the alias for the host")
	flag.Parse()

	username := fmt.Sprintf("%sServer", *host_alias)
	incomingTopic := fmt.Sprintf("%s_Commands", *host_alias)
	outboundTopic := fmt.Sprintf("%s_Outputs", *host_alias)

	resp, err := http.Get(
		fmt.Sprintf("http://%s/clientConnect?username=%s&password=serverpass",
			*psclient_address, username))
	if err != nil {
		fmt.Printf("%s\n", err)
	} else {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("%s\n", body)
	}

	resp, err = http.Get(
		fmt.Sprintf("http://%s/subscribe?username=%s&password=serverpass&topic=%s",
			*psclient_address, username, incomingTopic))
	if err != nil {
		fmt.Printf("%s\n", err)
		return
	}

	body, err := io.ReadAll(resp.Body)
	fmt.Printf("%s\n", body)

	for {
		resp, err = http.Get(
			fmt.Sprintf("http://%s/poll?username=%s&password=serverpass&topic=%s",
				*psclient_address, username, incomingTopic))
		if err != nil {
			fmt.Printf("%s\n", err)
			return
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			fmt.Printf("%s\n", err)
			return
		}
		var messages []DecodedMessage
		err = json.Unmarshal(body, &messages)
		if err != nil {
			fmt.Printf("Unmarshal: %s, %s\n", err, body)
		}
		for _, message := range messages {
			out, err := os.OpenFile("b.out", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
			if err != nil {
				fmt.Printf("%s\n", err)
				return
			}
			if _, err := out.Write(message.Msg); err != nil {
				fmt.Printf("%s\n", err)
				break
			}
			out.Close()
			fmt.Println("executing")
			cmd := exec.Command("./b.out")
			stdout, err := cmd.Output()
			if err != nil {
				log.Fatalf("Command failed: %s", err)
			}
			fmt.Print(string(stdout))
			publishURL := fmt.Sprintf("http://%s/publish?username=%s&password=serverpass&topic=%s",
				*psclient_address, username, outboundTopic)
			resp, err = http.Post(publishURL, "application/octet-stream", bytes.NewReader(stdout))
			if err != nil {
				fmt.Printf("Publish error: %v\n", err)
				return
			}
			defer resp.Body.Close()

			_, err = io.ReadAll(resp.Body)
			if err != nil {
				fmt.Printf("Error reading publish response: %v\n", err)
				return
			}
		}
		time.Sleep(time.Second * 5)
	}
}
