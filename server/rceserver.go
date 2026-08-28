package main

import (
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
	psclient_address := flag.String("psclient_address", "0.0.0.0:8099", "--address defines the address of the pubsubclient server")
	resp, err := http.Get(
		fmt.Sprintf("http://%s/clientConnect?username=server&password=serverpass",
			*psclient_address))
	if err != nil {
		fmt.Printf("%s\n", err)
	} else {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("%s\n", body)
	}

	resp, err = http.Get(
		fmt.Sprintf("http://%s/subscribe?username=server&password=serverpass&topic=newRCEUser",
			*psclient_address))
	if err != nil {
		fmt.Printf("%s\n", err)
		return
	}
	body, err := io.ReadAll(resp.Body)
	fmt.Printf("%s\n", body)

	for {
		resp, err = http.Get(
			fmt.Sprintf("http://%s/poll?username=server&password=serverpass&topic=newRCEUser",
				*psclient_address))
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
			fmt.Printf("%s\n", err)
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
		}
		time.Sleep(time.Second * 5)
	}
}
