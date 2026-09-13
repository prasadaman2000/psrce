package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"pubsubclientlib"
	"time"
)

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

	psclient, err := pubsubclientlib.NewPubSubClientInstance(username, "serverpass", *psclient_address)
	if err != nil {
		fmt.Printf("%s\n", err)
	}

	err = psclient.Subscribe(incomingTopic)
	if err != nil {
		fmt.Printf("%s\n", err)
		return
	}

	for {
		messages, err := psclient.Poll()
		if err != nil {
			fmt.Printf("%s\n", err)
			return
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
			err = psclient.Publish(outboundTopic, stdout)
			if err != nil {
				fmt.Printf("Publish error: %v\n", err)
				return
			}
		}
		time.Sleep(time.Second * 5)
	}
}
