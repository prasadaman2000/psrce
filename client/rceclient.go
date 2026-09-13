package main

import (
	"flag"
	"fmt"
	"os"
	"pubsubclientlib"
	"time"
)

func main() {
	psclient_address := flag.String("psclient_address", "0.0.0.0:8099",
		"--address defines the address of the pubsubclient server")
	executable_path := flag.String("executable_path", "a.out",
		"--executable_path defines the executable path to transfer")
	remote_alias := flag.String("remote_alias", "local",
		"--remote_alias defines the host to send command to")

	flag.Parse()

	psclient, err := pubsubclientlib.NewPubSubClientInstance("client", "clientpass", *psclient_address)
	if err != nil {
		fmt.Printf("Could not create client: %s", err)
		return
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
	err = psclient.Subscribe(subscribeTopic)
	if err != nil {
		fmt.Printf("Subscribe error: %v\n", err)
		return
	}

	publishTopic := fmt.Sprintf("%s_Commands", *remote_alias)
	err = psclient.Publish(publishTopic, fileBytes)
	if err != nil {
		fmt.Printf("Publish error: %v\n", err)
		return
	}

	time.Sleep(5 * time.Second)
	messages, err := psclient.Poll()
	if err != nil {
		fmt.Printf("%s\n", err)
		return
	}
	for _, message := range messages {
		fmt.Printf("%s\n", message.Msg)
	}
}
