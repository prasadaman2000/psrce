package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"pubsubclientlib"
	"rcelib"
	"time"
)

func main() {
	psclient_address := flag.String("psclient_address", "0.0.0.0:8099",
		"--address defines the address of the pubsubclient server")
	data_path := flag.String("data_path", "a.out",
		"--data_path defines the data path to transfer")
	is_executable := flag.Bool("is_executable", false,
		"--is_executable determines whether --data_path is an executable")
	remote_alias := flag.String("remote_alias", "local",
		"--remote_alias defines the host to send command to")
	timeout := flag.Duration("timeout", 5*time.Second,
		"--timeout defines how long to wait for the rce to complete")
	out_file := flag.String("out_file", "/tmp/psrce_out",
		"--out_file describes where to write the remote command output.")

	flag.Parse()

	psclient, err := pubsubclientlib.NewPubSubClientInstance("client", "clientpass", *psclient_address)
	if err != nil {
		fmt.Printf("Could not create client: %s", err)
		return
	}

	fileBytes, err := os.ReadFile(*data_path)
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
	payload, payloadID, err := rcelib.SerializePayload(filepath.Base(*data_path), *is_executable, fileBytes)
	if err != nil {
		fmt.Printf("%s\n", err)
		return
	}
	err = psclient.Publish(publishTopic, payload)
	if err != nil {
		fmt.Printf("Publish error: %v\n", err)
		return
	}

	waitInterval := *timeout / 10
	waitStart := time.Now()
	for {
		messages, err := psclient.Poll()
		if err != nil {
			fmt.Printf("%s\n", err)
			return
		}
		foundResponse := false
		for _, message := range messages {
			resp, err := rcelib.DeserializeResponse(message.Msg)
			if err != nil {
				fmt.Printf("%s", err)
				return
			}
			if resp.ID == payloadID {
				if resp.Status == rcelib.RCEStatusError {
					fmt.Printf("Error")
				}
				err = os.WriteFile(*out_file, resp.Data, 0755)
				if err != nil {
					fmt.Printf("%s\n", err)
				}
				fmt.Printf("Wrote command output to %s \n", *out_file)
				foundResponse = true
			}
		}
		if foundResponse {
			break
		}
		timeSinceStart := time.Since(waitStart)
		if timeSinceStart > *timeout {
			fmt.Printf("Did not get a response in %s, exiting.\n", timeSinceStart)
			break
		}
		fmt.Printf("Response not found, waiting for %s...\n", waitInterval)
		time.Sleep(waitInterval)
	}
}
