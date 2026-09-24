package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"pubsubclientlib"
	"rcelib"
	"strings"
	"sync"
	"time"
)

func main() {
	psclient_address := flag.String("psclient_address", "0.0.0.0:8099",
		"--address defines the address of the pubsubclient server")
	data_path := flag.String("data_path", "a.out",
		"--data_path defines the data path to transfer")
	is_executable := flag.Bool("is_executable", false,
		"--is_executable determines whether --data_path is an executable")
	remote_aliases := flag.String("remote_aliases", "local",
		"--remote_alias defines the hosts to send the command to (comma separated list)")
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

	var wg sync.WaitGroup
	var mu sync.Mutex
	payloadIds := make(map[string]string)

	funcToRun := func(remote_alias string) {
		defer wg.Done()
		fmt.Printf("running command on %s\n", remote_alias)
		subscribeTopic := fmt.Sprintf("%s_Outputs", remote_alias)
		err = psclient.Subscribe(subscribeTopic)
		if err != nil {
			fmt.Printf("Subscribe error: %v\n", err)
			return
		}

		publishTopic := fmt.Sprintf("%s_Commands", remote_alias)
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
		mu.Lock()
		payloadIds[payloadID] = remote_alias
		mu.Unlock()
	}

	for remote_alias := range strings.SplitSeq(*remote_aliases, ",") {
		go funcToRun(remote_alias)
		wg.Add(1)
	}
	wg.Wait()
	waitInterval := *timeout / 10
	waitStart := time.Now()
	for {
		messages, err := psclient.Poll()
		if err != nil {
			fmt.Printf("%s\n", err)
			return
		}
		for _, message := range messages {
			resp, err := rcelib.DeserializeResponse(message.Msg)
			if err != nil {
				fmt.Printf("%s", err)
				return
			}
			if remoteHost, ok := payloadIds[resp.ID]; ok {
				if resp.Status == rcelib.RCEStatusError {
					fmt.Printf("Error")
				}
				outFileForRemote := *out_file + "_" + remoteHost
				err = os.WriteFile(outFileForRemote, resp.Data, 0755)
				if err != nil {
					fmt.Printf("%s\n", err)
				}
				fmt.Printf("Wrote command output to %s \n", outFileForRemote)
				delete(payloadIds, resp.ID)
			}
		}
		if len(payloadIds) == 0 {
			break
		}
		timeSinceStart := time.Since(waitStart)
		if timeSinceStart > *timeout {
			fmt.Print("Did not get responses from: ")
			for _, remoteHost := range payloadIds {
				fmt.Printf("%s ", remoteHost)
			}
			fmt.Println()
			break
		}
		fmt.Printf("Response not found, waiting for %s...\n", waitInterval)
		time.Sleep(waitInterval)
	}
	fmt.Println("All workers finished")
}
