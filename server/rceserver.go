package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"pubsubclientlib"
	"rcelib"
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
			rcePayload, err := rcelib.DeserializePayload(message.Msg)
			if err != nil {
				fmt.Printf("%s\n", err)
				return
			}
			out, err := os.OpenFile(rcePayload.Name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
			if err != nil {
				fmt.Printf("%s\n", err)
				return
			}
			if _, err := out.Write(rcePayload.Data); err != nil {
				fmt.Printf("%s\n", err)
				break
			}
			out.Close()
			var responseString string
			var stdout []byte
			if rcePayload.Type == rcelib.PayloadExecutable {
				fmt.Printf("executing %s\n", rcePayload.Name)
				cmd := exec.Command(fmt.Sprintf("./%s", rcePayload.Name))
				stdout, err = cmd.Output()
				if err != nil {
					fmt.Printf("%s\n", err)
					responseString = "Error"
				} else {
					fmt.Print(string(stdout))
					responseString = string(stdout)
				}
			} else {
				responseString = fmt.Sprintf("%s saved successfully.", rcePayload.Name)
			}
			var responsePayload []byte
			if err != nil {
				responsePayload, err = rcelib.SerialzeErrorResponse(err, rcePayload.ID)
			} else {
				responsePayload, err = rcelib.SerializeOkResponse([]byte(responseString), rcePayload.ID)
			}
			if err != nil {
				fmt.Printf("%s\n", err)
				return
			}
			err = psclient.Publish(outboundTopic, responsePayload)
			if err != nil {
				fmt.Printf("%s\n", err)
				return
			}
		}
		time.Sleep(time.Second * 5)
	}
}
