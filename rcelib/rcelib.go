package rcelib

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

type PayloadType int

const (
	PayloadUnknown PayloadType = iota
	PayloadExecutable
	PayloadData
)

type RCEPayload struct {
	Data []byte      `json:"Data"`
	Name string      `json:"Name"`
	Type PayloadType `json:"Type"`
	ID   string      `json:"ID"`
}

type RCEStatusType int

const (
	RCEStatusUnknown RCEStatusType = iota
	RCEStatusOk
	RCEStatusError
)

type RCEResponse struct {
	Data   []byte        `json:"Data"`
	Status RCEStatusType `json:"Status"`
	ID     string        `json:"ID"`
}

func SerializePayload(name string, isExecutable bool, payload []byte) ([]byte, string, error) {
	var payloadType PayloadType
	if isExecutable {
		payloadType = PayloadExecutable
	} else {
		payloadType = PayloadData
	}

	id, err := uuid.NewRandom()
	if err != nil {
		return nil, "", err
	}

	serialized, err := json.Marshal(RCEPayload{
		payload,
		name,
		payloadType,
		id.String(),
	})

	if err != nil {
		return nil, "", err
	}

	return serialized, id.String(), nil
}

func DeserializePayload(payload []byte) (*RCEPayload, error) {
	var rcePayload RCEPayload
	err := json.Unmarshal(payload, &rcePayload)
	return &rcePayload, err
}

func SerializeOkResponse(payload []byte, cmdID string) ([]byte, error) {
	return json.Marshal(RCEResponse{
		payload,
		RCEStatusOk,
		cmdID,
	})
}

func SerialzeErrorResponse(err error, cmdID string) ([]byte, error) {
	return json.Marshal(RCEResponse{
		[]byte(fmt.Sprintf("%s", err)),
		RCEStatusOk,
		cmdID,
	})
}

func DeserializeResponse(payload []byte) (*RCEResponse, error) {
	var rceResponse RCEResponse
	err := json.Unmarshal(payload, &rceResponse)
	return &rceResponse, err
}
