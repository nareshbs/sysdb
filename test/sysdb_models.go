package tests

import (
	"encoding/json"
	"fmt"
	"sysdb"
)

type Msg struct {
	Code    int32  `json:"code"`
	Message string `json:"message"`
}

type OnEvent_Msg struct {
}

func (o *Msg) Save() (string, error) {
	a, err := json.Marshal(o)
	return string(a), err
}

func (o *Msg) Load(bytes []byte) error {
	err := json.Unmarshal(bytes, o)
	return err
}

func (o OnEvent_Msg) Event(path string, key string, evt sysdb.EventType, value sysdb.Obj, status error) {
	fmt.Printf("path = %v, event %v, value %v type %T\n", path, evt, value, value)
}

type Node struct {
	Id     int32  `json:"Id"`
	Desc   string `json:"Desc"`
	Uptime int64  `json:"Uptime"`
}

type OnEvent_Node struct {
}

func (o *Node) Save() (string, error) {
	a, err := json.Marshal(o)
	return string(a), err
}

func (o *Node) Load(bytes []byte) error {
	err := json.Unmarshal(bytes, o)
	return err
}

func (o OnEvent_Node) Event(path string, key string, evt sysdb.EventType, value sysdb.Obj, status error) {
	fmt.Printf("path = %v, event %v, value %v type %T\n", path, evt, value, value)
}

// Notification indicating an exceptional event occured
type Alert struct {
	Code        int64                  `json:"code"`        // numeric alert code of the event
	Description string                 `json:"description"` // description of the event
	Detail      map[string]interface{} `json:"detail"`      // An arbitrary JSON object, this allows us to keep the base specification extensible, The; interpretation of the objects is defined in application dependent manner
	Device      string                 `json:"device"`      // unique identifier of device generating the alert
	Time        string                 `json:"time"`        // timestamp of when the event occured in ISO-8601 UTC Format
}

func (o *Alert) Save() (string, error) {
	a, err := json.Marshal(o)
	return string(a), err
}

func (o *Alert) Load(bytes []byte) error {
	err := json.Unmarshal(bytes, o)
	return err
}
