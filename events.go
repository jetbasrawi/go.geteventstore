package goes

import (
	"encoding/json"
	"log"
	"time"
)

type EventResponse struct {
	Title   string
	ID      string
	Updated TimeStr
	Summary string
	Event   *Event
}

func (e *EventResponse) PrettyPrint() string {

	b, err := json.MarshalIndent(e, "", "	")
	if err != nil {
		log.Println(err)
		//panic(err)
	}
	return string(b)

}

type eventAtomResponse struct {
	Title   string      `json:"title"`
	ID      string      `json:"id"`
	Updated TimeStr     `json:"updated"`
	Summary string      `json:"summary"`
	Content interface{} `json:"content"`
}

func (e *eventAtomResponse) PrettyPrint() string {

	b, err := json.MarshalIndent(e, "", "	")
	if err != nil {
		log.Println(err)
		//panic(err)
	}
	return string(b)

}

type Event struct {
	EventStreamID string      `json:"eventStreamId,omitempty"`
	EventNumber   int         `json:"eventNumber,omitempty"`
	EventType     string      `json:"eventType,omitempty"`
	EventID       string      `json:"eventId,omitempty"`
	Data          interface{} `json:"data"`
	Links         []Link      `json:"links,omitempty"`
	MetaData      interface{} `json:"metadata,omitempty"`
}

func (e *Event) PrettyPrint() string {
	b, err := json.MarshalIndent(e, "", "	")
	if err != nil {
		panic(err)
	}
	return string(b)
}

type MetaData struct {
	EventType string      `json:"eventType"`
	EventID   string      `json:"eventId"`
	Data      interface{} `json:"data"`
}

type Link struct {
	URI      string `json:"uri"`
	Relation string `json:"relation"`
}

type TimeStr string

func Time(t time.Time) TimeStr {
	return TimeStr(t.Format("2006-01-02T15:04:05-07:00"))
}
