package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

type TopicAddedEvent struct {
	Name       string
	Popularity int
	Source     string
}

type EventResponse struct {
	Title   string      `json:"title"`
	ID      string      `json:"id"`
	Updated TimeStr     `json:"updated"`
	Summary string      `json:"summary"`
	Content interface{} `json:"content"`
}

type Event struct {
	EventStreamID string      `json:"eventStreamId"`
	EventNumber   int         `json:"eventNumber"`
	EventType     string      `json:"eventType"`
	EventID       string      `json:"eventId"`
	Data          interface{} `json:"data"`
	Links         []Link      `json:"links"`
	MetaData      interface{} `json:"metadata"`
}

type Link struct {
	URI      string `json:"uri"`
	Relation string `json:"relation"`
}

type TimeStr string

func Time(t time.Time) TimeStr {
	return TimeStr(t.Format("2006-01-02T15:04:05-07:00"))
}

var urls = []string{
	"http://localhost:2113/streams/TopicSource-StackOverflow/1100",
	"http://localhost:2113/streams/TopicSource-StackOverflow/1101",
	"http://localhost:2113/streams/TopicSource-StackOverflow/1102",
	"http://localhost:2113/streams/TopicSource-StackOverflow/1103",
	"http://localhost:2113/streams/TopicSource-StackOverflow/1104",
	"http://localhost:2113/streams/TopicSource-StackOverflow/1105",
	"http://localhost:2113/streams/TopicSource-StackOverflow/1106",
	"http://localhost:2113/streams/TopicSource-StackOverflow/1107",
}

func main() {

	status := true

	eventsChannel := make(chan *Event)

	go getEvents(eventsChannel)

	for status {
		select {
		case event := <-eventsChannel:
			if event != nil {
				printEvent(event)
			} else {
				status = false
			}
		}
	}

	fmt.Println("Finished")
}

func printEvent(e *Event) {
	topicAddedEvent := &TopicAddedEvent{}
	if data, ok := e.Data.(*json.RawMessage); ok {
		err := json.Unmarshal(*data, topicAddedEvent)
		if err != nil {
			fmt.Println(err)
		}
	}

	fmt.Printf("%#v\n", topicAddedEvent)

}

func loadUrls(linkChan chan string, statusChan chan bool) {

	var t, i int = 0, 0

	for {

		if t >= 100 {
			statusChan <- false
		}

		linkChan <- urls[i]
		i++

		if i >= len(urls)-1 {
			i = 0
		}

		t++
	}
}

func getEvents(eventsChannel chan *Event) {

	statusChannel := make(chan bool)
	linksChannel := make(chan string)

	go loadUrls(linksChannel, statusChannel)

	c := http.DefaultClient

	for {

		select {

		case url := <-linksChannel:

			r, err := http.NewRequest("GET", url, nil)
			if err != nil {
				log.Println(err)
			}

			r.Header.Set("Accept", "application/vnd.eventstore.atom+json")

			httpResp, err := c.Do(r)
			if err != nil {
				log.Println(err)
			}

			defer httpResp.Body.Close()

			var content json.RawMessage

			resp := &EventResponse{
				Content: &content,
			}

			err = json.NewDecoder(httpResp.Body).Decode(resp)
			if err == io.EOF {
				fmt.Println("EOF")
				err = nil
			}

			fmt.Println(resp.Updated)

			var data json.RawMessage
			event := &Event{
				Data: &data,
			}

			if err := json.Unmarshal(content, event); err != nil {
				log.Println(err)
			}

			eventsChannel <- event

		case <-statusChannel:
			eventsChannel <- nil
			break

		}

	}
}
