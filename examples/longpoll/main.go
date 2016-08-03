// This example demonstrates using LongPoll to listen for events at the
// head of a stream.
//
// When the ES-LongPoll header is used, the server will wait for some
// specified period of time before returning, unless new events are written
// to the stream being polled.
//
// LongPoll is useful for creating a catch-up like subscription. You can
// read all of the events in a stream and then poll at the head of the stream
// and recieve new events as they arrive.

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"time"

	"github.com/jetbasrawi/go.geteventstore"
)

var (
	serverURL = "http://localhost:2113"
)

// FooEvent is an example event type
//
// An event within your application will just be a plain go struct.
// The event will be serialized and deserialised to JSON and all of
// the standard golang JSON serialization features apply. Here the
// struct uses tags to specify a mapping from the JSON to the struct
// field names.
type FooEvent struct {
	Foo string `json:"foo"`
}

// FooMeta is a struct for metadata for FooEvents
type FooMeta struct {
	Bar string `json:"bar"`
}

func main() {

	// Creating a new Goes client.
	client, err := goes.NewClient(nil, serverURL)
	if err != nil {
		log.Fatal(err)
	}

	// Set authentication credentials
	client.SetBasicAuth("admin", "changeit")

	streamName := "longpollstream"

	writeEvents(client, streamName)

	log.Printf("\nReading events from the eventstore\n")

	// Create a new stream reader
	reader := client.NewStreamReader(streamName)

	for reader.Next() {

		if reader.Err() != nil {
			switch err := reader.Err().(type) {

			case *url.Error, *goes.TemporarilyUnavailableError:
				log.Println("The server is not ready. Will retry after 30 seconds.")
				<-time.After(time.Duration(30) * time.Second)

			case *goes.NotFoundError:
				<-time.After(time.Duration(10) * time.Second)

			case *goes.UnauthorizedError:
				log.Fatal(err)

			// When there are no events returned from the request a NoMoreEventsError is
			// returned. This typically happens when requesting events past the head of
			// the stream.
			// In this example we will set the reader to LongPoll the stream listening for
			// new events.
			case *goes.NoMoreEventsError:
				log.Println("No more events. Will poll head of stream.")
				reader.LongPoll(15)

			// Handle any other errors
			default:
				log.Fatal(err)
			}
		} else {

			event := FooEvent{}
			meta := FooMeta{}

			if err := reader.Scan(&event, &meta); err != nil {
				log.Fatal(err)
			}

			log.Printf("\n Event %d returned %#v\n Meta returned %#v\n\n", reader.EventResponse().Event.EventNumber, event, meta)
		}
	}

}

// writeEvents will create an initial set of events and then write them to
// the eventstore.
// At two second intervals a random number of events between 1 and 5 will be written
// to the eventstore.
func writeEvents(client *goes.Client, streamName string) {
	existingEvents := createTestEvents(10, streamName, serverURL, "FooEvent")

	writer := client.NewStreamWriter(streamName)
	err := writer.Append(nil, existingEvents...)
	if err != nil {
		log.Fatal(err)
	}

	ticker := time.NewTicker(time.Second * 2)
	go func() {
		for _ = range ticker.C {
			num := rand.Intn(5)
			newEvents := createTestEvents(num, streamName, serverURL, "FooEvent")
			writer.Append(nil, newEvents...)
		}
	}()

}

func createTestEvent(stream, server, eventType string, eventNumber int, data *json.RawMessage, meta *json.RawMessage) *goes.Event {
	e := goes.Event{}
	e.EventStreamID = stream
	e.EventNumber = eventNumber
	e.EventType = eventType

	uuid := goes.NewUUID()
	e.EventID = uuid

	e.Data = data

	u := fmt.Sprintf("%s/streams/%s", server, stream)
	eu := fmt.Sprintf("%s/%d/", u, eventNumber)
	l1 := goes.Link{URI: eu, Relation: "edit"}
	l2 := goes.Link{URI: eu, Relation: "alternate"}
	ls := []goes.Link{l1, l2}
	e.Links = ls

	if meta != nil {
		e.MetaData = meta
	} else {
		m := "\"\""
		mraw := json.RawMessage(m)
		e.MetaData = &mraw
	}
	return &e
}

func createTestEvents(numEvents int, stream string, server string, eventTypes ...string) []*goes.Event {
	se := []*goes.Event{}
	for i := 0; i < numEvents; i++ {
		r := rand.Intn(len(eventTypes))
		eventType := eventTypes[r]

		uuid := goes.NewUUID()
		d := fmt.Sprintf("{ \"foo\" : \"%s\" }", uuid)
		raw := json.RawMessage(d)

		m := fmt.Sprintf("{\"bar\": \"%s\"}", uuid)
		mraw := json.RawMessage(m)

		e := createTestEvent(stream, server, eventType, i, &raw, &mraw)

		se = append(se, e)
	}
	return se
}
