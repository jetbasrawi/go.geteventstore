// This example demonstrates soft deleting and hard deleting a stream.

package main

import (
	"log"
	"net/http"

	"github.com/jetbasrawi/go.geteventstore"
)

// FooEvent is an example event
type FooEvent struct {
	Foo string
}

func main() {

	// Create a new client
	client, err := goes.NewClient(nil, "http://localhost:2113")
	if err != nil {
		log.Fatal(err)
	}

	streamName := "deletestream"

	// Write an event to a new stream
	writer := client.NewStreamWriter(streamName)
	ev1 := goes.NewEvent("", "", &FooEvent{"Event 1"}, nil)
	err = writer.Append(nil, ev1)
	if err != nil {
		log.Fatal(err)
	}

	count := 0

	// Read the event back.
	// There should be one event int the stream.
	reader := client.NewStreamReader(streamName)
	for reader.Next() {
		if reader.Err() != nil {
			switch err := reader.Err().(type) {
			case *goes.ErrNoMoreEvents:
				if count == 1 {
					log.Println("The stream contains one event.")
				} else {
					log.Fatal("The stream contains an unexpected number of events.")
				}
			default:
				log.Fatal(err)
			}
		} else {
			count++
		}
	}

	// Soft Delete the stream.
	resp, err := client.DeleteStream(streamName, false)
	if err != nil {
		log.Fatal(err)
	}

	// Check that the stream has been soft deleted.
	// The status code returned should be a 204 No Content
	if resp.StatusCode == http.StatusNoContent {
		log.Println("The stream has been soft deleted.")
	} else {
		log.Fatal(resp.Status)
	}

	// Try to read the soft deleted stream.
	// This should result in an ErrNotFound.
	reader = client.NewStreamReader(streamName)
	for reader.Next() {
		if reader.Err() != nil {
			switch err := reader.Err().(type) {
			case *goes.ErrNotFound:
				log.Println("As expected, the stream is not found.")
				break
			default:
				log.Fatal(err)
			}
		}
	}

	// Write a second event to the soft deleted stream.
	// This should result in the stream being undeleted and the second event
	// being appended to the stream.
	ev2 := goes.NewEvent("", "", &FooEvent{"Event 2"}, nil)
	err = writer.Append(nil, ev2)
	if err != nil {
		log.Fatal(err)
	}

	// Read the events in the stream.
	// There should be two events.
	reader = client.NewStreamReader(streamName)
	for reader.Next() {
		if reader.Err() != nil {
			switch err := reader.Err().(type) {
			case *goes.ErrNoMoreEvents:
				if count == 2 {
					log.Printf("The deleted stream has been undeleted and contains two events.")
					break
				} else {
					log.Fatal("The number of events found is not correct.")
				}
			default:
				log.Fatal(err)
			}
		} else {
			count++
		}
	}

	// Hard delete the stream
	resp, err = client.DeleteStream(streamName, true)
	if err != nil {
		log.Fatal(err)
	}

	// Check that the status code returned is a 204
	if resp.StatusCode == http.StatusNoContent {
		log.Println("The stream has been hard deleted.")
	} else {
		log.Fatal(resp.Status)
	}

	// An attempt to write to the hard deleted stream should result in an ErrDeleted
	ev3 := goes.NewEvent("", "", &FooEvent{"Event 3"}, nil)
	err = writer.Append(nil, ev3)
	if err != nil {
		if _, ok := err.(*goes.ErrDeleted); ok {
			log.Println("As expected, an attempt to write to the hard deleted stream fails.")
		} else {
			log.Fatal(err)
		}
	}
}
