// This example demonstrates soft deleting and hard deleting a stream.
// The example will write an event to a stream then soft delete the stream.
// After this a new event will be written to the soft deleted stream and the
// stream will be undeleted. The stream will then be hard deleted and a subsequent
// request to the stream will be shown to fail.

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

	streamName := goes.NewUUID()

	log.Println("1. Write an event to a new stream.")
	writer := client.NewStreamWriter(streamName)
	ev1 := goes.NewEvent("", "", &FooEvent{"Event 1"}, nil)
	err = writer.Append(nil, ev1)
	if err != nil {
		log.Fatal(err)
	}

	count := 0

	log.Println("2. Read the event back. There should be one event in the stream.")
	reader := client.NewStreamReader(streamName)
	for reader.Next() {
		if reader.Err() != nil {
			if e, ok := reader.Err().(*goes.ErrNoMoreEvents); !ok {
				log.Fatal(e)
			}
			if count == 1 {
				log.Println(" - The stream contains one event.")
				break
			} else {
				log.Fatal(" - The stream contains an unexpected number of events.")
			}
		} else {
			count++
		}
	}

	log.Println("3. Soft Delete the stream.")
	resp, err := client.DeleteStream(streamName, false)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("4. Check that the stream has been soft deleted.")
	// The status code returned should be a 204 No Content
	if resp.StatusCode == http.StatusNoContent {
		log.Println(" - The stream has been soft deleted.")
	} else {
		log.Fatal(resp.Status)
	}

	log.Println("5. Try to read the soft deleted stream.")
	// This should result in an ErrNotFound.
	reader = client.NewStreamReader(streamName)
	for reader.Next() {
		if reader.Err() != nil {
			if e, ok := reader.Err().(*goes.ErrNotFound); !ok {
				log.Fatal(e)
			}
			log.Println(" - As expected, the stream is not found.")
			break
		}
	}

	log.Println("6. Write a second event to the soft deleted stream. The stream should be undeleted.")
	// This should result in the stream being undeleted and the second event
	// being appended to the stream.
	ev2 := goes.NewEvent("", "", &FooEvent{"Event 2"}, nil)
	err = writer.Append(nil, ev2)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("7. Read the events in the stream. There should be two events.")
	reader = client.NewStreamReader(streamName)
	for reader.Next() {
		if reader.Err() != nil {
			if e, ok := reader.Err().(*goes.ErrNotFound); ok {
				log.Fatal(e)
			}

			if count == 2 {
				log.Printf(" - The deleted stream has been undeleted and contains two events.")
				break
			} else {
				log.Fatal(" - The number of events found is not correct.")
			}

		} else {
			count++
		}
	}

	log.Println("8. Hard delete the stream")
	resp, err = client.DeleteStream(streamName, true)
	if err != nil {
		log.Fatal(err)
	}

	// Check that the status code returned is a 204 No Content.
	if resp.StatusCode == http.StatusNoContent {
		log.Println(" - The stream has been hard deleted.")
	} else {
		log.Fatal(resp.Status)
	}

	log.Println("9. Try to write to the hard deleted stream. This should result in an ErrDeleted")
	ev3 := goes.NewEvent("", "", &FooEvent{"Event 3"}, nil)
	err = writer.Append(nil, ev3)
	if err != nil {
		if _, ok := err.(*goes.ErrDeleted); ok {
			log.Println(" - As expected, an attempt to write to the hard deleted stream fails.")
		} else {
			log.Fatal(err)
		}
	}

	log.Println("10. The example has completed successfully.")
}
