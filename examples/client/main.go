// This example demonstrates using methods on the client to get feed pages
// and events.

package main

import (
	"log"

	"github.com/jetbasrawi/go.geteventstore"
)

// FooEvent is a test event type
type FooEvent struct {
	Foo string
}

func main() {

	client, err := goes.NewClient(nil, "http://localhost:2113")
	if err != nil {
		log.Fatal(err)
	}

	// Generate a random stream name.
	streamName := goes.NewUUID()

	// Write some events to the stream using a StreamWriter.
	streamWriter := client.NewStreamWriter(streamName)
	a := goes.NewEvent("", "", &FooEvent{goes.NewUUID()}, nil)
	b := goes.NewEvent("", "", &FooEvent{goes.NewUUID()}, nil)
	c := goes.NewEvent("", "", &FooEvent{goes.NewUUID()}, nil)
	streamWriter.Append(nil, a, b, c)

	// Get the path for the atom feed at the head of the stream.
	path, err := client.GetFeedPath(streamName, "backward", -1, 10)
	if err != nil {
		log.Fatal(err)
	}

	// Read the feed page.
	feed, _, err := client.ReadFeed(path)
	if err != nil {
		log.Fatal(err)
	}

	// Extract the event URLs from the feed page
	es, err := feed.GetEventURLs()
	if err != nil {
		log.Fatal(err)
	}

	// Get the events
	for _, url := range es {
		ev, _, err := client.GetEvent(url)
		if err != nil {
			log.Fatal(err)
		}
		log.Println(ev.PrettyPrint())
	}
}
