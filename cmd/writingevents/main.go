// This example demonstrates how to write events and event metadata
// to the eventstore.

package main

import (
	"log"

	"github.com/jetbasrawi/goes"
)

// FooEvent is an example event type
//
// An event within your application will just be a plain go struct.
// The event will be serialized and deserialised to JSON and all of
// the standard golang JSON serialisation features apply. Here the
// struct does not use tags to specify a different name for the JSON
// fields or wether a field is optional. The name of the field as it
// is will be used.
type FooEvent struct {
	FooField string
	BarField string
	BazField int
}

func main() {

	// Creating a new Goes client.
	client, err := goes.NewClient(nil, "http://localhost:2113")
	if err != nil {
		log.Fatal(err)
	}

	// Set authentication credentials
	client.SetBasicAuth("admin", "changeit")

	// Create some events.
	// Here we will create two events and their associated metadata demonstrating how
	// to write events and event metadata to the eventstore

	// Create an event of type FooEvent
	event1 := &FooEvent{
		FooField: "Lorem Ipsum",
		BarField: "Dolor Sit Amet",
		BazField: 42,
	}

	// Create a map for metadata. This could equally be a struct or anything that can be
	// serialised and deserialised to JSON
	event1Meta := make(map[string]string)
	event1Meta["Foo"] = "consectetur adipiscing elit"

	// Create a goes.Event which contains the event data and metadata.
	//
	// Here the eventID and EventType have been specified explicitly.
	goesEvent1 := goes.ToEventData(goes.NewUUID(), "FooEvent", event1, event1Meta)

	// Create another event of type foo event
	event2 := &FooEvent{
		FooField: "Mary had a little lamb",
		BarField: "It's fleece was white as snow",
		BazField: 1,
	}

	// This time the eventID and EventType have been left blank.
	// The eventID will be an automatically generated uuid.
	// The eventType will be reflected from the type and will be "FooEvent"
	goesEvent2 := goes.ToEventData("", "", event2, nil)

	//Writing to the stream

	// Create a new streamwriter passing in the name of the stream you want to write to.
	// If the stream does not exist, it will be created when events are written to it.
	writer := client.NewStreamWriter("foostream")

	// Write the events to the stream
	// The first argument allows you to specify the expected version. Here expected version
	// is nil and so the events will be appended at the head of the stream regardless of the
	// version of the stream.
	err = writer.Append(nil, goesEvent1, goesEvent2)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Written events to the eventStore.")

}
