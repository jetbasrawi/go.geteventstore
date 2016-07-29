// This example demonstrates how to write and read events and event metadata

package main

import (
	"log"
	"net/url"
	"time"

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

	streamName := "foostream"

	// Write some events to the eventstore
	writeEvents(client, streamName)

	// Read the events back
	readEvents(client, streamName)

}

func readEvents(client goes.Client, streamName string) {
	// Create a new stream reader
	reader := client.NewStreamReader(streamName)

	// The reader will default to version 0 and the first event returned in that
	// case will be the event at 0 on the stream. To begin reading from a
	// specific version call the NextVersion(int) method.
	// In this case the reader will begin reading at event number 5

	// Basic Authentication credentials can be set on the client.
	// Any request made will use these credentials.
	// You can change the credentials on a client and any subsequent requests
	// will use the new credentials. There is no need to create a new client
	// or stream reader.

	// By default the first call to next on the stream reader will start
	// cue up the event at version 0 if there are events to return
	//
	// To begin reading at a specific version use reader.NextVersion(2)
	//
	// Next() always return true. The boolean value returned is not intended
	// to provide information about status or control flow. It is simply a
	// convenient mechanism to facilitate an enumeration.
	for reader.Next() {

		// There are a number of errors that can occur when calling Next()
		// All of the events returned are typed.
		if reader.Err() != nil {
			switch err := reader.Err().(type) {

			// In the case of network and protocol errors such as when the
			// eventstore server is not online at the url provided a standard
			// *url.Error is returned. The causes of the error can be inspected
			// via this error. In this example we assume that we are waiting for
			// the server to come online and will respond to this error with a
			// retry after some time.
			// When the eventstore comes online there is a short period where
			// it responds with a StatusServiceUnavailable. The goes client
			// returns a TemporarilyUnavailableError and here we respond to this
			// with a retry after some duration.
			case *url.Error, *goes.TemporarilyUnavailableError:
				<-time.After(time.Duration(30) * time.Second)

			// If a stream is not found a StreamDoesNotExistError is returned.
			// In this example we will retry after some duration expecting that
			// the stream will be created eventually.
			case *goes.NotFoundError:
				<-time.After(time.Duration(10) * time.Second)

			// If the request is not authorised to read from the stream an
			// UnauthorizedError will be returned. For this example we assume
			// that this is not recoverable and we exit.
			case *goes.UnauthorizedError:
				log.Fatal(err)

			// When there are no events returned from the request a
			// NoMoreEventsError is returned. This may happen when requesting
			// events past the head of the stream.
			// In this example we respond by setting the reader to LongPoll.
			// The effect of this is that any subsequent calls to Next() will
			// block, waiting for the eventstore to either return events or
			// return nothing after the number of seconds specified in the call
			// to LongPoll.
			// This is how we can implement a form of subscription to a stream
			// over http. An equivalent to a catch up subsription, would be to
			// enumerate over the events in a stream and then to LongPoll when
			// there are no more events to return.
			case *goes.NoMoreEventsError:
				//Finished reading all the events.
				return

			// Handle any other errors
			default:
				log.Fatal(err)
			}
		} else {

			// If there are no errors, the reader has retrieved an event
			// the following code demonstrates how to deserialise the event data
			// returned from the eventstore.

			// the event data, event meta data and information about the
			// event are available via the reader's EventResponse() method.

			// One very important peice of data about the event is the event
			// type. The event type is a string containing the name of the event
			// type and is useful in more realistic scenarios for selecting
			// the correct type to pass to the scan method for deserialization.
			// eventType := reader.EventResponse().Event.EventType
			// In this simple example we only have one event type and so we do
			// not use it here.

			// Create an event to deserialise the event data into
			fooEvent := FooEvent{}

			// Create a map for the meta data
			fooMeta := make(map[string]string)

			// Call scan on the reader passing in pointers to the event and meta
			// data targets
			reader.Scan(&fooEvent, &fooMeta)

			// Check for any errors that have occured during deserialization
			if reader.Err() != nil {
				log.Fatal(reader.Err())
			}

			log.Printf("\n Event %d returned %#v\n Meta returned %#v\n\n", reader.EventResponse().Event.EventNumber, fooEvent, fooMeta)
		}
	}
}

func writeEvents(client goes.Client, streamName string) {
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
	writer := client.NewStreamWriter(streamName)

	// Write the events to the stream
	// The first argument allows you to specify the expected version. Here expected version
	// is nil and so the events will be appended at the head of the stream regardless of the
	// version of the stream.
	err := writer.Append(nil, goesEvent1, goesEvent2)
	if err != nil {
		log.Fatal(err)
	}

	// Lets repeat this but using an expected version that will cause an error
	// to demonstrate handling concurrency errors
	// This should result in a goes.ConcurrencyError
	v := 0
	err = writer.Append(&v, goesEvent1)
	if err != nil {
		log.Printf("Received expected error. %#v\n", err)
	}

	log.Printf("Written events to the eventStore.")

}
