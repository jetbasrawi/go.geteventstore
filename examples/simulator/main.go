// Atom feed simulator, reading stream and polling
//
// This example demonstrates how to use the atom feed simulator
// The example will set up the simulator to return 25 randomly generated
// events.  The first 10 events will be returned as if they exist in the
// eventstore. The subsequent events will be returned as a result of
// polling and be returned at intervals, simulating the arrival of new
// events while polling.

package main

import (
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"time"

	"github.com/jetbasrawi/goes"
)

var (
	mux       *http.ServeMux
	server    *httptest.Server
	numEvents = 25
)

func init() {

	// Set up a test server
	mux = http.NewServeMux()
	server = httptest.NewServer(mux)

	// Create a collection of random test events for the server to return.
	es := goes.CreateTestEvents(numEvents, "FooStream", server.URL, "FooEvent")

	// The atom feed returned will use this url to populate those parts of the
	// feed that contain urls such as links to events.
	u, _ := url.Parse(server.URL)

	// Create a new atom feed simulator
	handler, err := goes.NewAtomFeedSimulator(es, u, nil, 10)
	if err != nil {
		log.Fatal(err)
	}

	// Add the sim handler to the multiplexer
	mux.Handle("/", handler)
}

// FooEvent is a struct to into which we will deserialize event data
// Test events have a single field called Foo which are pupulated with
// a random string, which is just a uuid string
type FooEvent struct {
	Foo string `json:"foo"`
}

func main() {

	// Create a client providing the full URL to your eventstore server
	client, err := goes.NewClient(nil, server.URL)
	if err != nil {
		log.Fatal(err)
	}

	streamName := "FooStream"

	// Create a new stream reader for FooStream
	reader := client.NewStreamReader(streamName)

	// The reader will default to version 0 and the first event returned in that
	// case will be the event at 0 on the stream. To begin reading from a
	// specific version call the NextVersion(int) method.
	// In this case the reader will begin reading at event number 5
	reader.NextVersion(5)

	// Basic Authentication credentials can be set on the client.
	// Any request made will use these credentials.
	// You can change the credentials on a client and any subsequent requests
	// will use the new credentials. There is no need to create a new client
	// or stream reader.
	client.SetBasicAuth("admin", "changeit")

	log.Printf("\n\n Will begin reading stream.\n\n")

	// By default the first call to next on the stream reader will start
	// cue up the event at version 0 if there are events to return
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
				log.Fatalf("Not authorized to connect to the stream %s", streamName)

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
				// if reader.Version() >= numEvents-1 {
				// 	log.Println("Finished polling head of stream. Exiting.")
				// 	return
				// }

				log.Println("No more events. Will poll head of stream.")
				reader.LongPoll(5)

			// Handle unexpected errors
			default:
				log.Fatal(err)
			}
		} else {

			// If there are no errors, the reader has retrieved an event
			// the following code demonstrates how to deserialise the event data
			// returned from the eventstore.

			// the event data, event meta data and information about the
			// event are available via the reader's EventResponse() method.

			// One very important piece of data about the event is the event
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
			err := reader.Scan(&fooEvent, &fooMeta)
			// Check for any errors that have occurred during deserialization
			if err != nil {
				log.Fatal(reader.Err())
			}

			log.Printf("\n Event %d returned %#v\n Meta returned %#v\n\n", reader.EventResponse().Event.EventNumber, fooEvent, fooMeta)
		}
	}
}
