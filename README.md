#GOES [![license](https://img.shields.io/github/license/mashape/apistatus.svg?maxAge=2592000)](https://opensource.org/licenses/MIT) [![Go Report Card](https://goreportcard.com/badge/github.com/jetbasrawi/goes)](https://goreportcard.com/report/github.com/jetbasrawi/goes)

##A Golang HTTP Client for EventStore 

GOES is a http client for [EventStore](https://geteventstore.com) written in Go. The 
client abstracts interaction with the GetEventStore HTTP API providing easy to use features 
for reading and writing events and event metadata.

Below are some code examples giving a summary view of how the client works. To learn to use 
the client in more detail, commented example code can be found in the examples directory.

###Import the package
```go 
    import "github.com/jetbasrawi/goes"
```


###Create a new client

```go
    client, err := goes.NewClient(nil, "http://localhost:2113")
	if err != nil {
		log.Fatal(err)
	}

```

###Set Authentication

If required, you can set authentication on the client. Credentials can be changed at any time.
Requests are made with the credentials that were set last or none if none are set.

```go

    client.SetBasicAuth("admin", "changeit")

```

###Write Events and Event Metadata

Writing events and event metadata are supported via the StreamWriter. 

```go

    // Create a new StreamWriter
    writer := client.NewStreamWriter("FooStream")

    // Create your event
	myEvent := &FooEvent{
		FooField: "Lorem Ipsum",
		BarField: "Dolor Sit Amet",
		BazField: 42,
	}

    // Create your metadata type
    myEventMeta := make(map[string]string)
	myEventMeta["Foo"] = "consectetur adipiscing elit"

    // Wrap your event and event metadata in a goes.Event
	myGoesEvent := goes.ToEventData(goes.NewUUID(), "FooEvent", myEvent, myEventMeta)

    // Write the event to the stream, here we pass nil as the expectedVersion as we 
    // are not wanting to flag concurrency errors
    err := writer.Append(nil, myGoesEvent)
    if err != nil {
        // Handle errors
    }

```


###Read Events

Reading events using the goes.StreamReader loosely follows the iterator idiom used in 
the "database/sql" package and other go libraries that deal with databases. This idiom 
works very well for reading events and provides an easy way to control the rate at which 
events are returned and to poll at the head of a stream.

An example of how to read all the events in a stream and then exit can be found in the 
read and write events example.

An example of how to read up to the head of the stream and then continue to listen for new 
events can be found in the simulator example.

```go 

    // Create a new goes.StreamReader
    reader := client.NewStreamReader("FooStream")

    // Call Next to get the next event
    for reader.Next() {

        // Check if the call resulted in an error. 
        if reader.Err() != nil {
            // Handle errors
        }

        // If the call did not result in an error then an event was returned
        // Create the application types to hold the deserialized even data and meta data
        fooEvent := FooEvent{}
        fooMeta := make(map[string]string)

        // Call scan to deserialize the event data and meta data into your types
        reader.Scan(&fooEvent, &fooMeta)
        if reader.Err() != nil {
            // Handle errors that occured during deserialization
        }

    }

```


