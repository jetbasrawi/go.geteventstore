#Go.GetEventStore [![license](https://img.shields.io/badge/license-BSD-blue.svg?maxAge=2592000)](https://github.com/jetbasrawi/go.geteventstore/blob/master/LICENSE.md) [![Go Report Card](https://goreportcard.com/badge/github.com/jetbasrawi/go.geteventstore)](https://goreportcard.com/report/github.com/jetbasrawi/go.geteventstore) [![GoDoc](https://godoc.org/github.com/jetbasrawi/go.geteventstore?status.svg)](https://godoc.org/github.com/jetbasrawi/go.geteventstore)

##A Golang client for the GetEventStore HTTP API. 
Go.GetEventStore is a http client for [GetEventStore](https://geteventstore.com) written in Go. The 
client abstracts interaction with the GetEventStore HTTP API providing easy to use features 
for reading and writing of events and event metadata.

Below are some code examples giving a summary view of how the client works. To learn to use 
the client in more detail, heavily commented example code can be found in the examples directory.

###Get the package
```
    $ go get github.com/jetbasrawi/go.geteventstore"
```

###Import the package
```go 
    import "github.com/jetbasrawi/go.geteventstore"
```


###Create a new client

```go
    client, err := goes.NewClient(nil, "http://youreventstore:2113")
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
	myGoesEvent := goes.NewEvent(goes.NewUUID(), "FooEvent", myEvent, myEventMeta)

    // Create a new StreamWriter
    writer := client.NewStreamWriter("FooStream")

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
events can be found in the longpoll example.

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
        err := reader.Scan(&fooEvent, &fooMeta)
        if err != nil {
            // Handle errors that occured during deserialization
        }
    }

```

###Polling Head of Stream

```go 

    // Create a new goes.StreamReader
    reader := client.NewStreamReader("FooStream")

    // Call Next to get the next event
    for reader.Next() {

        // Check if the call resulted in an error. 
        if reader.Err() != nil {
            **if e, ok := reader.Err().(*goes.NoMoreEvents); ok {
                reader.LongPoll(15)
            }**
        } else {

        fooEvent := FooEvent{}
        _ := reader.Scan(&fooEvent, &fooMeta)
        
        }
    }

```

###Deleting Streams

The client supports both soft delete and hard delete of event streams. 

```go

    // Soft delete or hard delete is specified by a boolean argument
    // here foostream will be soft deleted. 
    resp, err := client.DeleteStream("foostream", false)

```

Example code for deleting streams can be found in the examples directory.


###Direct use of the client

The StreamReader and StreamWriter types are the easiest way to read and write events. If you would like to implement
some other logic around reading events, some methods are available on the Client type.




