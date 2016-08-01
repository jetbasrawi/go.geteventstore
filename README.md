#GOES

A HTTP Client for EventStore (https://geteventstore.com) written in Go (Golang)

Heavily commented example code can be found in the examles directory


##Create a new client

```go
    client, err := goes.NewClient(nil, "http://localhost:2113")
	if err != nil {
		log.Fatal(err)
	}

```

##Set Authentication

If required, you can set authentication on the client. Credentials can be changed at any time.
Requests are made with the credentials that were set last or none if none are set.

```go

    client.SetBasicAuth("admin", "changeit")

```

##Write Events and Event Metadata

```go

	event1 := &FooEvent{
		FooField: "Lorem Ipsum",
		BarField: "Dolor Sit Amet",
		BazField: 42,
	}

    event1Meta := make(map[string]string)
	event1Meta["Foo"] = "consectetur adipiscing elit"

	goesEvent1 := goes.ToEventData(goes.NewUUID(), "FooEvent", event1, event1Meta)

    writer := client.NewStreamWriter("FooStream")

    expectedVersion := 0
    err := writer.Append(&expectedVersion, event1)
    if err != nil {
        // Handle errors
    }

```


#Read Events

```go 

    reader := client.NewStreamReader("FooStream")

    for reader.Next() {

        if reader.Err() != nil {
            // Handle errors
        }

        fooEvent := FooEvent{}
        fooMeta := make(map[string]string)

        reader.Scan(&fooEvent, &fooMeta)
        if reader.Err() != nil {
            // Handle errors
        }

    }

```


