#Using LongPoll to listen for new events

A common use case is to read events to the head of an event stream and then listen for new 
events. This is sometimes called a catch-up subscription.

This example demonstrates how to do this.

Once the server is up simply **go run** the examples.

```
    $ go run main.go
```

The example will write a number of events to the eventstore that simulate existing events. It will 
then asynchrounously write events to the eventstore simulating the ongoing arrival of events at 
intervals.

The reader will read the existing events and then continue to poll the stream for new events.

This will continue until you terminate the application.
