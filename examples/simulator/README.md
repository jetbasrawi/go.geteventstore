#Using the atom feed simulator

The atom feed simulator is a http handler that can be used by the net/http/httptest
server to simulate the atom feeds served by the eventstore.

The reads a stream from version 0 and when it reaches the head of the stream it begins to 
LongPoll the stream, listening for new events. This is similar to a catch up subscription.

This example demonstrates
    - Setting up the simulator
    - Reading events from a stream
    - Handling errors
    - Polling

The code is heavily commented so have a look at the example code for more details 
about how to set up and use the simulator.

To run the example simply **go run** it.

```
$ go run main.go
```



