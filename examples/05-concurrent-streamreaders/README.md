#Concurrent StreamReaders

This example demonstrates how to use the goes client to use multiple stream readers concurrently.

The scenario is to read events from a global stream (e.g. a projection by category or event type) in one go func
and access a specific stream by name concurrently.

Once the GetEventStore server is up simply **go run** main.go from the concurrent-streamreaders directory.

```
$ go run main.go
```

