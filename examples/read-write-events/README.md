#Reading and writing events

This example demonstrates how to use the goes client to write events and event metadata to a stream and then read them back.

The code is heavily commented so just have a look at the code for further details.

To run this example you should have an eventstore server running on http://localhost:2113

If you are a docker user you can set up an eventstore server using the included Docker Compose file.

```
$ docker compose up -f examples/read-write-events/docker-compose.yml
```
Once the server is up simply **go run** the example.

```
$ go run examples/read-write-events/main.go
```

