#Go.GetEventStore Examples


Getting Started
To run these example you should have an eventstore server running on the default port http://localhost:2113. 

You can download and run an instance of the eventstore on your local development environment or you can use run a server in docker as described below.

##Containerized Eventstore Server
If you are a docker user you can set up an eventstore server using the included Docker Compose file.

```
$ github.com/jetbasrawi/go.geteventstore/examples/docker-compose up

```

##About the examples

Studying the examples in this order should provide a good idea about how to use go.geteventstore.

###Read Write Events
The example demonstrates how to write events to the eventstore and then how to read those events back.

###Long Poll
The example demonstrates how to read events from a stream and then continue to listen for the arrival of new events.

###Delete Stream
The example demonstrates soft and hard delete of streams.

###Client
The example demonstrates how you can use the client directly to read events.

