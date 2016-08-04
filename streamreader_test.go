package goes

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"

	. "gopkg.in/check.v1"
)

var _ = Suite(&StreamReaderSuite{})

type StreamReaderSuite struct{}

func (s *StreamReaderSuite) SetUpTest(c *C) {
	setup()
}
func (s *StreamReaderSuite) TearDownTest(c *C) {
	teardown()
}
func (s *StreamReaderSuite) TestGetStream(c *C) {
	streamName := "SomeStreamName"
	someStream := client.NewStreamReader(streamName)
	c.Assert(someStream, NotNil)
}

type FooEvent struct {
	Foo string `json:"foo"`
}

func (s *StreamReaderSuite) TestNextMovesForwardOneEvent(c *C) {
	streamName := "SomeStream"
	ne := 10
	es := CreateTestEvents(ne, streamName, server.URL, "EventTypeX")

	var ev *FooEvent
	for _, v := range es {
		data, _ := v.Data.(*json.RawMessage)
		_ = json.Unmarshal(*data, &ev)
	}

	setupSimulator(es, nil)

	//The first event is version 0 and is available after
	//the call to next
	count := 0
	stream := client.NewStreamReader(streamName)
	for stream.Next() {
		c.Assert(stream.Version(), Equals, count)
		if count <= len(es)-1 {
			break
		}

		count--
		var got, want *FooEvent
		err := stream.Scan(&got, nil)
		c.Assert(err, IsNil)

		ev := es[count]
		data, _ := ev.Data.(*json.RawMessage)
		err = json.Unmarshal(*data, &want)
		c.Assert(err, IsNil)

		c.Assert(got, DeepEquals, want)
	}
}

// If the consumer is trying to access a system stream such as a category
// stream, the response from the server may actually be an unauthorised error
// rather than not found. This is testing the not found case.
func (s *StreamReaderSuite) TestNextReturnsErrorIfStreamDoesNotExist(c *C) {
	stream := client.NewStreamReader("Something")
	ok := stream.Next()
	c.Assert(ok, Equals, true)
	c.Assert(typeOf(stream.Err()), DeepEquals, "ErrNotFound")
}

// Tests that the stream returns appropriate error when the request results in
// an Unauthorized status
func (s *StreamReaderSuite) TestNextErrorsIfNotAuthorisedToAccessStream(c *C) {
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "")
	})

	stream := client.NewStreamReader("Something")
	ok := stream.Next()
	c.Assert(ok, Equals, true)
	c.Assert(typeOf(stream.Err()), DeepEquals, "ErrUnauthorized")
}

// Testing the case where next has reached the end of the stream and there are
// no more events to return. The EventResponse should be nil but next returns
// true
func (s *StreamReaderSuite) TestNextAtHeadOfStreamReturnsTrueWithNoEvent(c *C) {
	streamName := "SomeStream"
	ne := 1
	es := CreateTestEvents(ne, streamName, server.URL, "EventTypeX")
	setupSimulator(es, nil)

	stream := client.NewStreamReader(streamName)
	res := stream.Next()
	c.Assert(res, Equals, true)
	c.Assert(stream.EventResponse, NotNil)

	res = stream.Next()
	c.Assert(res, Equals, true)
	c.Assert(stream.EventResponse(), IsNil)
}

// When there are no events to return, the stream error will be set to an
// appropriate error
func (s *StreamReaderSuite) TestReturnsErrorIfThereIsNoNextEventToReturn(c *C) {
	streamName := "SomeStream"
	ne := 1
	es := CreateTestEvents(ne, streamName, server.URL, "FooEvent")
	setupSimulator(es, nil)
	want := &ErrNoMoreEvents{}

	stream := client.NewStreamReader(streamName)
	_ = stream.Next()

	_ = stream.Next()
	c.Assert(stream.Err(), DeepEquals, want)
	c.Assert(stream.EventResponse(), IsNil)
}

// Tests the deserialization of events
func (s *StreamReaderSuite) TestScanEventData(c *C) {
	streamName := "SomeStream"
	ne := 1
	es := CreateTestEvents(ne, streamName, server.URL, "FooEvent")

	setupSimulator(es, nil)

	stream := client.NewStreamReader(streamName)
	_ = stream.Next()
	got := &FooEvent{}
	stream.Scan(&got, nil)
	c.Assert(stream.Err(), IsNil)

	//Get the AggregateID from the event headers
	raw, _ := es[0].Data.(*json.RawMessage)
	want := &FooEvent{}
	err := json.Unmarshal(*raw, &want)
	c.Assert(err, IsNil)
	c.Assert(got, DeepEquals, want)
}

// Test the deserialization of event meta data
func (s *StreamReaderSuite) TestScanMetaData(c *C) {
	streamName := "SomeStream"
	ne := 1
	es := CreateTestEvents(ne, streamName, server.URL, "FooEvent")

	setupSimulator(es, nil)

	stream := client.NewStreamReader(streamName)
	_ = stream.Next()
	got := make(map[string]string)
	stream.Scan(nil, &got)
	c.Assert(stream.Err(), IsNil)

	//Get the AggregateID from the event headers
	raw, _ := es[0].MetaData.(*json.RawMessage)
	want := make(map[string]string)
	err := json.Unmarshal(*raw, &want)
	c.Assert(err, IsNil)
	c.Assert(got, DeepEquals, want)
}

// Test reading the stream at a specific version
func (s *StreamReaderSuite) TestSetStreamVersion(c *C) {
	streamName := "FooStream"
	ne := 25
	es := CreateTestEvents(ne, streamName, server.URL, "FooEvent")

	setupSimulator(es, nil)

	stream := client.NewStreamReader(streamName)
	stream.NextVersion(9)
	stream.Next()
	c.Assert(stream.Err(), IsNil)
	c.Assert(stream.EventResponse().Event.EventNumber, Equals, 9)
}

// Test that the feed returns the correct number of results when the request
// span exceeds the number of events
func (s *StreamReaderSuite) TestFeedWithFewerEntriesThanThePageSize(c *C) {
	streamName := "SomeStream"
	ne := 25
	es := CreateTestEvents(ne, streamName, server.URL, "FooEvent")

	setupSimulator(es, nil)

	stream := client.NewStreamReader(streamName)
	count := 0
	for stream.Next() {
		switch err := stream.Err().(type) {
		case *ErrNoMoreEvents:
			c.Assert(err, NotNil)
			c.Assert(count, Equals, ne)
			return
		}
		c.Assert(stream.EventResponse(), NotNil)
		c.Assert(stream.Version(), Equals, count)
		count++
	}
}

// Tests that the request to the stream is made with the ES-LongPoll header.
// The header will cause the server to wait for events to be returned on requests
// to the server at the head of the stream.
func (s *StreamReaderSuite) TestLongPollNonZero(c *C) {
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		val := r.Header.Get("ES-LongPoll")
		c.Assert(val, Equals, "15")
	})
	stream := client.NewStreamReader("SomeStream")
	stream.LongPoll(15)
	stream.Next()
}

// Test that long poll header is not included when long poll is set to 0
func (s *StreamReaderSuite) TestLongPollZero(c *C) {
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		val := r.Header.Get("ES-LongPoll")
		c.Assert(val, Equals, "")
	})
	stream := client.NewStreamReader("SomeStream")
	stream.LongPoll(0)
	stream.Next()
}

func (s *StreamReaderSuite) TestGetMetaReturnsNilWhenStreamMetaDataIsEmpty(c *C) {
	stream := "Some-Stream"
	es := CreateTestEvents(10, stream, server.URL, "EventTypeX")
	setupSimulator(es, nil)

	streamReader := client.NewStreamReader(stream)
	got, err := streamReader.MetaData()

	c.Assert(err, IsNil)
	c.Assert(got, IsNil)
}

func (s *StreamReaderSuite) TestGetMetaData(c *C) {
	d := fmt.Sprintf("{ \"foo\" : %d }", rand.Intn(9999))
	raw := json.RawMessage(d)
	stream := "Some-Stream"
	es := CreateTestEvents(10, stream, server.URL, "EventTypeX")
	m := CreateTestEvent(stream, server.URL, "metadata", 10, &raw, nil)
	want := CreateTestEventResponse(m, nil)
	setupSimulator(es, m)

	reader := client.NewStreamReader(stream)
	got, err := reader.MetaData()
	c.Assert(err, IsNil)
	c.Assert(got.PrettyPrint(), Equals, want.PrettyPrint())
}

func (s *StreamReaderSuite) TestGetMetaDataReturnsErrUnauthorizedWhenGettingMetaURL(c *C) {
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, "{}")
	})
	reader := client.NewStreamReader("SomeStream")
	m, err := reader.MetaData()
	c.Assert(typeOf(err), Equals, "ErrUnauthorized")
	c.Assert(m, IsNil)
}

func (s *StreamReaderSuite) TestGetMetaDataReturnsErrNotFoundWhenGettingMetaURL(c *C) {
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, "{}")
	})
	reader := client.NewStreamReader("SomeStream")
	m, err := reader.MetaData()
	c.Assert(typeOf(err), Equals, "ErrNotFound")
	c.Assert(m, IsNil)
}

func (s *StreamReaderSuite) TestGetMetaDataReturnsErrTemporarilyUnavailableWhenGettingMetaURL(c *C) {
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, "{}")
	})
	reader := client.NewStreamReader("SomeStream")
	m, err := reader.MetaData()
	c.Assert(typeOf(err), Equals, "ErrTemporarilyUnavailable")
	c.Assert(m, IsNil)
}

func (s *StreamReaderSuite) TestGetMetaDataReturnsErrUnauthorizedWhenGettingEvent(c *C) {
	stream := "SomeStream"
	path := fmt.Sprintf("/streams/%s", stream)
	feedURL := fmt.Sprintf("%s%s", server.URL, path)
	es := CreateTestEvents(1, stream, server.URL, "EventTypeX")

	f, _ := CreateTestFeed(es, feedURL)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.String() == path+"/0/forward/1" {
			fmt.Fprintf(w, f.PrettyPrint())
		} else if r.URL.String() == "/streams/SomeStream/metadata" {
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, "{}")
		}
	})
	reader := client.NewStreamReader("SomeStream")
	m, err := reader.MetaData()
	c.Assert(typeOf(err), Equals, "ErrUnauthorized")
	c.Assert(m, IsNil)
}

func (s *StreamReaderSuite) TestGetMetaDataReturnsErrNotFoundWhenGettingEvent(c *C) {
	stream := "SomeStream"
	path := fmt.Sprintf("/streams/%s", stream)
	feedURL := fmt.Sprintf("%s%s", server.URL, path)
	es := CreateTestEvents(1, stream, server.URL, "EventTypeX")

	f, _ := CreateTestFeed(es, feedURL)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.String() == path+"/0/forward/1" {
			fmt.Fprintf(w, f.PrettyPrint())
		} else if r.URL.String() == "/streams/SomeStream/metadata" {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, "{}")
		}
	})
	reader := client.NewStreamReader("SomeStream")
	m, err := reader.MetaData()
	c.Assert(typeOf(err), Equals, "ErrNotFound")
	c.Assert(m, IsNil)
}

func (s *StreamReaderSuite) TestGetMetaDataReturnsErrTemporarilyUnavailableWhenGettingEvent(c *C) {
	stream := "SomeStream"
	path := fmt.Sprintf("/streams/%s", stream)
	feedURL := fmt.Sprintf("%s%s", server.URL, path)
	es := CreateTestEvents(1, stream, server.URL, "EventTypeX")

	f, _ := CreateTestFeed(es, feedURL)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.String() == path+"/0/forward/1" {
			fmt.Fprintf(w, f.PrettyPrint())
		} else if r.URL.String() == "/streams/SomeStream/metadata" {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, "{}")
		}
	})
	reader := client.NewStreamReader("SomeStream")
	m, err := reader.MetaData()
	c.Assert(typeOf(err), Equals, "ErrTemporarilyUnavailable")
	c.Assert(m, IsNil)
}
