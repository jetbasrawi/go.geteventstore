package goes

import (
	"encoding/json"
	"fmt"
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

func (s *StreamReaderSuite) TestNextReturnsErrorIfStreamDoesNotExist(c *C) {
	streamName := "foostream"
	path := fmt.Sprintf("/streams/%s/0/forward/100", streamName)

	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "")
	})

	stream := client.NewStreamReader(streamName)
	ok := stream.Next()
	c.Assert(ok, Equals, true)
	c.Assert(stream.Err(), DeepEquals, &StreamDoesNotExistError{})

}

func (s *StreamReaderSuite) TestNextReturnsErrorIfUserIsNotAuthorisedToAccessStream(c *C) {
	streamName := "SomeStream"
	path := fmt.Sprintf("/streams/%s/0/forward/20", streamName)

	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "")
	})

	stream := client.NewStreamReader(streamName)
	ok := stream.Next()
	c.Assert(ok, Equals, true)
	c.Assert(stream.Err(), Equals, &UnauthorizedError{})
}

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

func (s *StreamReaderSuite) TestNextReturnsErrorIfThereIsNoNextEventToReturn(c *C) {
	streamName := "SomeStream"
	ne := 1
	es := CreateTestEvents(ne, streamName, server.URL, "FooEvent")
	setupSimulator(es, nil)
	want := &NoMoreEventsError{}

	stream := client.NewStreamReader(streamName)
	_ = stream.Next()

	_ = stream.Next()
	c.Assert(stream.Err(), DeepEquals, want)
	c.Assert(stream.EventResponse(), IsNil)
}

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

func (s *StreamReaderSuite) TestFeedWithFewerEntriesThanThePageSize(c *C) {

	streamName := "SomeStream"
	ne := 25
	es := CreateTestEvents(ne, streamName, server.URL, "FooEvent")

	setupSimulator(es, nil)

	stream := client.NewStreamReader(streamName)

	count := 0

	for stream.Next() {

		switch err := stream.Err().(type) {
		case *NoMoreEventsError:
			c.Assert(err, NotNil)
			c.Assert(count, Equals, ne)
			return
		}

		//fmt.Printf("\n\nCount: %d\n", count)
		//spew.Dump(stream.EventResponse())

		c.Assert(stream.EventResponse(), NotNil)
		c.Assert(stream.Version(), Equals, count)
		count++
	}

}

//func (s *StreamSuite) TestGetMetaReturnsNilWhenStreamMetaDataIsEmpty(c *C) {
//stream := "Some-Stream"
//es := CreateTestEvents(10, stream, server.URL, "EventTypeX")
//setupSimulator(es, nil)

//streamReader := client.Dial(stream)
//got, resp, err := streamReader.MetaData()

//c.Assert(err, IsNil)
//c.Assert(got, IsNil)
//c.Assert(resp.StatusCode, Equals, http.StatusOK)
//}

//func (s *StreamSuite) TestGetMetaData(c *C) {
//d := fmt.Sprintf("{ \"foo\" : %d }", rand.Intn(9999))
//raw := json.RawMessage(d)
//stream := "Some-Stream"
//es := CreateTestEvents(10, stream, server.URL, "EventTypeX")
//m := CreateTestEvent(stream, server.URL, "metadata", 10, &raw, nil)
//want := CreateTestEventResponse(m, nil)
//setupSimulator(es, m)

//got, _, _ := client.GetStreamMetaData(stream)

//c.Assert(got.PrettyPrint(), Equals, want.PrettyPrint())
//}
