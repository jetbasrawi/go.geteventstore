package goes

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"time"

	. "gopkg.in/check.v1"
)

var _ = Suite(&SimSuite{})

type SimSuite struct{}

func (s *SimSuite) SetUpTest(c *C) {
	setup()
}

func (s *SimSuite) TearDownTest(c *C) {
	teardown()
}

// Test that an attempt to construct a simulator with no events returns an error
func (s *SimSuite) TestCreateSimulatorWithNoEventsReturnsError(c *C) {
	stream := "noevents-stream"
	es := CreateTestEvents(0, stream, server.URL, "EventTypeY")

	handler, err := NewAtomFeedSimulator(es, nil, nil, 0)

	c.Assert(err, NotNil)
	c.Assert(err, DeepEquals, errors.New("Must provide one or more events."))
	c.Assert(handler, IsNil)
}

func (s *SimSuite) TestGetEventResponse(c *C) {
	stream := "astream-54"
	es := CreateTestEvents(1, stream, server.URL, "EventTypeA")
	e := es[0]

	b, err := json.Marshal(e)
	raw := json.RawMessage(b)

	timeStr := Time(time.Now())

	want := &eventAtomResponse{
		Title:   fmt.Sprintf("%d@%s", e.EventNumber, stream),
		ID:      e.Links[0].URI,
		Updated: timeStr,
		Summary: e.EventType,
		Content: &raw,
	}

	got, err := CreateTestEventAtomResponse(e, &timeStr)
	c.Assert(err, IsNil)
	c.Assert(got, DeepEquals, want)
}

func (s *SimSuite) TestResolveEvent(c *C) {
	stream := "astream5"
	es := CreateTestEvents(100, stream, server.URL, "EventTypeX")
	eu := fmt.Sprintf("%s/streams/%s/%d/", server.URL, stream, 9)

	got, err := resolveEvent(es, eu)

	c.Assert(err, IsNil)
	c.Assert(got, DeepEquals, es[9])
}

func (s *SimSuite) TestGetSliceSectionForwardFromZero(c *C) {
	es := CreateTestEvents(15, "x", "x", "x")

	sl, isF, isL, isH := getSliceSection(es, 0, 10, "forward")

	c.Assert(sl, HasLen, 10)
	c.Assert(isF, Equals, false)
	c.Assert(isL, Equals, true)
	c.Assert(isH, Equals, false)
	c.Assert(sl[0].EventNumber, Equals, 0)
	c.Assert(sl[len(sl)-1].EventNumber, Equals, 9)
}

//Testing a slice from the middle of the strem not exceeding any bounds.
func (s *SimSuite) TestGetSliceSectionForward(c *C) {
	es := CreateTestEvents(100, "x", "x", "x")

	se, isF, isL, isH := getSliceSection(es, 25, 50, "forward")

	c.Assert(se, HasLen, 50)
	c.Assert(isF, Equals, false)
	c.Assert(isL, Equals, false)
	c.Assert(isH, Equals, false)

	c.Assert(se[0].EventNumber, Equals, 25)
	c.Assert(se[len(se)-1].EventNumber, Equals, 74)
}

//Testing a slice from the middle of the stream not exceeding any bounds
func (s *SimSuite) TestGetSliceSectionBackward(c *C) {
	es := CreateTestEvents(100, "x", "x", "x")

	se, isF, isL, isH := getSliceSection(es, 75, 50, "backward")

	c.Assert(se, HasLen, 50)
	c.Assert(isF, Equals, false)
	c.Assert(isL, Equals, false)
	c.Assert(isH, Equals, false)
	c.Assert(se[0].EventNumber, Equals, 26)
	c.Assert(se[len(se)-1].EventNumber, Equals, 75)
}

//Version number is in range, but page number means the set will exceed
//the number of events in the stream.
func (s *SimSuite) TestGetSliceSectionBackwardUnder(c *C) {
	es := CreateTestEvents(100, "x", "x", "x")

	se, isF, isL, isH := getSliceSection(es, 25, 50, "backward")

	c.Assert(se, HasLen, 26)
	c.Assert(isF, Equals, false)
	c.Assert(isL, Equals, true)
	c.Assert(isH, Equals, false)
	c.Assert(se[0].EventNumber, Equals, 0)
	c.Assert(se[len(se)-1].EventNumber, Equals, 25)
}

//Testing the case where the version may be over the
//size of the highest version. This will happen when
//polling the head of the stream waiting for changes
func (s *SimSuite) TestGetSliceSectionForwardOut(c *C) {
	es := CreateTestEvents(100, "x", "x", "x")

	se, isF, isL, isH := getSliceSection(es, 101, 50, "forward")

	c.Assert(se, HasLen, 0)
	c.Assert(isF, Equals, true)
	c.Assert(isL, Equals, false)
	c.Assert(isH, Equals, true)
}

// Version number is in range but version plus pagesize is greter the the highest
// event number and so the query exeeds the number of results that can be returned
func (s *SimSuite) TestGetSliceSectionForwardOver(c *C) {
	es := CreateTestEvents(100, "x", "x", "x")

	se, isF, isL, isH := getSliceSection(es, 75, 50, "forward")
	c.Assert(se, HasLen, 25)
	c.Assert(isF, Equals, true)
	c.Assert(isL, Equals, false)
	c.Assert(isH, Equals, true)
	c.Assert(se[0].EventNumber, Equals, 75)
	c.Assert(se[len(se)-1].EventNumber, Equals, 99)
}

// This test covers the case where the version is higher than the highest version
func (s *SimSuite) TestGetSliceSectionTail(c *C) {
	es := CreateTestEvents(100, "x", "x", "x")

	se, isF, isL, isH := getSliceSection(es, 100, 20, "forward")

	c.Assert(se, HasLen, 0)
	c.Assert(isF, Equals, true)
	c.Assert(isL, Equals, false)
	c.Assert(isH, Equals, true)
}

func (s *SimSuite) TestGetSliceSectionAllForward(c *C) {
	es := CreateTestEvents(100, "x", "x", "x")

	se, isF, isL, isH := getSliceSection(es, 0, 100, "forward")

	c.Assert(se, HasLen, 100)
	c.Assert(isF, Equals, true)
	c.Assert(isL, Equals, true)
	c.Assert(isH, Equals, true)
	c.Assert(se[0].EventNumber, Equals, 0)
	c.Assert(se[len(se)-1].EventNumber, Equals, 99)
}

func (s *SimSuite) TestParseURLVersioned(c *C) {
	srv := "http://localhost:2113"
	stream := "An-Qw3334rd-St333"
	ver := 50
	direction := "backward"
	pageSize := 10

	url := fmt.Sprintf("%s/streams/%s/%d/%s/%d", srv, stream, ver, direction, pageSize)

	er, err := parseURL(url)

	c.Assert(err, IsNil)
	c.Assert(er.Host, Equals, srv)
	c.Assert(er.Stream, Equals, stream)
	c.Assert(er.Version, Equals, ver)
	c.Assert(er.Direction, Equals, direction)
	c.Assert(er.PageSize, Equals, pageSize)
}

func (s *SimSuite) TestParseURLInvalidVersion(c *C) {
	srv := "http://localhost:2113"
	stream := "An-Qw3334rd-St333"
	pageSize := 20
	direction := "backward"
	version := -1
	url := fmt.Sprintf("%s/streams/%s/%d/%s/%d", srv, stream, version, direction, pageSize)

	_, err := parseURL(url)

	c.Assert(err, FitsTypeOf, invalidVersionError(version))
}

func (s *SimSuite) TestParseURLBase(c *C) {
	srv := "http://localhost:2113"
	stream := "An-Qw3334rd-St333"
	pageSize := 20
	direction := "backward"

	url := fmt.Sprintf("%s/streams/%s", srv, stream)

	er, err := parseURL(url)

	c.Assert(err, IsNil)
	c.Assert(er.Host, Equals, srv)
	c.Assert(er.Stream, Equals, stream)
	c.Assert(er.Version, Equals, 0)
	c.Assert(er.Direction, Equals, direction)
	c.Assert(er.PageSize, Equals, pageSize)
}

func (s *SimSuite) TestParseURLHead(c *C) {
	srv := "http://localhost:2113"
	stream := "An-Qw3334rd-St333"
	direction := "backward"
	pageSize := 100

	url := fmt.Sprintf("%s/streams/%s/%s/%s/%d", srv, stream, "head", direction, pageSize)

	er, err := parseURL(url)

	c.Assert(err, IsNil)
	c.Assert(er.Host, Equals, srv)
	c.Assert(er.Stream, Equals, stream)
	c.Assert(er.Version, Equals, 0)
	c.Assert(er.Direction, Equals, direction)
	c.Assert(er.PageSize, Equals, pageSize)
}

func (s *SimSuite) TestCreateFeedLinksBackward(c *C) {
	stream := "astream"
	ver := 50
	url := fmt.Sprintf("%s/streams/%s/%d/backward/20", server.URL, stream, ver)

	selfWant := fmt.Sprintf("%s/streams/%s", server.URL, stream)
	firstWant := fmt.Sprintf("%s/streams/%s/head/backward/20", server.URL, stream)
	lastWant := fmt.Sprintf("%s/streams/%s/0/forward/20", server.URL, stream)
	nextWant := fmt.Sprintf("%s/streams/%s/30/backward/20", server.URL, stream)
	prevWant := fmt.Sprintf("%s/streams/%s/51/forward/20", server.URL, stream)
	metaWant := fmt.Sprintf("%s/streams/%s/metadata", server.URL, stream)

	es := CreateTestEvents(100, stream, server.URL, "EventTypeX")
	m, _ := CreateTestFeed(es, url)

	var self, first, next, last, prev, meta bool
	for _, v := range m.Link {

		switch v.Rel {
		case "self":
			self = true
			c.Assert(v.Href, Equals, selfWant)
		case "first":
			first = true
			c.Assert(v.Href, Equals, firstWant)
		case "next":
			next = true
			c.Assert(v.Href, Equals, nextWant)
		case "last":
			last = true
			c.Assert(v.Href, Equals, lastWant)
		case "previous":
			prev = true
			c.Assert(v.Href, Equals, prevWant)
		case "metadata":
			meta = true
			c.Assert(v.Href, Equals, metaWant)
		}
	}

	c.Assert(self, Equals, true)
	c.Assert(first, Equals, true)
	c.Assert(next, Equals, true)
	c.Assert(last, Equals, true)
	c.Assert(prev, Equals, true)
	c.Assert(meta, Equals, true)
}

func (s *SimSuite) TestCreateFeedLinksLast(c *C) {
	stream := "astream"
	url := fmt.Sprintf("%s/streams/%s/0/forward/20", server.URL, stream)

	selfWant := fmt.Sprintf("%s/streams/%s", server.URL, stream)
	firstWant := fmt.Sprintf("%s/streams/%s/head/backward/20", server.URL, stream)
	prevWant := fmt.Sprintf("%s/streams/%s/20/forward/20", server.URL, stream)
	metaWant := fmt.Sprintf("%s/streams/%s/metadata", server.URL, stream)

	es := CreateTestEvents(100, stream, server.URL, "EventTypeX")
	m, _ := CreateTestFeed(es, url)

	var self, first, next, last, prev, meta bool
	for _, v := range m.Link {

		switch v.Rel {
		case "self":
			self = true
			c.Assert(v.Href, Equals, selfWant)
		case "first":
			first = true
			c.Assert(v.Href, Equals, firstWant)
		case "next":
			next = true
		case "last":
			last = true
		case "previous":
			prev = true
			c.Assert(v.Href, Equals, prevWant)
		case "metadata":
			meta = true
			c.Assert(v.Href, Equals, metaWant)
		}
	}

	c.Assert(self, Equals, true)
	c.Assert(first, Equals, true)
	c.Assert(next, Equals, false)
	c.Assert(last, Equals, false)
	c.Assert(prev, Equals, true)
	c.Assert(meta, Equals, true)
}

func (s *SimSuite) TestCreateFeedLinksTail(c *C) {
	stream := "astream"
	url := fmt.Sprintf("%s/streams/%s/100/forward/20", server.URL, stream)

	selfWant := fmt.Sprintf("%s/streams/%s", server.URL, stream)
	firstWant := fmt.Sprintf("%s/streams/%s/head/backward/20", server.URL, stream)
	lastWant := fmt.Sprintf("%s/streams/%s/0/forward/20", server.URL, stream)
	nextWant := fmt.Sprintf("%s/streams/%s/99/backward/20", server.URL, stream)
	metaWant := fmt.Sprintf("%s/streams/%s/metadata", server.URL, stream)

	es := CreateTestEvents(100, stream, server.URL, "EventTypeX")
	m, _ := CreateTestFeed(es, url)

	var self, first, next, last, prev, meta bool
	for _, v := range m.Link {

		switch v.Rel {
		case "self":
			self = true
			c.Assert(v.Href, Equals, selfWant)
		case "first":
			first = true
			c.Assert(v.Href, Equals, firstWant)
		case "next":
			next = true
			c.Assert(v.Href, Equals, nextWant)
		case "last":
			last = true
			c.Assert(v.Href, Equals, lastWant)
		case "previous":
			prev = true
		case "metadata":
			meta = true
			c.Assert(v.Href, Equals, metaWant)
		}
	}

	c.Assert(self, Equals, true)
	c.Assert(first, Equals, true)
	c.Assert(next, Equals, true)
	c.Assert(last, Equals, true)
	c.Assert(prev, Equals, false)
	c.Assert(meta, Equals, true)
}

func (s *SimSuite) TestCreateFeedLinksHead(c *C) {
	stream := "astream"
	url := fmt.Sprintf("%s/streams/%s/head/backward/20", server.URL, stream)

	selfWant := fmt.Sprintf("%s/streams/%s", server.URL, stream)
	firstWant := fmt.Sprintf("%s/streams/%s/head/backward/20", server.URL, stream)
	lastWant := fmt.Sprintf("%s/streams/%s/0/forward/20", server.URL, stream)
	nextWant := fmt.Sprintf("%s/streams/%s/79/backward/20", server.URL, stream)
	prevWant := fmt.Sprintf("%s/streams/%s/100/forward/20", server.URL, stream)
	metaWant := fmt.Sprintf("%s/streams/%s/metadata", server.URL, stream)

	es := CreateTestEvents(100, stream, server.URL, "EventTypeX")
	m, _ := CreateTestFeed(es, url)

	var self, first, next, last, prev, meta bool
	for _, v := range m.Link {
		switch v.Rel {
		case "self":
			self = true
			c.Assert(v.Href, Equals, selfWant)
		case "first":
			first = true
			c.Assert(v.Href, Equals, firstWant)
		case "next":
			next = true
			c.Assert(v.Href, Equals, nextWant)
		case "last":
			last = true
			c.Assert(v.Href, Equals, lastWant)
		case "previous":
			prev = true
			c.Assert(v.Href, Equals, prevWant)
		case "metadata":
			meta = true
			c.Assert(v.Href, Equals, metaWant)
		}
	}

	c.Assert(self, Equals, true)
	c.Assert(first, Equals, true)
	c.Assert(next, Equals, true)
	c.Assert(last, Equals, true)
	c.Assert(prev, Equals, true)
	c.Assert(meta, Equals, true)

}

func (s *SimSuite) TestCreateFeedEntriesLast(c *C) {
	stream := "astream"
	url := fmt.Sprintf("%s/streams/%s/0/forward/20", server.URL, stream)
	es := CreateTestEvents(100, stream, server.URL, "EventTypeX")
	m, _ := CreateTestFeed(es, url)

	c.Assert(m.Entry, HasLen, 20)
	for k, v := range m.Entry {
		num := (20 - 1) - k
		ti := fmt.Sprintf("%d@%s", num, stream)
		c.Assert(v.Title, Equals, ti)
	}
}

func (s *SimSuite) TestCreateFeedEntries(c *C) {
	stream := "astream"
	url := fmt.Sprintf("%s/streams/%s/20/forward/20", server.URL, stream)
	es := CreateTestEvents(100, stream, server.URL, "EventTypeX")
	m, _ := CreateTestFeed(es, url)

	c.Assert(m.Entry, HasLen, 20)
	for k, v := range m.Entry {
		num := (40 - 1) - k
		ti := fmt.Sprintf("%d@%s", num, stream)
		c.Assert(v.Title, Equals, ti)
	}
}

func (s *SimSuite) TestCreateFeedEntriesTail(c *C) {
	stream := "astream"
	url := fmt.Sprintf("%s/streams/%s/100/forward/20", server.URL, stream)
	es := CreateTestEvents(100, stream, server.URL, "EventTypeX")
	m, _ := CreateTestFeed(es, url)
	c.Assert(m.Entry, HasLen, 0)
}

func (s *SimSuite) TestCreateFeedEntriesHead(c *C) {
	stream := "astream"
	url := fmt.Sprintf("%s/streams/%s/head/backward/20", server.URL, stream)
	es := CreateTestEvents(100, stream, server.URL, "EventTypeX")
	m, _ := CreateTestFeed(es, url)

	c.Assert(m.Entry, HasLen, 20)
	for k, v := range m.Entry {
		num := (len(es) - 1) - k
		ti := fmt.Sprintf("%d@%s", num, stream)
		c.Assert(v.Title, Equals, ti)
	}
}

func (s *SimSuite) TestCreateEvents(c *C) {
	es := CreateTestEvents(100, "astream", server.URL, "EventTypeX")

	c.Assert(es, HasLen, 100)
	for i := 0; i <= 99; i++ {
		c.Assert(es[i].EventNumber, Equals, i)
	}
}

func (s *SimSuite) TestReverseSlice(c *C) {
	es := CreateTestEvents(100, "astream", server.URL, "EventTypeX")
	rs := reverseEventSlice(es)

	c.Assert(rs, HasLen, 100)
	top := len(es) - 1
	for i := 0; i <= top; i++ {
		c.Assert(rs[i].EventNumber, Equals, top-i)
	}
}

func (s *SimSuite) TestHeadOfStreamSetTrueWhenAtHeadOfStream(c *C) {
	stream := "astream"
	url := fmt.Sprintf("%s/streams/%s/90/forward/20", server.URL, stream)
	es := CreateTestEvents(100, stream, server.URL, "EventTypeX")
	m, _ := CreateTestFeed(es, url)

	c.Assert(m.HeadOfStream, Equals, true)
}

// If the result is the first page but has not exceeded the number of events
// the stream is not at the head of the stream. Only when the query exceeds the
// number of results is the reader at the head of the stream
func (s *SimSuite) TestHeadOfStreamSetFalseWhenNotAtHeadOfStream(c *C) {
	stream := "astream"
	url := fmt.Sprintf("%s/streams/%s/79/forward/20", server.URL, stream)
	es := CreateTestEvents(100, stream, server.URL, "EventTypeX")
	m, _ := CreateTestFeed(es, url)

	c.Assert(m.HeadOfStream, Equals, false)
}

func (s *SimSuite) TestSetStreamID(c *C) {
	stream := "some-stream"
	url := fmt.Sprintf("%s/streams/%s/90/forward/20", server.URL, stream)
	es := CreateTestEvents(100, stream, server.URL, "EventTypeX")
	m, _ := CreateTestFeed(es, url)

	c.Assert(m.StreamID, Equals, stream)
}

func (s *SimSuite) TestTrickleFeed(c *C) {

	stream := "trickle-stream"
	es := CreateTestEvents(6, stream, server.URL, "EventTypeX")

	u, _ := url.Parse(server.URL)
	handler, err := NewAtomFeedSimulator(es, u, nil, 5)
	if err != nil {
		log.Fatal(err)
	}
	mux.Handle("/", handler)

	reader := client.NewStreamReader(stream)

	count := 0
	for reader.Next() {
		if count > len(es)-1 {
			return
		}

		if count < 5 {
			c.Assert(reader.Err(), Equals, nil)
			c.Assert(reader.EventResponse, NotNil)
		} else if count == 5 {
			c.Assert(typeOf(reader.Err()), Equals, "NoMoreEventsError")
			reader.LongPoll(1)
		} else if count > 5 && count < 10 {
			c.Assert(reader.Err(), Equals, nil)
			c.Assert(reader.EventResponse, NotNil)
		}
		count++
	}
}
