package goes

import (
	"encoding/json"
	"fmt"
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

func (s *SimSuite) TestGetFeedWithNoEntries(c *C) {

}

func (s *SimSuite) TestGetSliceSectionForwardFromZero(c *C) {
	es := CreateTestEvents(1000, "x", "x", "x")

	sl, isF, isL := getSliceSection(es, &StreamVersion{Number: 0}, 100, "forward")

	c.Assert(sl, HasLen, 100)
	c.Assert(isF, Equals, false)
	c.Assert(isL, Equals, true)
	c.Assert(sl[0].EventNumber, Equals, 0)
	c.Assert(sl[len(sl)-1].EventNumber, Equals, 99)
}

//Testing a slice from the middle of the strem not exceeding any bounds.
func (su *SimSuite) TestGetSliceSectionForward(c *C) {
	es := CreateTestEvents(100, "x", "x", "x")

	s, isF, isL := getSliceSection(es, &StreamVersion{Number: 25}, 50, "forward")

	c.Assert(s, HasLen, 50)
	c.Assert(isF, Equals, false)
	c.Assert(isL, Equals, false)
	c.Assert(s[0].EventNumber, Equals, 25)
	c.Assert(s[len(s)-1].EventNumber, Equals, 74)
}

//Testing a slice from the middle of the stream not exceeding any bounds
func (su *SimSuite) TestGetSliceSectionBackward(c *C) {
	es := CreateTestEvents(100, "x", "x", "x")

	s, isF, isL := getSliceSection(es, &StreamVersion{Number: 75}, 50, "backward")

	c.Assert(s, HasLen, 50)
	c.Assert(isF, Equals, false)
	c.Assert(isL, Equals, false)
	c.Assert(s[0].EventNumber, Equals, 26)
	c.Assert(s[len(s)-1].EventNumber, Equals, 75)
}

//Version number is in range, but page number means the set will exceed
//the number of events in the stream.
func (su *SimSuite) TestGetSliceSectionBackwardUnder(c *C) {
	es := CreateTestEvents(100, "x", "x", "x")

	s, isF, isL := getSliceSection(es, &StreamVersion{Number: 25}, 50, "backward")

	c.Assert(s, HasLen, 26)
	c.Assert(isF, Equals, false)
	c.Assert(isL, Equals, true)
	c.Assert(s[0].EventNumber, Equals, 0)
	c.Assert(s[len(s)-1].EventNumber, Equals, 25)
}

//Testing the case where the version may be over the
//size of the highest version. This will happen when
//polling the head of the stream waiting for changes
func (su *SimSuite) TestGetSliceSectionForwardOut(c *C) {
	es := CreateTestEvents(100, "x", "x", "x")

	s, isF, isL := getSliceSection(es, &StreamVersion{Number: 101}, 50, "forward")

	c.Assert(s, HasLen, 0)
	c.Assert(isF, Equals, true)
	c.Assert(isL, Equals, false)
}

//Version number in range, but page size means full set will exceed
//the head of the stream
func (su *SimSuite) TestGetSliceSectionForwardOver(c *C) {
	es := CreateTestEvents(100, "x", "x", "x")

	s, isF, isL := getSliceSection(es, &StreamVersion{Number: 75}, 50, "forward")
	c.Assert(s, HasLen, 25)
	c.Assert(isF, Equals, true)
	c.Assert(isL, Equals, false)
	c.Assert(s[0].EventNumber, Equals, 75)
	c.Assert(s[len(s)-1].EventNumber, Equals, 99)
}

func (su *SimSuite) TestGetSliceSectionTail(c *C) {
	es := CreateTestEvents(100, "x", "x", "x")

	s, isF, isL := getSliceSection(es, &StreamVersion{Number: 100}, 20, "forward")

	c.Assert(s, HasLen, 0)
	c.Assert(isF, Equals, true)
	c.Assert(isL, Equals, false)
}

func (su *SimSuite) TestGetSliceSectionAllForward(c *C) {
	es := CreateTestEvents(100, "x", "x", "x")

	s, isF, isL := getSliceSection(es, nil, 100, "forward")

	c.Assert(s, HasLen, 100)
	c.Assert(isF, Equals, true)
	c.Assert(isL, Equals, true)
	c.Assert(s[0].EventNumber, Equals, 0)
	c.Assert(s[len(s)-1].EventNumber, Equals, 99)
}

func (s *SimSuite) TestParseURLVersioned(c *C) {
	srv := "http://localhost:2113"
	stream := "An-Qw3334rd-St333"
	ver := &StreamVersion{Number: 50}
	direction := "backward"
	pageSize := 10

	url := fmt.Sprintf("%s/streams/%s/%d/%s/%d", srv, stream, ver.Number, direction, pageSize)

	er, err := parseURL(url)

	c.Assert(err, IsNil)
	c.Assert(er.Host, Equals, srv)
	c.Assert(er.Stream, Equals, stream)
	c.Assert(er.Version.Number, Equals, ver.Number)
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
	c.Assert(er.Version, IsNil)
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
	c.Assert(er.Version, IsNil)
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
