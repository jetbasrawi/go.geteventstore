package goes

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestGetEventResponse(t *testing.T) {
	setup()
	defer teardown()

	stream := "astream-54"
	es := createTestEvents(1, stream, server.URL, "EventTypeA")
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

	got, err := createTestEventAtomResponse(e, &timeStr)
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("Got\n  %+v\n Want\n %+v\n", got, want)
	}

}

func TestResolveEvent(t *testing.T) {
	setup()
	defer teardown()

	stream := "astream5"
	es := createTestEvents(100, stream, server.URL, "EventTypeX")
	eu := fmt.Sprintf("%s/streams/%s/%d/", server.URL, stream, 9)

	got, err := resolveEvent(es, eu)
	if err != nil {
		t.Errorf("Unexpected Error %s", err)
	}

	want := es[9]

	if !reflect.DeepEqual(got, want) {
		t.Errorf("Got %+v\n Want %+v\n", got, want)
	}

}

func TestGetSliceSectionForwardFromZero(t *testing.T) {
	es := createTestEvents(1000, "x", "x", "x")

	s, isF, isL := getSliceSection(es, &StreamVersion{Number: 0}, 100, "forward")
	if len(s) != 100 {
		t.Errorf("Got: %d Want: %d", len(s), 100)
	}
	if isF || !isL {
		t.Errorf("isF: %t isL: %t", isF, isL)
	}
	if s[0].EventNumber != 0 {
		t.Errorf("Got %d, want %d", s[0].EventNumber, 0)
	}
	if s[len(s)-1].EventNumber != 99 {
		t.Errorf("Got %d, want %d", s[len(s)-1].EventNumber, 99)
	}

}

//Testing a slice from the middle of the strem not exceeding any bounds.
func TestGetSliceSectionForward(t *testing.T) {
	es := createTestEvents(100, "x", "x", "x")

	s, isF, isL := getSliceSection(es, &StreamVersion{Number: 25}, 50, "forward")
	if len(s) != 50 {
		t.Errorf("Got: %d Want: %d", len(s), 50)
	}
	if isF || isL {
		t.Errorf("isF: %t isL: %t", isF, isL)
	}
	if s[0].EventNumber != 25 {
		t.Errorf("Got %d, want %d", s[0].EventNumber, 25)
	}
	if s[len(s)-1].EventNumber != 74 {
		t.Errorf("Got %d, want %d", s[len(s)-1].EventNumber, 74)
	}

}

//Testing a slice from the middle of the stream not exceeding any bounds
func TestGetSliceSectionBackward(t *testing.T) {
	es := createTestEvents(100, "x", "x", "x")

	s, isF, isL := getSliceSection(es, &StreamVersion{Number: 75}, 50, "backward")
	if len(s) != 50 {
		t.Errorf("Got: %d Want: %d", len(s), 50)
	}
	if isF || isL {
		t.Errorf("isF: %t isL: %t", isF, isL)
	}
	if s[0].EventNumber != 26 {
		t.Errorf("Got %d, want %d", s[0].EventNumber, 26)
	}
	if s[len(s)-1].EventNumber != 75 {
		t.Errorf("Got %d, want %d", s[len(s)-1].EventNumber, 75)
	}

}

//Version number is in range, but page number means the set will exceed
//the number of events in the stream.
func TestGetSliceSectionBackwardUnder(t *testing.T) {
	es := createTestEvents(100, "x", "x", "x")

	s, isF, isL := getSliceSection(es, &StreamVersion{Number: 25}, 50, "backward")
	if len(s) != 26 {
		t.Errorf("Got: %d Want: %d", len(s), 26)
	}
	if isF || !isL {
		t.Errorf("isF: %t isL: %t", isF, isL)
	}
	if s[0].EventNumber != 0 {
		t.Errorf("Got %d, want %d", s[0].EventNumber, 0)
	}
	if s[len(s)-1].EventNumber != 25 {
		t.Errorf("Got %d, want %d", s[len(s)-1].EventNumber, 25)
	}

}

//Testing the case where the version may be over the
//size of the highest version. This will happen when
//polling the head of the stream waiting for changes
func TestGetSliceSectionForwardOut(t *testing.T) {
	es := createTestEvents(100, "x", "x", "x")

	s, isF, isL := getSliceSection(es, &StreamVersion{Number: 101}, 50, "forward")
	if len(s) != 0 {
		t.Errorf("Got: %d Want: %d", len(s), 0)
	}
	if !isF || isL {
		t.Errorf("isF: %t isL: %t", isF, isL)
	}
}

//Version number in range, but page size means full set will exceed
//the head of the stream
func TestGetSliceSectionForwardOver(t *testing.T) {
	es := createTestEvents(100, "x", "x", "x")

	s, isF, isL := getSliceSection(es, &StreamVersion{Number: 75}, 50, "forward")
	if len(s) != 25 {
		t.Errorf("Got: %d Want: %d", len(s), 25)
	}
	if !isF || isL {
		t.Errorf("isF: %t isL: %t", isF, isL)
	}
	if s[0].EventNumber != 75 {
		t.Errorf("Got %d, want %d", s[0].EventNumber, 75)
	}
	if s[len(s)-1].EventNumber != 99 {
		t.Errorf("Got %d, want %d", s[len(s)-1].EventNumber, 99)
	}

}

func TestGetSliceSectionTail(t *testing.T) {
	es := createTestEvents(100, "x", "x", "x")

	s, isF, isL := getSliceSection(es, &StreamVersion{Number: 100}, 20, "forward")
	if len(s) != 0 {
		t.Errorf("Got: %d Want: %d", len(s), 0)
	}
	if !isF || isL {
		t.Errorf("isF: %t isL: %t", isF, isL)
	}
}

func TestGetSliceSectionAllForward(t *testing.T) {
	es := createTestEvents(100, "x", "x", "x")

	s, isF, isL := getSliceSection(es, nil, 100, "forward")
	if len(s) != 100 {
		t.Errorf("Got: %d Want: %d", len(s), 100)
	}
	if !isF || !isL {
		t.Errorf("isF: %t isL: %t", isF, isL)
	}
	if s[0].EventNumber != 0 {
		t.Errorf("Got %d, want %d", s[0].EventNumber, 0)
	}
	if s[len(s)-1].EventNumber != 99 {
		t.Errorf("Got %d, want %d", s[len(s)-1].EventNumber, 99)
	}
}

func TestParseURLVersioned(t *testing.T) {
	setup()
	defer teardown()

	srv := "http://localhost:2113"
	stream := "An-Qw3334rd-St333"
	ver := &StreamVersion{Number: 50}
	direction := "backward"
	pageSize := 10

	url := fmt.Sprintf("%s/streams/%s/%d/%s/%d", srv, stream, ver.Number, direction, pageSize)

	er, err := parseURL(url)
	if err != nil {
		t.Errorf("Unexpected error %s", err)
	}

	if er.Host != srv {
		t.Errorf("Host incorrect Got: %s Want %s", er.Host, srv)
	}
	if er.Stream != stream {
		t.Errorf("Stream incorrect Got: %s Want %s", er.Stream, stream)
	}
	if er.Version.Number != er.Version.Number {
		t.Errorf("Got: %d, Want: %d", er.Version.Number, ver.Number)
	}
	if er.Direction != direction {
		t.Errorf("Direction incorrect Got: %s Want %s", er.Direction, direction)
	}
	if er.PageSize != pageSize {
		t.Errorf("PageSize incorrect Got: %d Want %d", er.PageSize, pageSize)
	}
}

func TestParseURLInvalidVersion(t *testing.T) {
	setup()
	defer teardown()

	srv := "http://localhost:2113"
	stream := "An-Qw3334rd-St333"
	pageSize := 20
	direction := "backward"
	version := -1
	url := fmt.Sprintf("%s/streams/%s/%d/%s/%d", srv, stream, version, direction, pageSize)

	_, err := parseURL(url)
	if serr, ok := err.(invalidVersionError); !ok {
		want := invalidVersionError(version)
		t.Errorf("Exected Invalid version error:\n Got %#v\n Want %#v\n", serr, want)
	}
}

func TestParseURLBase(t *testing.T) {
	setup()
	defer teardown()

	srv := "http://localhost:2113"
	stream := "An-Qw3334rd-St333"
	pageSize := 20
	direction := "backward"

	url := fmt.Sprintf("%s/streams/%s", srv, stream)

	er, err := parseURL(url)
	if err != nil {
		t.Errorf("Unexpected error %s", err)
	}

	if er.Host != srv {
		t.Errorf("Host incorrect Got: %s Want %s", er.Host, srv)
	}
	if er.Stream != stream {
		t.Errorf("Stream incorrect Got: %s Want %s", er.Stream, stream)
	}
	if er.Version != nil {
		t.Errorf("Version incorrect. Expected Nil")
	}
	if er.Direction != direction {
		t.Errorf("Direction incorrect Got: %s Want %s", er.Direction, direction)
	}
	if er.PageSize != pageSize {
		t.Errorf("PageSize incorrect Got: %d Want %d", er.PageSize, pageSize)
	}
}

func TestParseURLHead(t *testing.T) {
	setup()
	defer teardown()

	srv := "http://localhost:2113"
	stream := "An-Qw3334rd-St333"
	direction := "backward"
	pageSize := 100

	url := fmt.Sprintf("%s/streams/%s/%s/%s/%d", srv, stream, "head", direction, pageSize)

	er, err := parseURL(url)
	if err != nil {
		t.Errorf("Unexpected error %s", err)
	}

	if er.Host != srv {
		t.Errorf("Host incorrect Got: %s Want %s", er.Host, srv)
	}
	if er.Stream != stream {
		t.Errorf("Stream incorrect Got: %s Want %s", er.Stream, stream)
	}
	if er.Version != nil {
		t.Errorf("Version incorrect. Expected Nil")
	}
	if er.Direction != direction {
		t.Errorf("Direction incorrect Got: %s Want %s", er.Direction, direction)
	}
	if er.PageSize != pageSize {
		t.Errorf("PageSize incorrect Got: %d Want %d", er.PageSize, pageSize)
	}
}

func TestCreateFeedLinksBackward(t *testing.T) {
	setup()
	defer teardown()

	stream := "astream"
	ver := 50
	url := fmt.Sprintf("%s/streams/%s/%d/backward/20", server.URL, stream, ver)

	selfWant := fmt.Sprintf("%s/streams/%s", server.URL, stream)
	firstWant := fmt.Sprintf("%s/streams/%s/head/backward/20", server.URL, stream)
	lastWant := fmt.Sprintf("%s/streams/%s/0/forward/20", server.URL, stream)
	nextWant := fmt.Sprintf("%s/streams/%s/30/backward/20", server.URL, stream)
	prevWant := fmt.Sprintf("%s/streams/%s/51/forward/20", server.URL, stream)
	metaWant := fmt.Sprintf("%s/streams/%s/metadata", server.URL, stream)

	es := createTestEvents(100, stream, server.URL, "EventTypeX")
	m, _ := createTestFeed(es, url)

	var self, first, next, last, prev, meta bool
	for _, v := range m.Link {

		switch v.Rel {
		case "self":
			self = true
			if v.Href != selfWant {
				t.Errorf("Self Got %s Want %s", v.Href, selfWant)
			}
		case "first":
			first = true
			if v.Href != firstWant {
				t.Errorf("First Got %s Want %s", v.Href, firstWant)
			}

		case "next":
			next = true
			if v.Href != nextWant {
				t.Errorf("Next Got %s Want %s", v.Href, nextWant)
			}
		case "last":
			last = true
			if v.Href != lastWant {
				t.Errorf("Last Got %s Want %s", v.Href, lastWant)
			}
		case "previous":
			prev = true
			if v.Href != prevWant {
				t.Errorf("Prev Got %s Want %s", v.Href, prevWant)
			}
		case "metadata":
			meta = true
			if v.Href != metaWant {
				t.Errorf("Meta Got %s Want %s", v.Href, metaWant)
			}
		}
	}

	if !self || !first || !next || !last || !prev || !meta {
		t.Errorf("self: %+v\n first: %+v\n next: %+v\n last: %+v\n prev: %+v\n meta: %+v\n", self, first, next, last, prev, meta)
	}

}

func TestCreateFeedLinksLast(t *testing.T) {
	setup()
	defer teardown()

	stream := "astream"
	url := fmt.Sprintf("%s/streams/%s/0/forward/20", server.URL, stream)

	selfWant := fmt.Sprintf("%s/streams/%s", server.URL, stream)
	firstWant := fmt.Sprintf("%s/streams/%s/head/backward/20", server.URL, stream)
	prevWant := fmt.Sprintf("%s/streams/%s/20/forward/20", server.URL, stream)
	metaWant := fmt.Sprintf("%s/streams/%s/metadata", server.URL, stream)

	es := createTestEvents(100, stream, server.URL, "EventTypeX")
	m, _ := createTestFeed(es, url)

	var self, first, next, last, prev, meta bool
	for _, v := range m.Link {

		switch v.Rel {
		case "self":
			self = true
			if v.Href != selfWant {
				t.Errorf("Self Got %s Want %s", v.Href, selfWant)
			}
		case "first":
			first = true
			if v.Href != firstWant {
				t.Errorf("First Got %s Want %s", v.Href, firstWant)
			}

		case "next":
			next = true
		case "last":
			last = true
		case "previous":
			prev = true
			if v.Href != prevWant {
				t.Errorf("Prev Got %s Want %s", v.Href, prevWant)
			}
		case "metadata":
			meta = true
			if v.Href != metaWant {
				t.Errorf("Meta Got %s Want %s", v.Href, metaWant)
			}
		}
	}

	if !self || !first || next || last || !prev || !meta {
		t.Errorf("self: %+v\n first: %+v\n next: %+v\n last: %+v\n prev: %+v\n meta: %+v\n", self, first, next, last, prev, meta)
	}
}

func TestCreateFeedLinksTail(t *testing.T) {
	setup()
	defer teardown()

	stream := "astream"
	url := fmt.Sprintf("%s/streams/%s/100/forward/20", server.URL, stream)

	selfWant := fmt.Sprintf("%s/streams/%s", server.URL, stream)
	firstWant := fmt.Sprintf("%s/streams/%s/head/backward/20", server.URL, stream)
	lastWant := fmt.Sprintf("%s/streams/%s/0/forward/20", server.URL, stream)
	nextWant := fmt.Sprintf("%s/streams/%s/99/backward/20", server.URL, stream)
	metaWant := fmt.Sprintf("%s/streams/%s/metadata", server.URL, stream)

	es := createTestEvents(100, stream, server.URL, "EventTypeX")
	m, _ := createTestFeed(es, url)

	var self, first, next, last, prev, meta bool
	for _, v := range m.Link {

		switch v.Rel {
		case "self":
			self = true
			if v.Href != selfWant {
				t.Errorf("Self Got %s Want %s", v.Href, selfWant)
			}
		case "first":
			first = true
			if v.Href != firstWant {
				t.Errorf("First Got %s Want %s", v.Href, firstWant)
			}

		case "next":
			next = true
			if v.Href != nextWant {
				t.Errorf("Next Got %s Want %s", v.Href, nextWant)
			}
		case "last":
			last = true
			if v.Href != lastWant {
				t.Errorf("Last Got %s Want %s", v.Href, lastWant)
			}
		case "previous":
			prev = true
		case "metadata":
			meta = true
			if v.Href != metaWant {
				t.Errorf("Meta Got %s Want %s", v.Href, metaWant)
			}
		}
	}

	if !self || !first || !next || !last || prev || !meta {
		t.Errorf("self: %+v\n first: %+v\n next: %+v\n last: %+v\n prev: %+v\n meta: %+v\n", self, first, next, last, prev, meta)
	}

}

func TestCreateFeedLinksHead(t *testing.T) {
	setup()
	defer teardown()

	stream := "astream"
	url := fmt.Sprintf("%s/streams/%s/head/backward/20", server.URL, stream)

	selfWant := fmt.Sprintf("%s/streams/%s", server.URL, stream)
	firstWant := fmt.Sprintf("%s/streams/%s/head/backward/20", server.URL, stream)
	lastWant := fmt.Sprintf("%s/streams/%s/0/forward/20", server.URL, stream)
	nextWant := fmt.Sprintf("%s/streams/%s/79/backward/20", server.URL, stream)
	prevWant := fmt.Sprintf("%s/streams/%s/100/forward/20", server.URL, stream)
	metaWant := fmt.Sprintf("%s/streams/%s/metadata", server.URL, stream)

	es := createTestEvents(100, stream, server.URL, "EventTypeX")
	m, _ := createTestFeed(es, url)

	var self, first, next, last, prev, meta bool
	for _, v := range m.Link {

		switch v.Rel {
		case "self":
			self = true
			if v.Href != selfWant {
				t.Errorf("Self Got %s Want %s", v.Href, selfWant)
			}
		case "first":
			first = true
			if v.Href != firstWant {
				t.Errorf("First Got %s Want %s", v.Href, firstWant)
			}

		case "next":
			next = true
			if v.Href != nextWant {
				t.Errorf("Next Got %s Want %s", v.Href, nextWant)
			}
		case "last":
			last = true
			if v.Href != lastWant {
				t.Errorf("Last Got %s Want %s", v.Href, lastWant)
			}
		case "previous":
			prev = true
			if v.Href != prevWant {
				t.Errorf("Prev Got %s Want %s", v.Href, prevWant)
			}
		case "metadata":
			meta = true
			if v.Href != metaWant {
				t.Errorf("Meta Got %s Want %s", v.Href, metaWant)
			}

		}
	}

	if !self || !first || !next || !last || !prev || !meta {
		t.Errorf("self: %+v\n first: %+v\n next: %+v\n last: %+v\n prev: %+v\n meta: %+v\n", self, first, next, last, prev, meta)
	}

}

func TestCreateFeedEntriesLast(t *testing.T) {
	setup()
	defer teardown()

	stream := "astream"
	url := fmt.Sprintf("%s/streams/%s/0/forward/20", server.URL, stream)

	es := createTestEvents(100, stream, server.URL, "EventTypeX")
	m, _ := createTestFeed(es, url)

	if len(m.Entry) != 20 {
		t.Errorf("Got %d Want %d", len(m.Entry), 20)
	}

	for k, v := range m.Entry {
		num := (20 - 1) - k
		ti := fmt.Sprintf("%d@%s", num, stream)
		if v.Title != ti {
			t.Errorf("Got: %s Want: %s", v.Title, ti)
		}
	}
}

func TestCreateFeedEntries(t *testing.T) {
	setup()
	defer teardown()

	stream := "astream"
	url := fmt.Sprintf("%s/streams/%s/20/forward/20", server.URL, stream)

	es := createTestEvents(100, stream, server.URL, "EventTypeX")
	m, _ := createTestFeed(es, url)

	if len(m.Entry) != 20 {
		t.Errorf("Got %d Want %d", len(m.Entry), 20)
	}

	for k, v := range m.Entry {
		num := (40 - 1) - k
		ti := fmt.Sprintf("%d@%s", num, stream)
		if v.Title != ti {
			t.Errorf("Got: %s Want: %s", v.Title, ti)
		}
	}
}

func TestCreateFeedEntriesTail(t *testing.T) {
	setup()
	defer teardown()

	stream := "astream"
	url := fmt.Sprintf("%s/streams/%s/100/forward/20", server.URL, stream)

	es := createTestEvents(100, stream, server.URL, "EventTypeX")
	m, _ := createTestFeed(es, url)

	if len(m.Entry) != 0 {
		t.Errorf("Got %d Want %d", len(m.Entry), 0)
	}

}

func TestCreateFeedEntriesHead(t *testing.T) {
	setup()
	defer teardown()

	stream := "astream"
	url := fmt.Sprintf("%s/streams/%s/head/backward/20", server.URL, stream)

	es := createTestEvents(100, stream, server.URL, "EventTypeX")
	m, _ := createTestFeed(es, url)

	if len(m.Entry) != 20 {
		t.Errorf("Got %d Want %d", len(m.Entry), 20)
	}

	for k, v := range m.Entry {
		num := (len(es) - 1) - k
		ti := fmt.Sprintf("%d@%s", num, stream)
		if v.Title != ti {
			t.Errorf("Got: %s Want: %s", v.Title, ti)
		}
	}
}

func TestCreateEvents(t *testing.T) {
	setup()
	defer teardown()

	es := createTestEvents(100, "astream", server.URL, "EventTypeX")

	if len(es) != 100 {
		t.Errorf("Got %d Want %d", len(es), 100)
	}

	for i := 0; i <= 99; i++ {
		if es[i].EventNumber != i {
			t.Errorf("Got %d Want %d", es[i].EventNumber, i)
		}
	}
}

func TestReverseSlice(t *testing.T) {
	setup()
	defer teardown()

	es := createTestEvents(100, "astream", server.URL, "EventTypeX")

	rs := reverseEventSlice(es)

	if len(rs) != 100 {
		t.Errorf("Got %d Want %d", len(rs), 100)
	}
	top := len(es) - 1
	for i := 0; i <= top; i++ {
		if rs[i].EventNumber != top-i {
			t.Errorf("Got %d Want %d", rs[i].EventNumber, top-i)
		}
	}
}
