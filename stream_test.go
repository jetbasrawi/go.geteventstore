// Copyright 2016 Jet Basrawi. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package goes

import (
	"fmt"
	"net/http"
	"strings"

	. "gopkg.in/check.v1"
)

var _ = Suite(&StreamSuite{})

type StreamSuite struct{}

func (s *StreamSuite) SetUpTest(c *C) {
	setup()
}
func (s *StreamSuite) TearDownTest(c *C) {
	teardown()
}

func (s *StreamSuite) TestReadStream(c *C) {
	stream := "some-stream"
	path := fmt.Sprintf("/streams/%s/head/backward/20", stream)
	url := fmt.Sprintf("%s%s", server.URL, path)

	es := CreateTestEvents(2, stream, server.URL, "EventTypeX")
	f, _ := CreateTestFeed(es, url)

	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, Equals, "GET")
		fmt.Fprint(w, f.PrettyPrint())
		c.Assert(r.Header.Get("Accept"), Equals, "application/atom+xml")
	})

	feed, resp, _ := client.readStream(url)
	c.Assert(feed.PrettyPrint(), DeepEquals, f.PrettyPrint())
	c.Assert(resp.StatusCode, DeepEquals, http.StatusOK)
}

func (s *StreamSuite) TestUnmarshalFeed(c *C) {
	stream := "unmarshal-feed"
	count := 2

	es := CreateTestEvents(count, stream, server.URL, "EventTypeX")
	url := fmt.Sprintf("%s/streams/%s/head/backward/%d", server.URL, stream, count)

	wf, _ := CreateTestFeed(es, url)
	want := wf.PrettyPrint()

	gf, err := unmarshalFeed(strings.NewReader(want))
	c.Assert(err, IsNil)
	got := gf.PrettyPrint()

	c.Assert(got, DeepEquals, want)
}

//func (s *StreamSuite) TestReadFeedBackwardError(c *C) {
//stream := "ABigStream"
//errWant := errors.New("Stream Does Not Exist")
//mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
//http.Error(w, errWant.Error(), http.StatusNotFound)

//})

//_, resp, err := client.ReadStreamBackward(stream, nil, nil)
//c.Assert(err, NotNil)
//c.Assert(resp.StatusCode, Equals, http.StatusNotFound)

//}

//func (s *StreamSuite) TestReadFeedBackwardFromVersionAll(c *C) {
//stream := "ABigStream"
//ne := 1000
//es := CreateTestEvents(ne, stream, server.URL, "EventTypeX")

//setupSimulator(es, nil)

//ver := &StreamVersion{Number: 100}

//evs, _, err := client.ReadStreamBackward(stream, ver, nil)
//c.Assert(err, IsNil)
//nex := ver.Number + 1
//c.Assert(evs, HasLen, nex)
//c.Assert(evs[0].Event.EventNumber, Equals, ver.Number)
//c.Assert(evs[len(evs)-1].Event.EventNumber, Equals, 0)
//for k, v := range evs {
//ex := (nex - 1) - k
//c.Assert(v.Event.EventNumber, Equals, ex)

//}

//}

//func (s *StreamSuite) TestReadFeedBackwardAll(c *C) {
//stream := "ABigStream"
//ne := 1000
//es := CreateTestEvents(ne, stream, server.URL, "EventTypeX")

//setupSimulator(es, nil)

//evs, _, err := client.ReadStreamBackward(stream, nil, nil)
//c.Assert(err, IsNil)
//c.Assert(evs, HasLen, ne)
//c.Assert(evs[0].Event.EventNumber, Equals, ne-1)
//c.Assert(evs[len(evs)-1].Event.EventNumber, Equals, 0)
//for k, v := range evs {
//ex := (ne - 1) - k
//c.Assert(v.Event.EventNumber, Equals, ex)

//}

//}

//func (s *StreamSuite) TestReadFeedForwardError(c *C) {
//stream := "ABigStream"
//errWant := errors.New("Stream Does Not Exist")
//mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
//http.Error(w, errWant.Error(), http.StatusNotFound)

//})

//_, resp, err := client.ReadStreamForward(stream, nil, nil)
//c.Assert(err, NotNil)
//c.Assert(resp.StatusCode, Equals, http.StatusNotFound)

//}

//func (s *StreamSuite) TestReadFeedBackwardFromVersionWithTake(c *C) {
//stream := "ABigStream"
//ne := 1000
//es := CreateTestEvents(ne, stream, server.URL, "EventTypeX")

//setupSimulator(es, nil)

//ver := &StreamVersion{Number: 667}
//take := &Take{Number: 14}

//evs, _, err := client.ReadStreamBackward(stream, ver, take)
//c.Assert(err, IsNil)
//nex := take.Number
//c.Assert(evs, HasLen, nex)

//lstn := ver.Number - (take.Number - 1)
//c.Assert(evs[0].Event.EventNumber, Equals, ver.Number)
//c.Assert(evs[len(evs)-1].Event.EventNumber, Equals, lstn)
//for k, v := range evs {
//ex := ver.Number - k
//c.Assert(v.Event.EventNumber, Equals, ex)

//}

//}

//func (s *StreamSuite) TestReadFeedBackwardFromVersionWithTakeOutOfRangeUnder(c *C) {
//stream := "ABigStream"
//ne := 1000
//es := CreateTestEvents(ne, stream, server.URL, "EventTypeX")

//setupSimulator(es, nil)

//ver := &StreamVersion{Number: 49}
//take := &Take{Number: 59}

//evs, _, err := client.ReadStreamBackward(stream, ver, take)
//c.Assert(err, IsNil)

//nex := ver.Number + 1
//c.Assert(evs, HasLen, nex)

//lstn := 0
//c.Assert(evs[0].Event.EventNumber, Equals, ver.Number)
//c.Assert(evs[len(evs)-1].Event.EventNumber, Equals, lstn)
//for k, v := range evs {
//ex := ver.Number - k
//c.Assert(v.Event.EventNumber, Equals, ex)

//}

//}

////Try to get versions past head of stream that do not yet exist
////this use case is used to poll head of stream waiting for new events
//func (s *StreamSuite) TestReadFeedForwardTail(c *C) {
//stream := "ABigStream"
//ne := 1000
//es := CreateTestEvents(ne, stream, server.URL, "EventTypeX")
//setupSimulator(es, nil)
//ver := &StreamVersion{Number: 1000}

//evs, _, err := client.ReadStreamForward(stream, ver, nil)

//c.Assert(err, IsNil)
//c.Assert(evs, HasLen, 0)

//}

//func (s *StreamSuite) TestGetFeedURLInvalidVersion(c *C) {
//stream := "ABigStream"
//ver := &StreamVersion{Number: -1}

//_, err := getFeedURL(stream, "forward", ver, nil)
//c.Assert(err, FitsTypeOf, invalidVersionError(ver.Number))

//}

//func (s *StreamSuite) TestReadFeedForwardAll(c *C) {
//stream := "ABigStream"
//ne := 1000
//es := CreateTestEvents(ne, stream, server.URL, "EventTypeX")

//setupSimulator(es, nil)

//evs, _, err := client.ReadStreamForward(stream, nil, nil)
//c.Assert(err, IsNil)
//c.Assert(evs, HasLen, ne)
//c.Assert(evs[0].Event.EventNumber, Equals, 0)
//c.Assert(evs[len(evs)-1].Event.EventNumber, Equals, ne-1)
//for k, v := range evs {
//c.Assert(v.Event.EventNumber, Equals, k)

//}

//}

//func (s *StreamSuite) TestReadStreamForwardAsync(c *C) {

//stream := "SomeBigStream"
//ne := 1000
//es := CreateTestEvents(ne, stream, server.URL, "EventTypeY", "EventTypeZ")

//setupSimulator(es, nil)

//eventsChannel := client.ReadStreamForwardAsync(stream, nil, nil, 0)
//count := 0
//for {
//select {
//case ev, open := <-eventsChannel:
//if !open {
//c.Assert(count, Equals, ne)
//return

//}

//c.Assert(ev.Err, IsNil)
//c.Assert(ev.EventResp.Event.PrettyPrint(),
//DeepEquals,
//es[ev.EventResp.Event.EventNumber].PrettyPrint())
//count++

//}

//}

//}

//func (s *StreamSuite) TestReadStreamBackwardAsync(c *C) {

//stream := "SomeBigStream"
//ne := rand.Intn(550)
//es := CreateTestEvents(ne, stream, server.URL, "EventTypeY", "EventTypeZ")

//setupSimulator(es, nil)

//eventsChannel := client.ReadStreamBackwardAsync(stream, nil, nil)
//count := ne
//for {
//select {
//case ev, open := <-eventsChannel:
//if !open {
//c.Assert(count, Equals, 0)
//return

//}
//c.Assert(ev.Err, IsNil)
//c.Assert(ev.EventResp.Event.PrettyPrint(),
//DeepEquals,
//es[ev.EventResp.Event.EventNumber].PrettyPrint())
//count--

//}

//}

//}

//func (s *StreamSuite) TestReadStreamForwardAsyncWithVersion(c *C) {

//stream := "SomeBigStream"
//ne := rand.Intn(550)
//es := CreateTestEvents(ne, stream, server.URL, "EventTypeY", "EventTypeZ")

//setupSimulator(es, nil)

//ver := &StreamVersion{rand.Intn(ne)}
//eventsChannel := client.ReadStreamForwardAsync(stream, ver, nil, 0)
//count := 0
//var first *Event
//var last *Event
//for {
//select {
//case ev, open := <-eventsChannel:
//if !open {
//c.Assert(count, Equals, ne-ver.Number)
//c.Assert(first.EventNumber, Equals, es[ver.Number].EventNumber)
//c.Assert(last.EventNumber, DeepEquals, es[len(es)-1].EventNumber)
//return

//}

//if first == nil {
//first = ev.EventResp.Event

//}

//last = ev.EventResp.Event

//c.Assert(ev.Err, IsNil)
//c.Assert(ev.EventResp.Event.PrettyPrint(),
//DeepEquals,
//es[ev.EventResp.Event.EventNumber].PrettyPrint())
//count++

//}

//}

//}

//func (s *StreamSuite) TestReadStreamBackwardAsyncWithVersion(c *C) {

//stream := "SomeBigStream"
//ne := rand.Intn(550)
//es := CreateTestEvents(ne, stream, server.URL, "EventTypeY", "EventTypeZ")

//setupSimulator(es, nil)

//ver := &StreamVersion{rand.Intn(ne)}
//eventsChannel := client.ReadStreamBackwardAsync(stream, ver, nil)
//count := 0
//var first *Event
//var last *Event
//for {
//select {
//case ev, open := <-eventsChannel:
//if !open {
//c.Assert(count, Equals, ver.Number+1)
//c.Assert(first.EventNumber, Equals, es[ver.Number].EventNumber)
//c.Assert(last.EventNumber, DeepEquals, 0)
//return

//}

//if first == nil {
//first = ev.EventResp.Event

//}

//last = ev.EventResp.Event

//c.Assert(ev.Err, IsNil)
//c.Assert(ev.EventResp.Event.PrettyPrint(),
//DeepEquals,
//es[ev.EventResp.Event.EventNumber].PrettyPrint())
//count++

//}

//}

//}

//func (s *StreamSuite) TestReadStreamForwardAsyncWithVersionAndTake(c *C) {

//stream := "SomeBigStream"
//ne := rand.Intn(550)
//es := CreateTestEvents(ne, stream, server.URL, "EventTypeY", "EventTypeZ")

//setupSimulator(es, nil)

//ver := &StreamVersion{rand.Intn(ne)}
//take := &Take{rand.Intn((ne) - ver.Number)}
//eventsChannel := client.ReadStreamForwardAsync(stream, ver, take, 0)
//count := 0
//var first *Event
//var last *Event
//for {
//select {
//case ev, open := <-eventsChannel:
//if !open {
//c.Assert(count, Equals, take.Number)
//c.Assert(first.EventNumber, Equals, es[ver.Number].EventNumber)
//c.Assert(last.EventNumber, DeepEquals, es[take.Number].EventNumber+ver.Number-1)
//return

//}

//if first == nil {
//first = ev.EventResp.Event

//}

//last = ev.EventResp.Event

//c.Assert(ev.Err, IsNil)
//c.Assert(ev.EventResp.Event.PrettyPrint(),
//DeepEquals,
//es[ev.EventResp.Event.EventNumber].PrettyPrint())
//count++

//}

//}

//}

//func (s *StreamSuite) TestReadStreamBackwardAsyncWithVersionAndTake(c *C) {

//stream := "SomeBigStream"
//ne := 550
//es := CreateTestEvents(ne, stream, server.URL, "EventTypeY", "EventTypeZ")

//setupSimulator(es, nil)

//ver := &StreamVersion{450}
//take := &Take{50}
//eventsChannel := client.ReadStreamBackwardAsync(stream, ver, take)
//count := 0
//var first *Event
//var last *Event
//for {
//select {
//case ev, open := <-eventsChannel:
//if !open {
//c.Assert(count, Equals, take.Number)
//c.Assert(first.EventNumber, Equals, es[ver.Number].EventNumber)
//c.Assert(last.EventNumber, Equals, es[ver.Number-take.Number].EventNumber+1)
//return

//}

//if first == nil {
//first = ev.EventResp.Event

//}

//last = ev.EventResp.Event
//c.Assert(ev.Err, IsNil)
//c.Assert(ev.EventResp.Event.PrettyPrint(),
//DeepEquals,
//es[ev.EventResp.Event.EventNumber].PrettyPrint())
//count++

//}

//}

//}

//func (s *StreamSuite) TestGetFeedURLForwardLowTake(c *C) {
//want := "/streams/some-stream/0/forward/10"
//got, _ := getFeedURL("some-stream", "forward", nil, &Take{Number: 10})
//c.Assert(got, Equals, want)

//}

//func (s *StreamSuite) TestGetFeedURLBackwardLowTake(c *C) {
//want := "/streams/some-stream/head/backward/15"
//got, _ := getFeedURL("some-stream", "backward", nil, &Take{Number: 15})
//c.Assert(got, Equals, want)

//}

//func (s *StreamSuite) TestGetFeedURLInvalidDirection(c *C) {
//want := errors.New("Invalid Direction")
//_, got := getFeedURL("some-stream", "nonesense", nil, nil)
//c.Assert(got, DeepEquals, want)

//}

//func (s *StreamSuite) TestGetFeedURLBackwardNilAll(c *C) {
//want := "/streams/some-stream/head/backward/100"
//got, _ := getFeedURL("some-stream", "", nil, nil)
//c.Assert(got, Equals, want)

//}

//func (s *StreamSuite) TestGetFeedURLForwardNilAll(c *C) {
//want := "/streams/some-stream/0/forward/100"
//got, _ := getFeedURL("some-stream", "forward", nil, nil)
//c.Assert(got, Equals, want)

//}

//func (s *StreamSuite) TestGetFeedURLForwardVersioned(c *C) {
//want := "/streams/some-stream/15/forward/100"
//got, _ := getFeedURL("some-stream", "forward", &StreamVersion{Number: 15}, nil)
//c.Assert(got, Equals, want)

//}
