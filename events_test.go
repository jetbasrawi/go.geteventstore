// Copyright 2016 Jet Basrawi. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package goes

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	. "gopkg.in/check.v1"
)

var _ = Suite(&EventSuite{})

type EventSuite struct{}

func (s *EventSuite) SetUpTest(c *C) {
	setup()
}
func (s *EventSuite) TearDownTest(c *C) {
	teardown()
}

type MyDataType struct {
	Field1 int    `json:"my_field_1"`
	Field2 string `json:"my_field_2"`
}

type MyMetaDataType struct {
	MetaField1 int    `json:"my_meta_field_1"`
	MetaField2 string `json:"my_meta_field_2"`
}

func (s *EventSuite) TestNewEvent(c *C) {
	uuid, _ := NewUUID()
	eventType := "MyEventType"
	data := &MyDataType{Field1: 555, Field2: "Some string"}
	meta := &MyMetaDataType{MetaField1: 1010, MetaField2: "Some meta string"}
	want := &Event{EventID: uuid, EventType: eventType, Data: data, MetaData: meta}

	got := client.ToEventData(uuid, eventType, data, meta)

	c.Assert(got, DeepEquals, want)
}

func (s *EventSuite) TestAppendEventsSingle(c *C) {
	data := &MyDataType{Field1: 445, Field2: "Some string"}
	et := "SomeEventType"
	ev := client.ToEventData("", et, data, nil)
	stream := "Some-Stream"
	url := fmt.Sprintf("/streams/%s", stream)
	mux.HandleFunc(url, func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, Equals, "POST")

		b, _ := ioutil.ReadAll(r.Body)
		se := []Event{}
		err := json.NewDecoder(bytes.NewReader(b)).Decode(&se)
		c.Assert(err, IsNil)
		c.Assert(se[0].PrettyPrint(), Equals, ev.PrettyPrint())

		mt := "application/vnd.eventstore.events+json"
		mediaType := r.Header.Get("Content-Type")
		c.Assert(mt, Equals, mediaType)

		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, "")
	})

	resp, err := client.AppendToStream(stream, nil, ev)

	c.Assert(err, IsNil)
	c.Assert(resp.StatusMessage, Equals, "201 Created")
	c.Assert(resp.StatusCode, Equals, http.StatusCreated)
}

func (s *EventSuite) TestAppendEventsMultiple(c *C) {

	et := "SomeEventType"
	d1 := &MyDataType{Field1: 445, Field2: "Some string"}
	d2 := &MyDataType{Field1: 446, Field2: "Some other string"}
	ev1 := client.ToEventData("", et, d1, nil)
	ev2 := client.ToEventData("", et, d2, nil)

	stream := "Some-Stream"
	url := fmt.Sprintf("/streams/%s", stream)

	mux.HandleFunc(url, func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, Equals, "POST")

		b, _ := ioutil.ReadAll(r.Body)
		se := []Event{}
		err := json.NewDecoder(bytes.NewReader(b)).Decode(&se)
		c.Assert(err, IsNil)
		c.Assert(se[0].PrettyPrint(), Equals, ev1.PrettyPrint())
		c.Assert(se[1].PrettyPrint(), Equals, ev2.PrettyPrint())

		mt := "application/vnd.eventstore.events+json"
		mediaType := r.Header.Get("Content-Type")
		c.Assert(mt, Equals, mediaType)

		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, "")
	})

	resp, err := client.AppendToStream(stream, nil, ev1, ev2)

	c.Assert(err, IsNil)
	c.Assert(resp.StatusMessage, Equals, "201 Created")
	c.Assert(resp.StatusCode, Equals, http.StatusCreated)
}

func (s *EventSuite) TestAppendEventsWithExpectedVersion(c *C) {
	data := &MyDataType{Field1: 445, Field2: "Some string"}
	et := "SomeEventType"
	ev := client.ToEventData("", et, data, nil)

	stream := "Some-Stream"
	url := fmt.Sprintf("/streams/%s", stream)

	expectedVersion := &StreamVersion{Number: 5}

	mux.HandleFunc(url, func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, Equals, "POST")

		want := strconv.Itoa(expectedVersion.Number)
		got := r.Header.Get("ES-ExpectedVersion")
		c.Assert(got, Equals, want)

		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "")
	})

	resp, err := client.AppendToStream(stream, expectedVersion, ev)
	c.Assert(err, NotNil)
	c.Assert(resp.StatusMessage, Equals, "400 Bad Request")
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)
}

func (s *EventSuite) TestGetEvent(c *C) {
	stream := "GetEventStream"
	es := CreateTestEvents(1, stream, server.URL, "SomeEventType")
	ti := Time(time.Now())

	want, _ := CreateTestEventResponse(es[0], &ti)

	er, _ := CreateTestEventAtomResponse(es[0], &ti)
	str := er.PrettyPrint()

	mux.HandleFunc("/streams/some-stream/299", func(w http.ResponseWriter, r *http.Request) {
		got := r.Header.Get("Accept")
		want := "application/vnd.eventstore.atom+json"
		c.Assert(got, Equals, want)

		fmt.Fprint(w, str)
	})

	got, _, err := client.GetEvent("/streams/some-stream/299")
	c.Assert(err, IsNil)
	c.Assert(got.PrettyPrint(), Equals, want.PrettyPrint())
}

func (s *EventSuite) TestGetEventURLs(c *C) {
	es := CreateTestEvents(2, "some-stream", "http://localhost:2113", "EventTypeX")
	f, _ := CreateTestFeed(es, "http://localhost:2113/streams/some-stream/head/backward/2")

	got, err := getEventURLs(f)
	c.Assert(err, IsNil)
	want := []string{
		"http://localhost:2113/streams/some-stream/1",
		"http://localhost:2113/streams/some-stream/0",
	}
	c.Assert(got, DeepEquals, want)
}
