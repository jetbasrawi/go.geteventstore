// Copyright 2016 Jet Basrawi. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package goes

import (
	"fmt"
	"net/http"
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
	uuid := NewUUID()
	eventType := "MyEventType"
	data := &MyDataType{Field1: 555, Field2: "Some string"}
	meta := &MyMetaDataType{MetaField1: 1010, MetaField2: "Some meta string"}
	want := &Event{EventID: uuid, EventType: eventType, Data: data, MetaData: meta}

	got := ToEventData(uuid, eventType, data, meta)

	c.Assert(got, DeepEquals, want)
}

func (s *EventSuite) TestGetEvent(c *C) {
	stream := "GetEventStream"
	es := CreateTestEvents(1, stream, server.URL, "SomeEventType")
	ti := Time(time.Now())

	want := CreateTestEventResponse(es[0], &ti)

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
