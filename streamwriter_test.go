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

	. "gopkg.in/check.v1"
)

var _ = Suite(&StreamWriterSuite{})

type StreamWriterSuite struct{}

func (s *StreamWriterSuite) SetUpTest(c *C) {
	setup()
}
func (s *StreamWriterSuite) TearDownTest(c *C) {
	teardown()
}

func (s *StreamWriterSuite) TestAppendEventsSingle(c *C) {
	data := &MyDataType{Field1: 445, Field2: "Some string"}
	et := "SomeEventType"
	ev := ToEventData("", et, data, nil)
	streamName := "Some-Stream"
	url := fmt.Sprintf("/streams/%s", streamName)
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

	streamWriter := client.NewStreamWriter(streamName)
	resp, err := streamWriter.Append(nil, ev)

	c.Assert(err, IsNil)
	c.Assert(resp.StatusMessage, Equals, "201 Created")
	c.Assert(resp.StatusCode, Equals, http.StatusCreated)
}

func (s *StreamWriterSuite) TestAppendEventsMultiple(c *C) {
	et := "SomeEventType"
	d1 := &MyDataType{Field1: 445, Field2: "Some string"}
	d2 := &MyDataType{Field1: 446, Field2: "Some other string"}
	ev1 := ToEventData("", et, d1, nil)
	ev2 := ToEventData("", et, d2, nil)

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

	streamWriter := client.NewStreamWriter(stream)
	resp, err := streamWriter.Append(nil, ev1, ev2)

	c.Assert(err, IsNil)
	c.Assert(resp.StatusMessage, Equals, "201 Created")
	c.Assert(resp.StatusCode, Equals, http.StatusCreated)
}

func (s *EventSuite) TestAppendEventsWithExpectedVersion(c *C) {
	data := &MyDataType{Field1: 445, Field2: "Some string"}
	et := "SomeEventType"
	ev := ToEventData("", et, data, nil)

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

	streamWriter := client.NewStreamWriter(stream)
	resp, err := streamWriter.Append(expectedVersion, ev)
	c.Assert(err, NotNil)
	c.Assert(resp.StatusMessage, Equals, "400 Bad Request")
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)
}

//func (s *StreamSuite) TestAppendStreamMetadata(c *C) {
//eventType := "MetaData"
//stream := "Some-Stream"

//// Before updating the metadata, the method needs to get the MetaData url
//// According to the docs, the eventstore team reserve the right to change
//// the metadata url.
//fURL := fmt.Sprintf("/streams/%s/head/backward/100", stream)
//fullURL := fmt.Sprintf("%s%s", server.URL, fURL)
//mux.HandleFunc(fURL, func(w http.ResponseWriter, r *http.Request) {
//es := CreateTestEvents(1, stream, server.URL, eventType)
//f, _ := CreateTestFeed(es, fullURL)
//fmt.Fprint(w, f.PrettyPrint())
//})

//meta := fmt.Sprintf("{\"baz\":\"boo\"}")
//want := json.RawMessage(meta)

//url := fmt.Sprintf("/streams/%s/metadata", stream)

//mux.HandleFunc(url, func(w http.ResponseWriter, r *http.Request) {
//c.Assert(r.Method, Equals, "POST")

//var got json.RawMessage
//ev := &Event{Data: &got}
//err := json.NewDecoder(r.Body).Decode(ev)
//c.Assert(err, IsNil)
//c.Assert(got, DeepEquals, want)

//w.WriteHeader(http.StatusCreated)
//fmt.Fprint(w, "")
//})

//resp, err := client.UpdateStreamMetaData(stream, &want)
//c.Assert(err, IsNil)
//c.Assert(resp.StatusMessage, Equals, "201 Created")
//c.Assert(resp.StatusCode, Equals, http.StatusCreated)
//}
