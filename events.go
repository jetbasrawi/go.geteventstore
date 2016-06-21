// Copyright 2016 Jet Basrawi. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package goes

import (
	"bytes"
	"encoding/json"
	"io"
	"time"
)

// EventResponse encapsulates the response for an event reflecting the atom
// response returned from the server which contains data in addition to the
// actual event when requested as content type application/vnd.eventstore.atom+json
//
// For more information on the server response see:
// http://docs.geteventstore.com/http-api/3.6.0/reading-streams/
type EventResponse struct {
	Title   string
	ID      string
	Updated TimeStr
	Summary string
	Event   *Event
}

// PrettyPrint renders an indented json view of the EventResponse.
func (e *EventResponse) PrettyPrint() string {

	b, err := json.MarshalIndent(e, "", "	")
	if err != nil {
		panic(err)
	}
	return string(b)

}

// eventAtomResponse is used internally to unmarshall the raw response
type eventAtomResponse struct {
	Title   string      `json:"title"`
	ID      string      `json:"id"`
	Updated TimeStr     `json:"updated"`
	Summary string      `json:"summary"`
	Content interface{} `json:"content"`
}

// PrettyPrint renders and indented json view of the eventAtomResponse
func (e *eventAtomResponse) PrettyPrint() string {

	b, err := json.MarshalIndent(e, "", "	")
	if err != nil {
		panic(err)
	}
	return string(b)

}

// Event encapsulates the data of an eventstore event.
//
//EventStreamID is ..TODO
// EventNumber represents the stream version for this event.
// EventType describes the event type.
// EventID is the guid of the event.
// Data contains the data of the event.
// Links contains the urls of the event on the evenstore
// MetaData contains the metadata for the event.
type Event struct {
	EventStreamID string      `json:"eventStreamId,omitempty"`
	EventNumber   int         `json:"eventNumber,omitempty"`
	EventType     string      `json:"eventType,omitempty"`
	EventID       string      `json:"eventId,omitempty"`
	Data          interface{} `json:"data"`
	Links         []Link      `json:"links,omitempty"`
	MetaData      interface{} `json:"metadata,omitempty"`
}

// PrettyPrint renders an indented json view of the Event object.
func (e *Event) PrettyPrint() string {
	b, err := json.MarshalIndent(e, "", "	")
	if err != nil {
		panic(err)
	}
	return string(b)
}

// Link encapsulates url data for events.
type Link struct {
	URI      string `json:"uri"`
	Relation string `json:"relation"`
}

// TimeStr is a type used to format feed dates.
type TimeStr string

func Time(t time.Time) TimeStr {
	return TimeStr(t.Format("2006-01-02T15:04:05-07:00"))
}

type EventFactory interface {
	GetEvent(string) interface{}
}

// NewEvent creates a new event object.
//
// If an empty eventId is provided an eventId will be generated
// and retured in the event.
func ToEventData(eventId, eventType string, data interface{}, meta interface{}) *Event {
	e := &Event{EventType: eventType}
	if eventId == "" {
		e.EventID = NewUUID()
	} else {
		e.EventID = eventId
	}
	e.Data = data
	e.MetaData = meta
	return e
}

// GetEvents gets all the events specified in the urls argument.
//
// The event slice will be nil in an error case.
// *Response may be nil if an error occurs before the http request. Otherwise
// it will contain the raw http response and status.
// If an error occurs during the http request an *ErrorResponse will be returned
// as the error. The *ErrorResponse will contain the raw http response and status
// and a description of the error.
func (c *Client) GetEvents(urls []string) ([]*EventResponse, *Response, error) {
	s := make([]*EventResponse, len(urls))
	for i := 0; i < len(urls); i++ {
		e, resp, err := c.GetEvent(urls[i])
		if err != nil {
			return nil, resp, err
		}
		s[i] = e
	}
	return s, nil, nil
}

// GetEvent gets a single event
//
// The event response will be nil in an error case.
// *Response may be nil if an error occurs before the http request. Otherwise
// it will contain the raw http response and status.
// If an error occurs during the http request an *ErrorResponse will be returned
// as the error. The *ErrorResponse will contain the raw http response and status
// and a description of the error.
func (c *Client) GetEvent(url string) (*EventResponse, *Response, error) {

	r, err := c.newRequest("GET", url, nil)
	if err != nil {
		return nil, nil, err
	}

	r.Header.Set("Accept", "application/vnd.eventstore.atom+json")

	var b bytes.Buffer
	resp, err := c.do(r, &b)
	if err != nil {
		return nil, resp, err
	}

	if b.String() == "{}" {
		return nil, resp, nil
	}
	var raw json.RawMessage
	er := &eventAtomResponse{Content: &raw}
	err = json.NewDecoder(bytes.NewReader(b.Bytes())).Decode(er)
	if err == io.EOF {
		return nil, resp, nil
	}

	if err != nil {
		return nil, resp, err
	}

	var d json.RawMessage
	var m json.RawMessage
	ev := &Event{Data: &d, MetaData: &m}

	err = json.Unmarshal(raw, ev)
	if err == io.EOF {
		err = nil
	}
	if err != nil {
		return nil, resp, err
	}

	e := EventResponse{}
	e.Title = er.Title
	e.ID = er.ID
	e.Updated = er.Updated
	e.Summary = er.Summary
	e.Event = ev

	return &e, resp, nil
}
