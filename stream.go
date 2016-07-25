// Copyright 2016 Jet Basrawi. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package goes

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/jetbasrawi/goes/internal/atom"
)

type invalidVersionError int

func (i invalidVersionError) Error() string {
	return fmt.Sprintf("%d is not a valid event number", i)
}

type NoMoreEventsError struct{}

func (e NoMoreEventsError) Error() string {
	return "There are no more events to load."
}

type StreamDoesNotExistError struct{}

func (e StreamDoesNotExistError) Error() string {
	return "The stream does not exist."
}

type UnauthorizedError struct{}

func (e UnauthorizedError) Error() string {
	return "You are not authorised to access the stream or the stream does not exist."
}

type TemporarilyUnavailableError struct{}

func (e TemporarilyUnavailableError) Error() string {
	return "Server Is Not Ready"
}

// StreamVersion is used to communicate the desired version of the stream when
// reading or appending to a stream.
//
// There are special cases that can be used when appending to a stream.
// http://docs.geteventstore.com/http-api/3.6.0/writing-to-a-stream/
// -2 : The write should never conflict with anything and should always succeed
// -1 : The stream should not exist at the time of writing. This write will create it.
//  0 : The stream should exist but it should be empty
//type StreamVersion struct {
//Number int
//}

// Take is used to specify the number of events that should be returned in a
// request to read events from a stream.
type Take struct {
	Number int
}

// MetaData encapsulates stream MetaData
type MetaData struct {
	EventType string      `json:"eventType"`
	EventID   string      `json:"eventId"`
	Data      interface{} `json:"data"`
}

// readStream reads a stream atom feed page specified by the url.
// returns an *atom.Feed object
//
// The feed object returned may be nil in case of an error.
// The *Response may also be nil if the error occurred before the http request.
// If the error occured after the http request, the *Response will contain the
// raw http response and status.
// If the error occured during the http request an *ErrorResponse will be returned
// and this will also contain the raw http request and status and an error message.
func (c *Client) readStream(url string) (*atom.Feed, *Response, error) {

	req, err := c.newRequest("GET", url, nil)
	if err != nil {
		return nil, nil, err
	}

	req.Header.Set("Accept", "application/atom+xml")

	var b bytes.Buffer
	resp, err := c.do(req, &b)
	if err != nil {
		return nil, resp, err
	}

	feed, err := unmarshalFeed(bytes.NewReader(b.Bytes()))
	if err != nil {
		return nil, resp, err
	}

	return feed, resp, nil
}

// getMetadataURL gets the url for the stream metadata.
// according to the documentation the metadata url should be acquired through
// a query to the stream feed as the authors of GetEventStore reserve the right
// to change the url.
// http://docs.geteventstore.com/http-api/3.7.0/stream-metadata/
func (c *Client) getMetadataURL(stream string) (string, *Response, error) {

	url, err := getFeedURL(stream, "backward", 0, nil, 1)
	if err != nil {
		return "", nil, err
	}

	f, resp, err := c.readStream(url)
	if err != nil {
		return "", resp, err
	}
	for _, v := range f.Link {
		if v.Rel == "metadata" {
			return v.Href, resp, nil
		}
	}
	return "", resp, nil
}

// unmarshalFeed decodes the io.Reader taken from the http response body and
// returns an *atom.Feed object.
// In case of an error, the returned Feed object will be nil.
func unmarshalFeed(r io.Reader) (*atom.Feed, error) {
	f := &atom.Feed{}
	err := xml.NewDecoder(r).Decode(f)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// getEventURLs extracts a slice of event urls from the feed object.
func getEventURLs(f *atom.Feed) ([]string, error) {
	s := make([]string, len(f.Entry))
	for i := 0; i < len(f.Entry); i++ {
		e := f.Entry[i]
		s[i] = strings.TrimRight(e.Link[1].Href, "/")
	}
	return s, nil
}

// getFeedURL constructs the correct url format for the stream using the version
// and take parameters.
//
// If version, take an direction are all nil or empty, the url returned will be
// to read from the head of the stream backward with a page size of 100
func getFeedURL(stream, direction string, version int, take *Take, pageSize int) (string, error) {

	ps := pageSize
	if take != nil && take.Number < ps {
		ps = take.Number
	}

	dir := ""
	switch direction {
	case "":
		if version == 0 {
			dir = "forward"
		} else {
			dir = "backward"
		}

	case "forward", "backward":
		dir = direction
	default:
		return "", fmt.Errorf("Invalid Direction %s\n", direction)
	}

	ver := "head"
	if version < 0 {
		return "", invalidVersionError(version)
	}
	ver = strconv.Itoa(version)

	return fmt.Sprintf("/streams/%s/%s/%s/%d", stream, ver, dir, ps), nil
}
