package goes

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/jetbasrawi/goes/internal/atom"
)

// StreamVersion is used to communicate the desired version of the stream when
// reading or appending to a stream.
//
// There are special cases that can be used when appending to a stream.
// http://docs.geteventstore.com/http-api/3.6.0/writing-to-a-stream/
// -2 : The write should never conflict with anything and should always succeed
// -1 : The stream should not exist at the time of writing. This write will create it.
//  0 : The stream should exist but it should be empty
type StreamVersion struct {
	Number int
}

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

// AppendToStream writes an event to the head of the stream
//
// If the stream does not exist, it will be created.
//
// There are some special version numbers that can be provided.
// http://docs.geteventstore.com/http-api/3.6.0/writing-to-a-stream/
// -2 : The write should never conflict with anything and should always succeed
// -1 : The stream should not exist at the time of writing. This write will create it.
//  0 : The stream should exist but it should be empty
func (c *Client) AppendToStream(streamName string, expectedVersion *StreamVersion, events ...*Event) (*Response, error) {

	u := fmt.Sprintf("/streams/%s", streamName)

	req, err := c.newRequest("POST", u, events)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/vnd.eventstore.events+json")

	if expectedVersion != nil {
		req.Header.Set("ES-ExpectedVersion", strconv.Itoa(expectedVersion.Number))
	}

	resp, err := c.do(req, nil)
	if err != nil {
		return resp, err
	}

	return resp, nil
}

// UpdateMetaData updates the metadata for a stream
//
// The operation will replace the current stream metadata
//
// For more information on stream metadata see:
// http://docs.geteventstore.com/http-api/3.6.0/stream-metadata/
func (c *Client) UpdateStreamMetaData(stream string, metadata interface{}) (*Response, error) {

	m := c.ToEventData("", "MetaData", metadata, nil)
	mURL, resp, err := c.getMetadataURL(stream)
	if err != nil {
		return resp, err
	}
	req, err := c.newRequest("POST", mURL, m)
	if err != nil {
		return resp, err
	}

	req.Header.Set("Content-Type", "application/vnd.eventstore.events+json")

	resp, err = c.do(req, nil)
	if err != nil {
		return resp, err
	}

	return resp, nil
}

// GetStreamMetaData gets the metadata for a stream.
//
// Stream metadata is retured as an EventResponse.
// If the stream metadata is empty the EventResponse will be nil and the Response will
// contain a 200 status code.
//
// For more information on stream metadata see:
// http://docs.geteventstore.com/http-api/3.6.0/stream-metadata/
func (c *Client) GetStreamMetaData(stream string) (*EventResponse, *Response, error) {

	url, resp, err := c.getMetadataURL(stream)
	if err != nil {
		return nil, resp, err
	}
	er, resp, err := c.GetEvent(url)
	if err != nil {
		return nil, resp, err
	}

	return er, resp, nil
}

// ReadStreamForward gets all events in the stream starting at the version specified
// up to the end of the stream or up to the number of events specified by the take
// argument.
//
// If version argument is nil, reading will begin at version 0.
// If take is nil, all events from the version number to the end of the stream
// will be returned.
//
// The events will be returned as a slice of *EventResponse. *Response will
// contain the raw http response and status. In success case error will be nil.
//
// In case of an error, a nil slice will be returned. *Response may be nil in
// cases where the error occured before the http request was performed to read
// the stream. If the error occured after the http request was made, *Response
// will be returned and will contain the raw http response and status.
// An *ErrorResponse will also be returned and this will contain the raw http
// response and status and a description of the error.
func (c *Client) ReadStreamForward(stream string, version *StreamVersion, take *Take) ([]*EventResponse, *Response, error) {

	url, err := getFeedURL(stream, "forward", version, take)
	if err != nil {
		return nil, nil, err
	}

	urls := []string{}

	for {
		f, resp, err := c.readStream(url)
		if err != nil {
			return nil, resp, err
		}

		u, err := getEventURLs(f)
		if err != nil {
			return nil, nil, err
		}
		if len(u) <= 0 {
			break
		}

		var rev []string
		for i := len(u) - 1; i >= 0; i-- {
			rev = append(rev, u[i])
		}

		urls = append(urls, rev...)

		if take != nil && len(urls) >= take.Number {
			break
		}

		p := ""
		for _, v := range f.Link {
			if v.Rel == "previous" {
				p = v.Href
				break
			}
		}
		url = p
	}

	e, resp, err := c.GetEvents(urls)
	if err != nil {
		return nil, resp, err
	}
	if take != nil {
		d := len(e) - take.Number
		if d < 0 {
			return e, nil, nil
		}
		return e[:take.Number], nil, nil
	}

	return e, nil, nil
}

// ReadStreamForwardAsync returns a channel of struct containing an *EventResponse, a *Response and an error.
//
// When the method has completed reading the number of events requested it will close the channel.
// In case of an error the error will be returned on the channel. If the error occured before the
// http request was processed the *Response will be nil. If the error occurred during the http
// request then the *Response will contain details of the error and the http request and response.
func (c *Client) ReadStreamForwardAsync(stream string, version *StreamVersion, take *Take) <-chan struct {
	*EventResponse
	*Response
	error
} {

	eventsChannel := make(chan struct {
		*EventResponse
		*Response
		error
	})

	go func() {
		count := 0

		url, err := getFeedURL(stream, "forward", version, take)
		if err != nil {
			eventsChannel <- struct {
				*EventResponse
				*Response
				error
			}{nil, nil, err}
			close(eventsChannel)
			return
		}

		for {

			f, resp, err := c.readStream(url)
			if err != nil {
				eventsChannel <- struct {
					*EventResponse
					*Response
					error
				}{nil, resp, err}
				close(eventsChannel)
				return
			}

			u, err := getEventURLs(f)
			if err != nil {
				eventsChannel <- struct {
					*EventResponse
					*Response
					error
				}{nil, nil, err}
				close(eventsChannel)
				return
			}

			if len(u) <= 0 {
				close(eventsChannel)
				return
			}

			var rev []string
			for i := len(u) - 1; i >= 0; i-- {
				rev = append(rev, u[i])
			}

			p := ""
			for _, v := range f.Link {
				if v.Rel == "previous" {
					p = v.Href
					break
				}
			}
			url = p

			for _, v := range rev {
				e, resp, err := c.GetEvent(v)
				if err != nil {
					eventsChannel <- struct {
						*EventResponse
						*Response
						error
					}{nil, resp, err}
					close(eventsChannel)
					return
				}

				eventsChannel <- struct {
					*EventResponse
					*Response
					error
				}{e, resp, nil}

				count++

				//fmt.Printf("%d\n", count)
				if take != nil && take.Number <= count {
					close(eventsChannel)
					return
				}
			}
		}
	}()

	return eventsChannel
}

func (c *Client) ReadStreamBackwardAsync(stream string, version *StreamVersion, take *Take) <-chan struct {
	*EventResponse
	*Response
	error
} {

	eventsChannel := make(chan struct {
		*EventResponse
		*Response
		error
	})

	go func() {
		count := 0

		url, err := getFeedURL(stream, "backward", version, take)
		if err != nil {
			eventsChannel <- struct {
				*EventResponse
				*Response
				error
			}{nil, nil, err}
			close(eventsChannel)
			return
		}

		for {

			if url == "" {
				close(eventsChannel)
				return
			}

			f, resp, err := c.readStream(url)
			if err != nil {
				eventsChannel <- struct {
					*EventResponse
					*Response
					error
				}{nil, resp, err}
				close(eventsChannel)
				return
			}

			u, err := getEventURLs(f)
			if err != nil {
				eventsChannel <- struct {
					*EventResponse
					*Response
					error
				}{nil, nil, err}
				close(eventsChannel)
				return
			}

			if len(u) <= 0 {
				close(eventsChannel)
				return
			}

			n := ""
			for _, v := range f.Link {
				if v.Rel == "next" {
					n = v.Href
					break
				}
			}
			url = n

			for _, v := range u {
				e, resp, err := c.GetEvent(v)
				if err != nil {
					eventsChannel <- struct {
						*EventResponse
						*Response
						error
					}{nil, resp, err}
					close(eventsChannel)
					return
				}

				eventsChannel <- struct {
					*EventResponse
					*Response
					error
				}{e, resp, nil}

				count++

				//fmt.Printf("%d\n", count)
				if take != nil && take.Number <= count {
					close(eventsChannel)
					return
				}
			}
		}
	}()

	return eventsChannel
}

// ReadStreamBackward gets all events in the stream starting at the version specified
// up to version 0  of the stream or up to the number of events specified by the take
// argument.
//
// If version argument is nil, reading will begin at the head of the stream.
// If take is nil, all events from the version number to the first event of the stream
// will be returned.
//
// The events will be returned as a slice of *EventResponse. *Response will
// contain the raw http response and status. In success case error will be nil.
//
// In case of an error, a nil slice will be returned. *Response may be nil in
// cases where the error occured before the http request was performed to read
// the stream. If the error occured after the http request was made, *Response
// will be returned and will contain the raw http response and status.
// An *ErrorResponse will also be returned and this will contain the raw http
// response and status and a description of the error.
func (c *Client) ReadStreamBackward(stream string, version *StreamVersion, take *Take) ([]*EventResponse, *Response, error) {

	url, err := getFeedURL(stream, "backward", version, take)
	if err != nil {
		return nil, nil, err
	}

	urls := []string{}
	for {

		f, resp, err := c.readStream(url)
		if err != nil {
			return nil, resp, err
		}

		u, err := getEventURLs(f)
		if err != nil {
			return nil, nil, err
		}
		if len(u) <= 0 {
			break
		}
		urls = append(urls, u...)

		if take != nil && len(urls) >= take.Number {
			break
		}

		n := ""
		for _, v := range f.Link {
			if v.Rel == "next" {
				n = v.Href
				break
			}
		}
		if n == "" {
			break
		} else {
			url = n
		}
	}

	e, resp, err := c.GetEvents(urls)
	if err != nil {
		return nil, resp, err
	}

	if take != nil {
		d := len(e) - take.Number
		if d < 0 {
			return e, nil, nil
		}
		return e[:take.Number], nil, nil
	}

	return e, nil, nil
}

// readStream is a reads a stream atom feed page specified by the url.
// returns a *atom.Feed object
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
func (c *Client) getMetadataURL(stream string) (string, *Response, error) {

	url, err := getFeedURL(stream, "backward", nil, nil)
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
// In case of an error, the returned object will be nil.
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
func getFeedURL(stream, direction string, version *StreamVersion, take *Take) (string, error) {

	ps := 100
	if take != nil && take.Number < ps {
		ps = take.Number
	}
	dir := ""
	switch direction {
	case "":
		if version != nil && version.Number == 0 {
			dir = "forward"
		} else {
			dir = "backward"
		}

	case "forward", "backward":
		dir = direction
	default:
		return "", errors.New("Invalid Direction")
	}

	ver := "head"
	if version != nil {
		if version.Number < 0 {
			return "", invalidVersionError(version.Number)
		}
		ver = strconv.Itoa(version.Number)
	} else if dir == "forward" {
		ver = "0"
	}

	return fmt.Sprintf("/streams/%s/%s/%s/%d", stream, ver, dir, ps), nil
}
