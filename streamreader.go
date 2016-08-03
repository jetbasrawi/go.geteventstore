package goes

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/jetbasrawi/go.geteventstore/internal/atom"
)

// StreamReader provides functions
type StreamReader struct {
	streamName    string
	client        *Client
	version       int
	nextVersion   int
	index         int
	currentURL    string
	pageSize      int
	eventResponse *EventResponse
	feedPage      *atom.Feed
	lasterr       error
	loadFeedPage  bool
}

// Err returns any error that is raised as a result of a call to Next() or Scan()
func (s *StreamReader) Err() error {
	return s.lasterr
}

// Version returns the current stream version of the reader
func (s *StreamReader) Version() int {
	return s.version
}

// NextVersion is the version of the stream that will be returned by a call to Next()
func (s *StreamReader) NextVersion(version int) {
	s.nextVersion = version
}

// EventResponse returns the container for the event that is returned from a call to Next().
func (s *StreamReader) EventResponse() *EventResponse {
	return s.eventResponse
}

//Next gets the next event on the stream from the current version number.
//
//Next returns a boolean that allow your to create a loop to enumerate over
//the events in the stream. Next is idiomatic of the API for database/sql
//package.
//
//Next should be treated more like a cursor over the stream rather than an
//enumerator over a collection of results.
//
//The boolean retun value is mainly intended to provide a convenient loop to
//enumerate and process events, it should in general not be considered a status
//report. To undertand the outcomes of operations the stream's Err() should be
//inspected. It is left to the client to determine under what conditions to exit
//the loop.
//
//s approach provides a strightforward mechanism to enumerate events. s
//approach also provides a convenient way to handle retries in response to
//network or protocol errors.
//
//It is also a convenient way to poll the head of a stream.
//
//The boolean retun value is mainly intended to provide a convenient loop to
//enumerate and process events, it should in general not be considered a status
//report. To undertand the outcomes of operations the stream's Err() should be
//inspected.
//head of a stream.
//
//When next is called, it will go to the eventstore and get the event.
//
//I
//  for stream.Next() {
//		if stream.Err() != nil {
//			//Connection and protocol errors etc are returned from the http
//			//client. s may indicate that the eventstore server is not
//			//available or some other infrastructure issue.
//			if e, ok := stream.Err().(*url.Error); ok {
//				<- time.After()
//			}
//			switch stream.Err() {
//				case ErrNoEvents:
//					//Indicates that there are no more events
//					//typically encountered when reaching the end of the stream.
//					//If you want to read only up to the end of the stream you
//					//could exit the loop.
//					//If you want to poll the end of the stream for new events
//					//wait some duration and then retry as shown below.
//					<- time.After()
//					s.version--
//					continue
//				case ErrUnauthorised:
//					// Indicates that you are not authorised to access the stream
// however, when accessing system streams such as a category
// projection s error is returned in cases where
//				case ErrStreamDoesNotExist:
//					//
//				default:
//					log.Fatal(stream.Err())
//			}
//		}
//
//		myevent := myEventFactory(stream.EventResponse.EventType)
//		err := stream.Scan(&myevent)
//	}
//
// Errors that may be returned.
//
// NotFoundError
//If the stream does not exist an NotFoundError will
//be returned. True is returned to let the client decide in
//their loop how to respond.
//
//UnauthorizedError
//If the request has insufficient permissions to access the
//stream an UnauthorizedError will be returned.
// TemporarilyUnavailableError
// 503 ServiceUnavailable is returned if the server is temporarily unable
// to handle the request. This can occur during startup of the eventstore
// for a brief period after the server has come online but is not yet ready
// to handle requests
func (s *StreamReader) Next() bool {
	s.lasterr = nil

	numEntries := 0
	if s.feedPage != nil {
		numEntries = len(s.feedPage.Entry)
	}

	// The feed page will be nil when the stream reader is first created.
	// The initial feed page url will be constructed based on the current
	// version number.
	if s.feedPage == nil {
		s.index = -1
		url, err := getFeedURL(s.streamName, "forward", s.nextVersion, s.pageSize)
		if err != nil {
			s.lasterr = err
			return false
		}
		s.currentURL = url
	}

	// If the index is less than 0 load the previous feed page.
	// GetEventStore uses previous to point to more recent feed pages and uses
	// next to point to older feed pages. A stream starts at the most recent
	// event and ends at the oldest event.
	if s.index < 0 {
		if s.feedPage != nil {
			// Get the url for the previous feed page. If the reader is at the head
			// of the stream, the previous link in the feedpage will be nil.
			if l := s.feedPage.GetLink("previous"); l != nil {
				s.currentURL = l.Href
			}
		}

		//Read the feedpage at the current url
		f, _, err := s.client.ReadFeed(s.currentURL)
		if err != nil {
			s.lasterr = err
			return true
		}

		s.feedPage = f
		numEntries = len(f.Entry)
		s.index = numEntries - 1
	}

	//If there are no events returned at the url return an error
	if numEntries <= 0 {
		s.eventResponse = nil
		s.lasterr = &NoMoreEventsError{}
		return true
	}

	//There are events returned, get the event for the current version
	entry := s.feedPage.Entry[s.index]
	url := strings.TrimRight(entry.Link[1].Href, "/")
	e, _, err := s.client.GetEvent(url)
	if err != nil {
		s.lasterr = err
		return true
	}
	s.eventResponse = e
	s.version = s.nextVersion
	s.nextVersion++
	s.index--

	return true
}

// Scan deserializes event and event metadata into the types passed in
// as arguments e and m.
func (s *StreamReader) Scan(e interface{}, m interface{}) error {

	if s.lasterr != nil {
		return s.lasterr
	}

	if s.eventResponse == nil {
		return &NoMoreEventsError{}
	}

	if e != nil {
		data, ok := s.eventResponse.Event.Data.(*json.RawMessage)
		if !ok {
			return fmt.Errorf("Could not unmarshal the event. Event data is not of type *json.RawMessage")
		}

		if err := json.Unmarshal(*data, e); err != nil {
			return err
		}
	}

	if m != nil && s.EventResponse().Event.MetaData != nil {
		meta, ok := s.eventResponse.Event.MetaData.(*json.RawMessage)
		if !ok {
			return fmt.Errorf("Could not unmarshal the event. Event data is not of type *json.RawMessage")
		}

		if err := json.Unmarshal(*meta, &m); err != nil {
			return err
		}
	}

	return nil
}

// LongPoll causes the server to wait up to the number of seconds specified for
// for results to become available at the URL requested.
//
// LongPoll will cause the next request for a feedpage to be made with the
// with the ES-LongPoll optional header. When reading a stream past the head,
// the server will wait for the duration specified or until new events are
// returned.
//
// LongPoll is useful when polling the end of the stream as it returns as soon
// as new events are found which means that it is more responsive than polling
// over some time interval like 2 seconds. It also reduces the number of
// unproductive calls to the server polling for events when there are no new
// events to return.
//
// Setting the argument seconds to any integer value above 0 will cause the
// request to be made with ES-LongPoll set to that value. Any value 0 or below
// will cause the request to be made without ES-LongPoll and the server will not
// wait to return.
func (s *StreamReader) LongPoll(seconds int) {
	if seconds > 0 {
		s.client.SetHeader("ES-LongPoll", strconv.Itoa(seconds))
	} else {
		s.client.DeleteHeader("ES-LongPoll")
	}
}

// MetaData gets the metadata for a stream.
//
// Stream metadata is retured as an EventResponse.
//
// For more information on stream metadata see:
// http://docs.geteventstore.com/http-api/3.7.0/stream-metadata/
func (s *StreamReader) MetaData() (*EventResponse, error) {
	url, _, err := s.client.GetMetadataURL(s.streamName)
	if err != nil {
		return nil, err
	}
	ev, _, err := s.client.GetEvent(url)
	if err != nil {
		return nil, err
	}
	return ev, nil
}
