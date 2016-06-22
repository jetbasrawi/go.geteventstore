package goes

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"

	"github.com/jetbasrawi/goes/internal/atom"
)

type StreamReader interface {
	Version() int
	NextVersion(int)
	Err() error
	Next() bool
	Scan(e interface{}, m interface{}) error
	EventResponse() *EventResponse
}

type streamReader struct {
	streamName    string
	client        *Client
	version       int
	nextVersion   int
	currentURL    string
	pageSize      int
	eventResponse *EventResponse
	feedPage      *atom.Feed
	lasterr       error
	loadFeedPage  bool
}

func (this *streamReader) Err() error {
	return this.lasterr
}

func (this *streamReader) Version() int {
	return this.version
}

func (this *streamReader) NextVersion(version int) {
	this.nextVersion = version
}

func (this *streamReader) EventResponse() *EventResponse {
	return this.eventResponse
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
//This approach provides a strightforward mechanism to enumerate events. This
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
//			//client. This may indicate that the eventstore server is not
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
//					this.version--
//					continue
//				case ErrUnauthorised:
//					//
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
//Errors
//ErrStreamDoesNotExist: Returned when the requested stream does not exist.
//ErrUnauthorized: Returned when the request is not authorised to read a stream.
func (this *streamReader) Next() bool {
	this.lasterr = nil

	numEntries := 0
	if this.feedPage != nil {
		numEntries = len(this.feedPage.Entry)
	}

	// index is the index of entries in the current feed page
	index := int(math.Mod(float64(this.nextVersion), float64(this.pageSize)))
	fmt.Printf("INDEX %d\n", index)
	// The final page may have fewer entries than the page size
	if index >= numEntries {
		// If the index is greater than the index of the last entry set the index
		// to 0 and initiate loading the next page
		index = 0
		this.loadFeedPage = true
	}

	fmt.Printf("Version %d NextVersion %d index %d entries %d\n",
		this.version, this.nextVersion, index, numEntries)
	fmt.Printf("URL %s\n", this.currentURL)
	fmt.Printf("Load: %t\n", this.loadFeedPage)

	// The feed page will be nil when the stream reader is first created.
	// The initial feed page url will be constructed based on the current
	// version number.
	if this.feedPage == nil {
		url, err := getFeedURL(
			this.streamName,
			"forward",
			&StreamVersion{Number: this.nextVersion},
			nil,
			this.pageSize,
		)
		if err != nil {
			this.lasterr = err
			return false
		}
		this.currentURL = url
		this.loadFeedPage = true
		fmt.Println(url)
	}

	// If the index is 0 load the previous feed page.
	// GetEventStore uses previous to point to more recent feed pages and uses
	// next to point to older feed pages. A stream starts at the most recent
	// event and ends at the oldest event.
	if this.loadFeedPage {
		if this.feedPage != nil {
			// Get the url for the previous feed page. If the reader is at the head
			// of the stream, the previous link in the feedpage will be nil.
			if l := this.feedPage.GetLink("previous"); l != nil {
				this.currentURL = l.Href
			}
		}

		//Read the feedpage at the current url
		f, resp, err := this.client.readStream(this.currentURL)
		if err != nil {
			// If the response is nil, the error has occured before a http
			// request. Typically this will be some protocol issue such as
			// the eventstore server not being available.
			if resp == nil {
				this.lasterr = err
				return true
			}

			// If the response is not nil the error has occured during the http
			// request. The response contains further details on the error.
			if resp != nil {
				switch resp.StatusCode {
				//If the stream does not exist an ErrStreamDoesNotExist will
				//be returned. True is returned to let the client decide in
				//their loop how to respond.
				case http.StatusNotFound:
					this.lasterr = ErrStreamDoesNotExist
					return true
				//If the request has insufficient permissions to access the
				//stream ErrUnauthorized will be returned.
				//True is returned to let the client decide in
				//their loop how to respond.
				case http.StatusUnauthorized:
					this.lasterr = ErrUnauthorized
					return true
				}
			}

		}
		this.feedPage = f
		this.loadFeedPage = false
		numEntries = len(f.Entry)
	}

	//If there are no events returned at the url return an error
	if numEntries <= 0 {
		this.eventResponse = nil
		this.lasterr = ErrNoEvents
		this.loadFeedPage = true
		return true
	}

	//There are events returned, get the event for the current version
	revIndex := (numEntries - 1) - index
	fmt.Printf("revIndex %d numEntries %d\n", revIndex, numEntries)
	entry := this.feedPage.Entry[revIndex]
	url := strings.TrimRight(entry.Link[1].Href, "/")
	e, _, err := this.client.GetEvent(url)
	if err != nil {
		this.lasterr = err
		return true
	}
	this.eventResponse = e
	this.version = this.nextVersion
	this.nextVersion++

	return true
}

func (this *streamReader) Scan(e interface{}, m interface{}) error {

	if this.lasterr != nil {
		return this.lasterr
	}

	if this.eventResponse == nil {
		return ErrNoEvents
	}

	if e != nil {
		data, ok := this.eventResponse.Event.Data.(*json.RawMessage)
		if !ok {
			return fmt.Errorf("Could not unmarshal the event. Event data is not of type *json.RawMessage")
		}

		if err := json.Unmarshal(*data, e); err != nil {
			return err
		}
	}

	if m != nil {
		meta, ok := this.eventResponse.Event.MetaData.(*json.RawMessage)
		if !ok {
			return fmt.Errorf("Could not unmarshal the event. Event data is not of type *json.RawMessage")
		}

		if err := json.Unmarshal(*meta, &m); err != nil {
			return err
		}
	}

	return nil
}

//// MetaData gets the metadata for a stream.
////
//// Stream metadata is retured as an EventResponse.
//// If the stream metadata is empty the EventResponse will be nil and the Response will
//// contain a 200 status code.
////
//// For more information on stream metadata see:
//// http://docs.geteventstore.com/http-api/3.6.0/stream-metadata/
//func (s *streamReader) MetaData() (*EventResponse, *Response, error) {

//url, resp, err := s.client.getMetadataURL(s.stream)
//if err != nil {
//return nil, resp, err
//}
//er, resp, err := s.client.GetEvent(url)
//if err != nil {
//return nil, resp, err
//}

//return er, resp, nil
//}
