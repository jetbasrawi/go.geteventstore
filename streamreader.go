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
	Err() error
	Next() bool
	Scan(i interface{}) error
	EventResponse() *EventResponse
}

type streamReader struct {
	streamName string
	client     *Client
	version    int
	currentURL string
	pageSize   int

	eventResponse *EventResponse
	//Event         interface{}
	FeedPage *atom.Feed
	lasterr  error
}

func (this *streamReader) Err() error {
	return this.lasterr
}

func (this *streamReader) Version() int {
	return this.version
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
	this.version++
	this.lasterr = nil

	index := int(math.Mod(float64(this.version), float64(this.pageSize)))

	var urlToLoad string = ""
	if this.FeedPage == nil {
		url, err := getFeedURL(this.streamName, "forward", &StreamVersion{Number: this.version}, nil)
		if err != nil {
			this.lasterr = err
			return false
		}
		urlToLoad = url
		//EventStore has links in the atom feed which will
	} else if index == 0 {
		l := this.FeedPage.GetLink("previous")
		urlToLoad = l.Href
	}

	if urlToLoad != "" {
		//Read the page of the streame
		f, resp, err := this.client.readStream(urlToLoad)
		if err != nil {
			if resp != nil {
				switch resp.StatusCode {
				//If the stream does not exist an ErrStreamDoesNotExist will
				//be returned. True is returned to let the client decide in
				//their loop how to respond.
				case http.StatusNotFound:
					this.version--
					this.lasterr = ErrStreamDoesNotExist
					return true
				//If the request has insufficient permissions to access the
				//stream ErrUnauthorized will be returned.
				//True is returned to let the client decide in
				//their loop how to respond.
				case http.StatusUnauthorized:
					this.version--
					this.lasterr = ErrUnauthorized
					return true
				}
			}

			this.version--
			this.lasterr = err
			return false
		}
		this.FeedPage = f
	}

	//End of the stream reached
	//return true but with nil event
	if index >= len(this.FeedPage.Entry) {
		this.eventResponse = nil
		this.lasterr = ErrNoEvents
		return true
	}

	//spew.Dump(this.FeedPage.Entry)
	revIndex := (len(this.FeedPage.Entry) - 1) - index
	entry := this.FeedPage.Entry[revIndex]
	url := strings.TrimRight(entry.Link[1].Href, "/")
	e, _, err := this.client.GetEvent(url)
	if err != nil {
		this.lasterr = err
		return false
	}
	this.eventResponse = e

	return true
}

func (this *streamReader) Scan(e interface{}) error {

	if this.lasterr != nil {
		return this.lasterr
	}

	if this.eventResponse == nil {
		return ErrNoEvents
	}

	data, ok := this.eventResponse.Event.Data.(*json.RawMessage)
	if !ok {
		return fmt.Errorf("Could not unmarshal the event. Event data is not of type *json.RawMessage")
	}

	if err := json.Unmarshal(*data, e); err != nil {
		return err
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
