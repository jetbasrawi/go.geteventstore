package goes

import (
	"fmt"
	"net/http"
	"strconv"
)

// StreamWriter is the interface that stream writers should implement
type StreamWriter interface {
	Append(*int, ...*Event) error
	WriteMetaData(string, interface{}) error
}

type streamWriter struct {
	client     *Client
	streamName string
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
func (s *streamWriter) Append(expectedVersion *int, events ...*Event) error {

	u := fmt.Sprintf("/streams/%s", s.streamName)

	req, err := s.client.newRequest(http.MethodPost, u, events)
	if err != nil {
		return err
	}

	//TODO: review this
	req.Header.Set("Content-Type", "application/vnd.eventstore.events+json")

	if expectedVersion != nil {
		req.Header.Set("ES-ExpectedVersion", strconv.Itoa(*expectedVersion))
	}

	_, err = s.client.do(req, nil)
	if err != nil {
		if e, ok := err.(*BadRequestError); ok {
			return &ConcurrencyError{ErrorResponse: e.ErrorResponse}
		}
		return err
	}

	return nil
}

// WriteMetaData writes the metadata for a stream
//
// The operation will replace the current stream metadata
//
// For more information on stream metadata see:
// http://docs.geteventstore.com/http-api/3.6.0/stream-metadata/
//
// If the metadata was written successfully the error returned will be nil.
//
// If an error occurs the error returned may be an UnauthorizedError, a
// TemporarilyUnavailableError or an UnexpectedError if the error occured during a
// http request to the server. In these cases, the *ErrorResponse will be available
// for inspection as an ErrorResponse field on the error.
// If an error occurred outside of the http request another type of error will be returned
// such as a *url.Error in cases where the streamwriter is unable to connect to the server.
func (s *streamWriter) WriteMetaData(stream string, metadata interface{}) error {

	m := ToEventData("", "MetaData", metadata, nil)
	mURL, _, err := s.client.GetMetadataURL(stream)
	if err != nil {
		return err
	}
	req, err := s.client.newRequest(http.MethodPost, mURL, m)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/vnd.eventstore.events+json")

	_, err = s.client.do(req, nil)
	if err != nil {
		return err
	}

	return nil
}
