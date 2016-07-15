package goes

import (
	"fmt"
	"strconv"
)

//StreamWriter
type StreamWriter interface {
	Append(*int, ...*Event) (*Response, error)
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
func (s *streamWriter) Append(expectedVersion *int, events ...*Event) (*Response, error) {

	u := fmt.Sprintf("/streams/%s", s.streamName)

	req, err := s.client.newRequest("POST", u, events)
	if err != nil {
		return nil, err
	}

	//TODO: review this
	req.Header.Set("Content-Type", "application/vnd.eventstore.events+json")

	if expectedVersion != nil {
		req.Header.Set("ES-ExpectedVersion", strconv.Itoa(*expectedVersion))
	}

	resp, err := s.client.do(req, nil)
	if err != nil {
		return resp, err
	}

	return resp, nil
}

//// UpdateMetaData updates the metadata for a stream
////
//// The operation will replace the current stream metadata
////
//// For more information on stream metadata see:
//// http://docs.geteventstore.com/http-api/3.6.0/stream-metadata/
//func (c *Client) UpdateStreamMetaData(stream string, metadata interface{}) (*Response, error) {

//m := ToEventData("", "MetaData", metadata, nil)
//mURL, resp, err := c.getMetadataURL(stream)
//if err != nil {
//return resp, err
//}
//req, err := c.newRequest("POST", mURL, m)
//if err != nil {
//return resp, err
//}

//req.Header.Set("Content-Type", "application/vnd.eventstore.events+json")

//resp, err = c.do(req, nil)
//if err != nil {
//return resp, err
//}

//return resp, nil
//}
