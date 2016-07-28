package goes

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/jetbasrawi/goes/internal/atom"
)

type basicAuthCredentials struct {
	Username string
	Password string
}

//TODO: Create a client interface type so that the type can be unexported

// Client is the type used to interact with the eventstore.
//
// Use the NewClient constructor to get a client.
type Client struct {
	client      *http.Client
	BaseURL     *url.URL
	credentials *basicAuthCredentials
	headers     map[string]string
}

// NewClient constructs and returns a new client.
//
// httpClient will usually be nil and will use the http.DefaultClient.
// serverURL is the url to your eventstore server.
func NewClient(httpClient *http.Client, serverURL string) (*Client, error) {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	baseURL, err := url.Parse(serverURL)
	if err != nil {
		return nil, err
	}

	c := &Client{
		client:  httpClient,
		BaseURL: baseURL,
		headers: make(map[string]string),
	}

	c.headers["Content-Type"] = "application/vnd.eventstore.events+json"

	return c, nil
}

// NewStreamReader constructs a new StreamReader instance
func (c *Client) NewStreamReader(streamName string) StreamReader {
	return &streamReader{
		streamName: streamName,
		client:     c,
		version:    -1,
		pageSize:   20,
	}
}

// NewStreamWriter constructs a new StreamWriter instance
func (c *Client) NewStreamWriter(streamName string) StreamWriter {
	return &streamWriter{
		client:     c,
		streamName: streamName,
	}
}

func (c *Client) do(req *http.Request, v io.Writer) (*Response, error) {

	// keep is a copy of the request body that will be returned
	// with the response for diagnostic purposes.
	// send will be used to make the request.
	var keep, send io.ReadCloser

	if req.Body != nil {
		if buf, err := ioutil.ReadAll(req.Body); err == nil {
			keep = ioutil.NopCloser(bytes.NewReader(buf))
			send = ioutil.NopCloser(bytes.NewReader(buf))
			req.Body = send
		}
	}

	// An error is returned if caused by client policy (such as CheckRedirect),
	// or if there was an HTTP protocol error. A non-2xx response doesn't cause
	// an error.
	resp, err := c.client.Do(req)
	if err != nil {
		//TODO: Return a custom error wrapper
		return nil, err
	}

	defer resp.Body.Close()

	// Create a *Response to wrap the http.Response
	response := newResponse(resp)

	// After the request has been made the req.Body will be unreadable.
	// assign keep to the request body so that it can be returned in the
	// response for diagnostic purposes.
	if keep != nil {
		req.Body = keep
	}

	// If the request returned an error status checkResponse will return an
	// *errorResponse containing the original request, status code and status message
	err = getError(resp, req)
	if err != nil {
		// even though there was an error, we still return the response
		// in case the caller wants to inspect it further
		return response, err
	}

	// When handling post requests v will be nil
	if v != nil {
		io.Copy(v, resp.Body)
	}

	return response, nil
}

func getError(r *http.Response, req *http.Request) error {
	if c := r.StatusCode; 200 <= c && c <= 299 {
		return nil
	}

	errorResponse := &ErrorResponse{Response: r}
	data, err := ioutil.ReadAll(r.Body)
	if err == nil && data != nil {
		json.Unmarshal(data, errorResponse)
	}
	errorResponse.Message = r.Status
	errorResponse.StatusCode = r.StatusCode
	errorResponse.Request = req

	switch r.StatusCode {
	case http.StatusBadRequest:
		return &BadRequestError{ErrorResponse: errorResponse}
	case http.StatusUnauthorized:
		return &UnauthorizedError{ErrorResponse: errorResponse}
	case http.StatusServiceUnavailable:
		return &TemporarilyUnavailableError{ErrorResponse: errorResponse}
	case http.StatusNotFound:
		return &NotFoundError{ErrorResponse: errorResponse}
	default:
		return &UnexpectedError{ErrorResponse: errorResponse}
	}
}

// SetBasicAuth sets the credentials for requests.
//
// Requests will retreive the credentials from the client before each request and
// so credentials can be changed without the need to create a new StreamReader or
// StreamWriter.
func (c *Client) SetBasicAuth(username, password string) {
	c.credentials = &basicAuthCredentials{
		Username: username,
		Password: password,
	}
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

// Response encapsulates data from the http response.
//
// A Response object is returned from all method interacting with the eventstore
// server. It is intended to provide access to data about the response in case
// the user wants to inspect the response such as the status or the raw http
// response.
//
// A Response object contains the raw http response, the status code returned
// and the status message returned.
type Response struct {
	*http.Response
	StatusCode    int
	StatusMessage string
}

// ErrorResponse encapsulates data about an interaction with the eventstore that
// produced an http error.
//
// Response contains the raw http response.
// Message contains the status message returned from the server.
// StatusCode contains the status code returned from the server.
type ErrorResponse struct {
	*http.Response // HTTP response that caused this error
	Request        *http.Request
	Message        string
	StatusCode     int
}

func (r *ErrorResponse) Error() string {
	return fmt.Sprintf("%v %v: %d %+v",
		r.Response.Request.Method, r.Response.Request.URL,
		r.Response.StatusCode, r.Message)
}

// Take is used to specify the number of events that should be returned in a
// request to read events from a stream.
type Take struct {
	Number int
}

func (c *Client) newRequest(method, urlString string, body interface{}) (*http.Request, error) {

	url, err := url.Parse(urlString)
	if err != nil {
		return nil, err
	}

	if !url.IsAbs() {
		url = c.BaseURL.ResolveReference(url)
	}

	var buf io.ReadWriter
	if body != nil {
		buf = new(bytes.Buffer)
		err := json.NewEncoder(buf).Encode(body)
		if err != nil {
			return nil, err
		}
	}

	req, err := http.NewRequest(method, url.String(), buf)
	if err != nil {
		return nil, err
	}

	if c.credentials != nil {
		req.SetBasicAuth(c.credentials.Username, c.credentials.Password)
	}

	for k, v := range c.headers {
		req.Header.Set(k, v)
	}

	return req, nil
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

// GetMetadataURL gets the url for the stream metadata.
// according to the documentation the metadata url should be acquired through
// a query to the stream feed as the authors of GetEventStore reserve the right
// to change the url.
// http://docs.geteventstore.com/http-api/3.6.0/stream-metadata/
func (c *Client) GetMetadataURL(stream string) (string, *Response, error) {

	url, err := getFeedURL(stream, "forward", 0, nil, 1)
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

	//TODO: Validate stream argumemt to ensure that it contains only url safe characters

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

// newResponse creates a new Response for the provided http.Response.
func newResponse(r *http.Response) *Response {
	response := &Response{Response: r}
	response.StatusMessage = r.Status
	response.StatusCode = r.StatusCode
	return response
}
