package goes

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/jetbasrawi/goes/internal/atom"
)

type Client struct {
	client *http.Client

	BaseURL *url.URL
}

/*
An ErrorResponse reports one or more errors caused by an API request.

GitHub API docs: http://developer.github.com/v3/#client-errors
*/
type ErrorResponse struct {
	Response   *http.Response // HTTP response that caused this error
	Message    string
	StatusCode int
}

func (r *ErrorResponse) Error() string {
	return fmt.Sprintf("%v %v: %d %+v",
		r.Response.Request.Method, r.Response.Request.URL,
		r.Response.StatusCode, r.Message)
}

func NewClient(httpClient *http.Client, serverURL string) (*Client, error) {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	baseURL, err := url.Parse(serverURL)
	if err != nil {
		return nil, err
	}

	c := &Client{client: httpClient, BaseURL: baseURL}

	return c, nil
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

	req.Header.Set("Content-Type", "application/vnd.eventstore.events+json")

	return req, nil
}

func checkResponse(r *http.Response) error {
	if c := r.StatusCode; 200 <= c && c <= 299 {
		return nil
	}
	errorResponse := &ErrorResponse{Response: r}
	data, err := ioutil.ReadAll(r.Body)
	if err == nil && data != nil {
		json.Unmarshal(data, errorResponse)
	}

	return errorResponse
}

type Response struct {
	*http.Response
	StatusCode    int
	StatusMessage string
}

// newResponse creates a new Response for the provided http.Response.
func newResponse(r *http.Response) *Response {
	response := &Response{Response: r}
	response.StatusMessage = r.Status
	response.StatusCode = r.StatusCode
	return response
}

func (c *Client) do(req *http.Request, v interface{}) (*Response, error) {

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	response := newResponse(resp)

	err = checkResponse(resp)
	if err != nil {
		// even though there was an error, we still return the response
		// in case the caller wants to inspect it further
		return response, err
	}

	if v != nil {
		if v, ok := v.(io.Writer); ok {
			io.Copy(v, resp.Body)

		} else {
			i, _ := ioutil.ReadAll(resp.Body)
			fmt.Println("Decoding %s", string(i))
			//TODO
			//err := json.NewDecoder(bytes.NewReader(i)).Decode(v)
			err := json.Unmarshal(i, v)
			if err == io.EOF {
				log.Println(err)
				err = nil // ignore EOF errors caused by empty response body

			}
		}
	}
	return response, err
}

func (c *Client) NewEvent(eventId, eventType string, data interface{}) *Event {

	e := &Event{EventType: eventType}

	if eventId == "" {
		e.EventID, _ = NewUUID()
	} else {
		e.EventID = eventId
	}

	e.Data = data

	return e
}

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

func (c *Client) UpdateMetaData(stream string, metadata ...*MetaData) (*Response, error) {

	mURL, resp, err := c.getMetadataURL(stream)
	if err != nil {
		return resp, err
	}
	req, err := c.newRequest("POST", mURL, metadata)
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

// Response may be nil
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

func (c *Client) getMetadataURL(stream string) (string, *Response, error) {

	url, err := getFeedURL(stream, "backward", nil, nil)
	if err != nil {
		return "", nil, err
	}

	f, resp, err := c.readFeed(url)
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

func (c *Client) ReadFeedForward(stream string, version *StreamVersion, take *Take) ([]*EventResponse, *Response, error) {

	url, err := getFeedURL(stream, "forward", version, take)
	if err != nil {
		return nil, nil, err
	}

	urls := []string{}

	for {
		f, resp, err := c.readFeed(url)
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

	e, _, err := c.GetEvents(urls)
	if err != nil {
		return nil, nil, err
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

func (c *Client) ReadFeedBackward(stream string, version *StreamVersion, take *Take) ([]*EventResponse, *Response, error) {

	url, err := getFeedURL(stream, "backward", version, take)
	if err != nil {
		return nil, nil, err
	}

	urls := []string{}
	for {

		f, resp, err := c.readFeed(url)
		if err != nil {
			return nil, resp, err
		}

		u, err := getEventURLs(f)
		if err != nil {
			log.Printf("Error getting event URLs %#v\n", err)
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
		log.Printf("Error getting events %#v\n", err)
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

func (c *Client) GetEvents(urls []string) ([]*EventResponse, *Response, error) {

	s := make([]*EventResponse, len(urls))

	for i := 0; i < len(urls); i++ {
		e, resp, err := c.GetEvent(urls[i])
		if err != nil {
			return nil, resp, err
		}
		//fmt.Printf("EVENT: %+v\n", e)
		s[i] = e
	}
	return s, nil, nil
}

func (c *Client) GetEvent(url string) (*EventResponse, *Response, error) {

	r, err := c.newRequest("GET", url, nil)
	if err != nil {
		return nil, nil, err
	}

	r.Header.Set("Accept", "application/vnd.eventstore.atom+json")

	var b bytes.Buffer
	resp, err := c.do(r, &b)
	if err != nil {
		log.Println("Error processing request")
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
		log.Printf("unmarshallEventResponse Error: %s", err)
		return nil, resp, err
	}

	var d json.RawMessage
	var m json.RawMessage
	ev := &Event{Data: &d, MetaData: &m}

	err = json.Unmarshal(raw, ev)
	if err == io.EOF {
		log.Println(err)
		err = nil
	}
	if err != nil {
		log.Printf("GetEvent: %#v\n", err)
	}

	e := EventResponse{}
	e.Title = er.Title
	e.ID = er.ID
	e.Updated = er.Updated
	e.Summary = er.Summary
	e.Event = ev

	return &e, resp, nil
}

func (c *Client) readFeed(url string) (*atom.Feed, *Response, error) {

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

func unmarshalFeed(r io.Reader) (*atom.Feed, error) {

	f := &atom.Feed{}
	err := xml.NewDecoder(r).Decode(f)
	if err != nil {
		return nil, err

	}
	return f, nil

}

func getEventURLs(f *atom.Feed) ([]string, error) {
	s := make([]string, len(f.Entry))

	for i := 0; i < len(f.Entry); i++ {
		e := f.Entry[i]
		s[i] = strings.TrimRight(e.Link[1].Href, "/")
	}
	return s, nil
}

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

type StreamVersion struct {
	Number int
}

type Take struct {
	Number int
}
