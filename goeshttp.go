// Copyright 2016 Jet Basrawi. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package goes

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
)

//type StreamAppender interface {
//AppendToStream(string, *StreamVersion, ...*Event) (*Response, error)
//}

//type MetaDataUpdater interface {
//UpdateStreamMetaData(string, interface{}) (*Response, error)
//}

//type MetaDataReader interface {
//GetStreamMetaData(string) (*EventResponse, *Response, error)
//}

//type StreamReader interface {
//ReadStreamForward(string, *StreamVersion, *Take) ([]*EventResponse, *Response, error)
//ReadStreamBackward(string, *StreamVersion, *Take) ([]*EventResponse, *Response, error)
//}

//type StreamReaderAsync interface {
//ReadStreamForwardAsync(string, *StreamVersion, *Take) <-chan struct {
//*EventResponse
//*Response
//error
//}
//ReadStreamBackwardAsync(string, *StreamVersion, *Take) <-chan struct {
//*EventResponse
//*Response
//error
//}
//}

type GetEventStoreRepositoryClient interface {
	ReadStreamForwardAsync(string, *StreamVersion, *Take) <-chan struct {
		*EventResponse
		*Response
		error
	}

	AppendToStream(string, *StreamVersion, ...*Event) (*Response, error)
}

type EventBuilder interface {
	ToEventData(string, string, interface{}, interface{}) *Event
}

type EventReader interface {
	GetEvent(string) (*EventResponse, *Response, error)
	GetEvents([]string) ([]*EventResponse, *Response, error)
}

// client is the object used to interact with the eventstore.
//
// Use the NewClient constructor to get a client.
type Client struct {
	client  *http.Client
	BaseURL *url.URL
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

	c := &Client{client: httpClient, BaseURL: baseURL}

	return c, nil
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
	Response   *http.Response // HTTP response that caused this error
	Request    *http.Request
	Message    string
	StatusCode int
}

func (r *ErrorResponse) Error() string {
	return fmt.Sprintf("%v %v: %d %+v",
		r.Response.Request.Method, r.Response.Request.URL,
		r.Response.StatusCode, r.Message)
}

func checkResponse(r *http.Response, req *http.Request) error {
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

	return errorResponse
}

// newResponse creates a new Response for the provided http.Response.
func newResponse(r *http.Response) *Response {
	response := &Response{Response: r}
	response.StatusMessage = r.Status
	response.StatusCode = r.StatusCode
	return response
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

func (c *Client) do(req *http.Request, v io.Writer) (*Response, error) {

	var keep, send io.ReadCloser

	if req.Body != nil {
		if buf, err := ioutil.ReadAll(req.Body); err == nil {
			keep = ioutil.NopCloser(bytes.NewReader(buf))
			send = ioutil.NopCloser(bytes.NewReader(buf))
			req.Body = send
		}
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	response := newResponse(resp)

	if keep != nil {
		req.Body = keep
	}
	err = checkResponse(resp, req)
	if err != nil {

		// even though there was an error, we still return the response
		// in case the caller wants to inspect it further
		return response, err
	}

	// When handling post requests v will be nil
	if v != nil {
		io.Copy(v, resp.Body)
	}

	return response, err
}
