// Copyright 2016 Jet Basrawi. All rights reserved.
//
// Use of this source code is governed by a permissive BSD 3 Clause License
// that can be found in the license file.

package goes_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"time"

	"github.com/jetbasrawi/go.geteventstore"
	"github.com/jetbasrawi/go.geteventstore.testfeed"
	. "gopkg.in/check.v1"
)

var _ = Suite(&ClientAPISuite{})

type ClientAPISuite struct{}

func (s *ClientAPISuite) SetUpTest(c *C) {
	setup()
}
func (s *ClientAPISuite) TearDownTest(c *C) {
	teardown()
}

func (s *ClientAPISuite) TestReadStream(c *C) {
	stream := "some-stream"
	path := fmt.Sprintf("/streams/%s/head/backward/20", stream)
	url := fmt.Sprintf("%s%s", server.URL, path)

	es := mock.CreateTestEvents(2, stream, server.URL, "EventTypeX")
	f, _ := mock.CreateTestFeed(es, url)

	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, Equals, "GET")
		fmt.Fprint(w, f.PrettyPrint())
		c.Assert(r.Header.Get("Accept"), Equals, "application/atom+xml")
	})

	feed, resp, _ := client.ReadFeed(url)
	c.Assert(feed.PrettyPrint(), DeepEquals, f.PrettyPrint())
	c.Assert(resp.StatusCode, DeepEquals, http.StatusOK)
}

func (s *ClientAPISuite) TestConstructNewClientInvalidURL(c *C) {
	invalidURL := ":"
	_, err := goes.NewClient(nil, invalidURL)
	c.Assert(err, ErrorMatches, "parse :: missing protocol scheme")
}

func (s *ClientAPISuite) TestNewRequest(c *C) {
	reqURL, outURL := "/foo", server.URL+"/foo"
	reqBody := &goes.Event{EventID: "some-uuid", EventType: "SomeEventType", Data: "some-string"}
	eventStructJSON := `{"eventType":"SomeEventType","eventId":"some-uuid","data":"some-string"}`
	outBody := eventStructJSON + "\n"
	req, _ := client.NewRequest("GET", reqURL, reqBody)

	// test that the relative url was concatenated
	c.Assert(req.URL.String(), Equals, outURL)

	// test that body was JSON encoded
	body, _ := ioutil.ReadAll(req.Body)
	c.Assert(string(body), Equals, outBody)
}

func (s *ClientAPISuite) TestRequestsAreSentWithBasicAuthIfSet(c *C) {
	username := "user"
	password := "pass"
	headerStr := "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))

	var authFound bool
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		header := r.Header.Get("Authorization")
		authFound = header == headerStr
		fmt.Fprintf(w, "")
	})

	client.SetBasicAuth("user", "pass")
	streamReader := client.NewStreamReader("something")
	_ = streamReader.Next()
	c.Assert(authFound, Equals, true)
}

func (s *ClientAPISuite) TestNewRequestWithInvalidJSONReturnsError(c *C) {
	type T struct {
		A map[int]interface{}
	}
	ti := &T{}
	_, err := client.NewRequest(http.MethodGet, "/", ti)
	c.Assert(err, NotNil)
	tp := reflect.TypeOf(ti.A)
	c.Assert(err, FitsTypeOf, &json.UnsupportedTypeError{Type: tp})
}

func (s *ClientAPISuite) TestNewRequestWithBadURLReturnsError(c *C) {
	_, err := client.NewRequest(http.MethodGet, ":", nil)
	c.Assert(err, ErrorMatches, "parse :: missing protocol scheme")
}

// If a nil body is passed to the API, make sure that nil is also
// passed to http.NewRequest.  In most cases, passing an io.Reader that returns
// no content is fine, since there is no difference between an HTTP request
// body that is an empty string versus one that is not set at all.  However in
// certain cases, intermediate systems may treat these differently resulting in
// subtle errors.
func (s *ClientAPISuite) TestNewRequestWithEmptyBody(c *C) {
	req, err := client.NewRequest(http.MethodGet, "/", nil)
	c.Assert(err, IsNil)
	c.Assert(req.Body, IsNil)
}

func (s *ClientAPISuite) TestDo(c *C) {

	te := mock.CreateTestEvents(1, "some-stream", "localhost:2113", "SomeEventType")
	body := te[0].Data

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, Equals, http.MethodPost)
		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, body)
	})

	req, _ := client.NewRequest(http.MethodPost, "/", nil)
	resp, err := client.Do(req, nil)
	c.Assert(err, IsNil)

	want := &goes.Response{
		Response:   resp.Response,
		StatusCode: http.StatusCreated,
		Status:     "201 Created"}

	c.Assert(want, DeepEquals, resp)
}

func (s *ClientAPISuite) TestErrorResponseContainsCopyOfTheOriginalRequest(c *C) {
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "")
	})

	req, _ := client.NewRequest(http.MethodPost, "/", "[{\"some_field\": 34534}]")

	_, err := client.Do(req, nil)

	if e, ok := err.(*goes.ErrBadRequest); ok {
		c.Assert(e.ErrorResponse.Request, DeepEquals, req)
	} else {
		c.FailNow()
	}
}

func (s *ClientAPISuite) TestErrorResponseContainsStatusCodeAndMessage(c *C) {
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Response Body")
	})

	req, _ := client.NewRequest(http.MethodPost, "/", nil)

	_, err := client.Do(req, nil)

	if e, ok := err.(*goes.ErrBadRequest); ok {
		c.Assert(e.ErrorResponse.StatusCode, Equals, http.StatusBadRequest)
		c.Assert(e.ErrorResponse.Status, Equals, "400 Bad Request")
	} else {
		c.FailNow()
	}
}

func (s *ClientAPISuite) TestGetEvent(c *C) {
	stream := "GetEventStream"
	es := mock.CreateTestEvents(1, stream, server.URL, "SomeEventType")
	ti := mock.Time(time.Now())

	want := mock.CreateTestEventResponse(es[0], &ti)

	er, _ := mock.CreateTestEventAtomResponse(es[0], &ti)
	str := er.PrettyPrint()

	mux.HandleFunc("/streams/some-stream/299", func(w http.ResponseWriter, r *http.Request) {
		got := r.Header.Get("Accept")
		want := "application/vnd.eventstore.atom+json"
		c.Assert(got, Equals, want)

		fmt.Fprint(w, str)
	})

	got, _, err := client.GetEvent("/streams/some-stream/299")
	c.Assert(err, IsNil)
	c.Assert(got.PrettyPrint(), Equals, want.PrettyPrint())
}

func (s *ClientAPISuite) TestGetEventURLs(c *C) {
	es := mock.CreateTestEvents(2, "some-stream", "http://localhost:2113", "EventTypeX")
	f, _ := mock.CreateTestFeed(es, "http://localhost:2113/streams/some-stream/head/backward/2")

	got, err := f.GetEventURLs()
	c.Assert(err, IsNil)
	want := []string{
		"http://localhost:2113/streams/some-stream/1",
		"http://localhost:2113/streams/some-stream/0",
	}
	c.Assert(got, DeepEquals, want)
}

func (s *ClientAPISuite) TestSoftDeleteStream(c *C) {
	streamName := "foostream"
	mux.HandleFunc("/streams/foostream", func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, Equals, http.MethodDelete)
		h := r.Header.Get("ES-HardDelete")
		c.Assert(h, DeepEquals, "")
		w.WriteHeader(http.StatusNoContent)
		fmt.Fprint(w, "")
	})

	resp, err := client.DeleteStream(streamName, false)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusNoContent)
}

func (s *ClientAPISuite) TestHardDeleteStream(c *C) {
	streamName := "foostream"
	mux.HandleFunc("/streams/foostream", func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, Equals, http.MethodDelete)
		h := r.Header.Get("ES-HardDelete")
		c.Assert(h, DeepEquals, "true")
		w.WriteHeader(http.StatusNoContent)
		fmt.Fprint(w, "")
	})

	resp, err := client.DeleteStream(streamName, true)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusNoContent)
}

func (s *ClientAPISuite) TestDeletingDeletedStreamReturnsDeletedError(c *C) {
	streamName := "foostream"
	mux.HandleFunc("/streams/foostream", func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, Equals, http.MethodDelete)
		h := r.Header.Get("ES-HardDelete")
		c.Assert(h, DeepEquals, "true")
		w.WriteHeader(http.StatusGone)
		fmt.Fprint(w, "")
	})

	resp, err := client.DeleteStream(streamName, true)
	c.Assert(err, NotNil)
	c.Assert(reflect.TypeOf(err).Elem().Name(), Equals, "ErrDeleted")
	c.Assert(resp.StatusCode, Equals, http.StatusGone)
}

func (s *ClientAPISuite) TestGetFeedPathForward(c *C) {
	streamName := "foostream"
	direction := "forward"
	version := 0
	pageSize := 10

	want := fmt.Sprintf("/streams/%s/%d/%s/%d", streamName, version, direction, pageSize)

	got, err := client.GetFeedPath(streamName, direction, version, pageSize)
	c.Assert(err, IsNil)
	c.Assert(got, DeepEquals, want)
}

func (s *ClientAPISuite) TestGetFeedPathBackward(c *C) {
	streamName := "foostream"
	direction := "backward"
	version := 10
	pageSize := 20

	want := fmt.Sprintf("/streams/%s/%d/%s/%d", streamName, version, direction, pageSize)

	got, err := client.GetFeedPath(streamName, direction, version, pageSize)
	c.Assert(err, IsNil)
	c.Assert(got, DeepEquals, want)
}

func (s *ClientAPISuite) TestGetFeedPathHeadBackward(c *C) {
	streamName := "foostream"
	direction := "backward"
	version := -1
	pageSize := 20

	want := fmt.Sprintf("/streams/%s/%s/%s/%d", streamName, "head", direction, pageSize)

	got, err := client.GetFeedPath(streamName, direction, version, pageSize)
	c.Assert(err, IsNil)
	c.Assert(got, DeepEquals, want)
}

func (s *ClientAPISuite) TestGetFeedPathInvalidDirection(c *C) {
	streamName := "foostream"
	direction := "somethingwrong"
	version := -1
	pageSize := 20

	want := ""

	got, err := client.GetFeedPath(streamName, direction, version, pageSize)
	c.Assert(err, NotNil)
	c.Assert(got, DeepEquals, want)
	c.Assert(err, DeepEquals, fmt.Errorf("Invalid Direction %s. Allowed values are \"forward\" or \"backward\" \n", direction))
}

func (s *ClientAPISuite) TestGetFeedPathInvalidDirectionAndVersion(c *C) {
	streamName := "foostream"
	direction := "forward"
	version := -1
	pageSize := 20

	want := ""

	got, err := client.GetFeedPath(streamName, direction, version, pageSize)
	c.Assert(err, NotNil)
	c.Assert(got, DeepEquals, want)
	c.Assert(err, DeepEquals, fmt.Errorf("Invalid Direction (%s) and version (head) combination.\n", direction))
}
