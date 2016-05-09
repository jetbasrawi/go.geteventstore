package goes

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

var _ = Suite(&GoesSuite{})

type GoesSuite struct{}

func (s *GoesSuite) SetUpTest(c *C) {
	setup()
}

func (s *GoesSuite) TearDownTest(c *C) {
	teardown()
}

const (
	defaultBaseURL = "http://somedomain:2113/"
)

var (

	// mux is the HTTP request multiplexer used with the test server
	mux *http.ServeMux

	// client is the EventStore client being tested
	client *Client

	// server is a test HTTP server used to provide mack API responses
	server *httptest.Server
)

func setup() {
	mux = http.NewServeMux()
	server = httptest.NewServer(mux)

	client, _ = NewClient(nil, server.URL)
}

func setupSimulator(es []*Event, m *Event) {
	u, _ := url.Parse(server.URL)
	handler := ESAtomFeedSimulator{Events: es, BaseURL: u, MetaData: m}
	mux.Handle("/", handler)
}

func teardown() {
	server.Close()
}

func (s *GoesSuite) TestNewClient(c *C) {
	invalidURL := ":"
	client, err := NewClient(nil, invalidURL)
	c.Assert(err, ErrorMatches, "parse :: missing protocol scheme")

	client, _ = NewClient(nil, defaultBaseURL)
	got, want := client.BaseURL.String(), defaultBaseURL
	c.Assert(got, Equals, want)
}

func (s *GoesSuite) TestNewRequest(c *C) {
	client, _ := NewClient(nil, defaultBaseURL)
	inURL, outURL := "/foo", defaultBaseURL+"foo"
	inBody := &Event{EventID: "some-uuid", EventType: "SomeEventType", Data: "some-string"}
	eventStructJSON := `{"eventType":"SomeEventType","eventId":"some-uuid","data":"some-string"}`
	outBody := eventStructJSON + "\n"
	req, _ := client.newRequest("GET", inURL, inBody)

	contentType := "application/vnd.eventstore.events+json"

	// test that the relative url was concatenated
	c.Assert(req.URL.String(), Equals, outURL)

	// test that body was JSON encoded
	body, _ := ioutil.ReadAll(req.Body)
	c.Assert(string(body), Equals, outBody)
	c.Assert(req.Header.Get("Content-Type"), Equals, contentType)
}

func (s *GoesSuite) TestNewRequestWithInvalidJSONReturnsError(c *C) {
	client, _ := NewClient(nil, defaultBaseURL)
	type T struct {
		A map[int]interface{}
	}
	ti := &T{}
	_, err := client.newRequest("GET", "/", ti)
	c.Assert(err, NotNil)
	tp := reflect.TypeOf(ti.A)
	c.Assert(err, FitsTypeOf, &json.UnsupportedTypeError{tp})
}

func (s *GoesSuite) TestNewRequestWithBadURLReturnsError(c *C) {
	clit, _ := NewClient(nil, defaultBaseURL)
	_, err := clit.newRequest("GET", ":", nil)
	c.Assert(err, ErrorMatches, "parse :: missing protocol scheme")
}

// If a nil body is passed to the API, make sure that nil is also
// passed to http.NewRequest.  In most cases, passing an io.Reader that returns
// no content is fine, since there is no difference between an HTTP request
// body that is an empty string versus one that is not set at all.  However in
// certain cases, intermediate systems may treat these differently resulting in
// subtle errors.
func (s *GoesSuite) TestNewRequestWithEmptyBody(c *C) {
	clit, _ := NewClient(nil, defaultBaseURL)
	req, err := clit.newRequest("GET", "/", nil)
	c.Assert(err, IsNil)
	c.Assert(req.Body, IsNil)
}

func (s *GoesSuite) TestDo(c *C) {

	te := CreateTestEvents(1, "some-stream", "localhost:2113", "SomeEventType")
	body := te[0].Data

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, Equals, "POST")
		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, body)
	})

	req, _ := client.newRequest("POST", "/", nil)
	resp, err := client.do(req, nil)
	c.Assert(err, IsNil)

	want := &Response{
		Response:      resp.Response,
		StatusCode:    http.StatusCreated,
		StatusMessage: "201 Created"}

	c.Assert(want, DeepEquals, resp)
}

func (s *GoesSuite) TestErrorResponseContainsCopyOfTheOriginalRequest(c *C) {
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "")
	})

	req, _ := client.newRequest("POST", "/", "[{\"some_field\": 34534}]")

	_, err := client.do(req, nil)

	if e, ok := err.(*ErrorResponse); ok {
		c.Assert(e.Request, DeepEquals, req)
	} else {
		c.FailNow()
	}
}

func (s *GoesSuite) TestErrorResponseContainsStatusCodeAndMessage(c *C) {
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Response Body")
	})

	req, _ := client.newRequest("POST", "/", nil)

	_, err := client.do(req, nil)

	if e, ok := err.(*ErrorResponse); ok {
		c.Assert(e.StatusCode, Equals, http.StatusBadRequest)
		c.Assert(e.Message, Equals, "400 Bad Request")
	} else {
		c.FailNow()
	}
}

func (s *GoesSuite) TestNewResponse(c *C) {

	r := http.Response{
		Status:     "201 Created",
		StatusCode: http.StatusCreated,
	}

	resp := newResponse(&r)

	c.Assert(resp.Status, Equals, "201 Created")
	c.Assert(resp.StatusCode, Equals, http.StatusCreated)
}
