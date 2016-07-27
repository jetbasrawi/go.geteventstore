package goes

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"

	. "gopkg.in/check.v1"
)

var _ = Suite(&ClientSuite{})

type ClientSuite struct{}

func (s *ClientSuite) SetUpTest(c *C) {
	setup()
}
func (s *ClientSuite) TearDownTest(c *C) {
	teardown()
}

func (s *ClientSuite) TestReadStream(c *C) {
	stream := "some-stream"
	path := fmt.Sprintf("/streams/%s/head/backward/20", stream)
	url := fmt.Sprintf("%s%s", server.URL, path)

	es := CreateTestEvents(2, stream, server.URL, "EventTypeX")
	f, _ := CreateTestFeed(es, url)

	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, Equals, "GET")
		fmt.Fprint(w, f.PrettyPrint())
		c.Assert(r.Header.Get("Accept"), Equals, "application/atom+xml")
	})

	feed, resp, _ := client.readStream(url)
	c.Assert(feed.PrettyPrint(), DeepEquals, f.PrettyPrint())
	c.Assert(resp.StatusCode, DeepEquals, http.StatusOK)
}

func (s *ClientSuite) TestUnmarshalFeed(c *C) {
	stream := "unmarshal-feed"
	count := 2

	es := CreateTestEvents(count, stream, server.URL, "EventTypeX")
	url := fmt.Sprintf("%s/streams/%s/head/backward/%d", server.URL, stream, count)

	wf, _ := CreateTestFeed(es, url)
	want := wf.PrettyPrint()

	gf, err := unmarshalFeed(strings.NewReader(want))
	c.Assert(err, IsNil)
	got := gf.PrettyPrint()

	c.Assert(got, DeepEquals, want)
}

func (s *ClientSuite) TestNewClient(c *C) {
	invalidURL := ":"
	client, err := NewClient(nil, invalidURL)
	c.Assert(err, ErrorMatches, "parse :: missing protocol scheme")

	client, _ = NewClient(nil, defaultBaseURL)
	got, want := client.BaseURL.String(), defaultBaseURL
	c.Assert(got, Equals, want)
}

func (s *ClientSuite) TestNewRequest(c *C) {
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

func (s *ClientSuite) TestRequestsAreSentWithBasicAuthIfSet(c *C) {

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

func (s *ClientSuite) TestNewRequestWithInvalidJSONReturnsError(c *C) {
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

func (s *ClientSuite) TestNewRequestWithBadURLReturnsError(c *C) {
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
func (s *ClientSuite) TestNewRequestWithEmptyBody(c *C) {
	clit, _ := NewClient(nil, defaultBaseURL)
	req, err := clit.newRequest("GET", "/", nil)
	c.Assert(err, IsNil)
	c.Assert(req.Body, IsNil)
}

func (s *ClientSuite) TestDo(c *C) {

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

func (s *ClientSuite) TestErrorResponseContainsCopyOfTheOriginalRequest(c *C) {
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "")
	})

	req, _ := client.newRequest("POST", "/", "[{\"some_field\": 34534}]")

	_, err := client.do(req, nil)

	if e, ok := err.(*BadRequestError); ok {
		c.Assert(e.ErrorResponse.Request, DeepEquals, req)
	} else {
		c.FailNow()
	}
}

func (s *ClientSuite) TestErrorResponseContainsStatusCodeAndMessage(c *C) {
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Response Body")
	})

	req, _ := client.newRequest("POST", "/", nil)

	_, err := client.do(req, nil)

	if e, ok := err.(*BadRequestError); ok {
		c.Assert(e.ErrorResponse.StatusCode, Equals, http.StatusBadRequest)
		c.Assert(e.ErrorResponse.Message, Equals, "400 Bad Request")
	} else {
		c.FailNow()
	}
}

func (s *ClientSuite) TestNewResponse(c *C) {

	r := http.Response{
		Status:     "201 Created",
		StatusCode: http.StatusCreated,
	}

	resp := newResponse(&r)

	c.Assert(resp.Status, Equals, "201 Created")
	c.Assert(resp.StatusCode, Equals, http.StatusCreated)
}
