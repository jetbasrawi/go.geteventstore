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
)

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

	eventStructJSON = `{"eventType":"SomeEventType","eventId":"some-uuid","data":"some-string"}`
)

func setup() {
	mux = http.NewServeMux()
	server = httptest.NewServer(mux)

	client, _ = NewClient(nil, server.URL)
}

func setupSimulator(es []*Event, m *Event) {
	u, _ := url.Parse(server.URL)
	handler := ESHandler{Events: es, BaseURL: u, MetaData: m}
	mux.Handle("/", handler)
}

func teardown() {
	server.Close()
}

func testMethod(t *testing.T, r *http.Request, want string) {
	if got := r.Method; got != want {
		t.Errorf("Request method: %v, want %v", got, want)
	}
}

func testURLParseError(t *testing.T, err error) {
	if err == nil {
		t.Errorf("Expected error to be returned")
	}
	if err, ok := err.(*url.Error); !ok || err.Op != "parse" {
		t.Errorf("Expected URL parse error, got %+v", err)
	}
}

func TestNewClient(t *testing.T) {
	setup()
	defer teardown()

	invalidURL := ":"

	c, err := NewClient(nil, invalidURL)
	testURLParseError(t, err)

	c, _ = NewClient(nil, defaultBaseURL)

	if got, want := c.BaseURL.String(), defaultBaseURL; got != want {
		t.Errorf("NewClient BaseURL is %v want %v", got, want)
	}

}

func TestNewRequest(t *testing.T) {
	c, _ := NewClient(nil, defaultBaseURL)

	inURL, outURL := "/foo", defaultBaseURL+"foo"
	inBody := &Event{EventID: "some-uuid", EventType: "SomeEventType", Data: "some-string"}
	outBody := eventStructJSON + "\n"
	req, _ := c.newRequest("GET", inURL, inBody)

	contentType := "application/vnd.eventstore.events+json"

	// test that the relative url was concatenated
	if got, want := req.URL.String(), outURL; got != want {
		t.Errorf("NewRequest(%q) URL is %v, want %v", inURL, got, want)
	}

	// test that body was JSON encoded
	body, _ := ioutil.ReadAll(req.Body)
	if got, want := string(body), outBody; got != want {
		t.Errorf("NewRequest(%v) \nGot\n %v, \nwant\n %+v", inBody, got, want)
	}

	if got, want := req.Header.Get("Content-Type"), contentType; got != want {
		t.Errorf("NewRequest Content-Type is %v, want %+v", got, want)
	}
}

func TestNewRequest_invalidJSON(t *testing.T) {

	c, _ := NewClient(nil, defaultBaseURL)

	type T struct {
		A map[int]interface{}
	}
	_, err := c.newRequest("GET", "/", &T{})

	if err == nil {
		t.Error("Expected error to be returned.")

	}
	if err, ok := err.(*json.UnsupportedTypeError); !ok {
		t.Errorf("Expected a JSON error; got %#v.", err)
	}
}

func TestNewRequest_badURL(t *testing.T) {
	c, _ := NewClient(nil, defaultBaseURL)
	_, err := c.newRequest("GET", ":", nil)
	testURLParseError(t, err)
}

// If a nil body is passed to the API, make sure that nil is also
// passed to http.NewRequest.  In most cases, passing an io.Reader that returns
// no content is fine, since there is no difference between an HTTP request
// body that is an empty string versus one that is not set at all.  However in
// certain cases, intermediate systems may treat these differently resulting in
// subtle errors.
func TestNewRequest_emptyBody(t *testing.T) {
	c, _ := NewClient(nil, defaultBaseURL)
	req, err := c.newRequest("GET", "/", nil)
	if err != nil {
		t.Fatalf("NewRequest returned unexpected error: %v", err)
	}
	if req.Body != nil {
		t.Fatalf("constructed request contains a non-nil Body")
	}
}

func TestDo(t *testing.T) {
	setup()
	defer teardown()

	body := eventStructJSON

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if m := "POST"; m != r.Method {
			t.Errorf("Request method = %v, want %v", r.Method, m)
		}
		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, body)

	})

	req, _ := client.newRequest("POST", "/", nil)

	resp, err := client.do(req, nil)
	if err != nil {
		t.Errorf("An error ocurred %v", err)

	}

	want := &Response{Response: resp.Response, StatusCode: http.StatusCreated, StatusMessage: "201 Created"}

	if !reflect.DeepEqual(want, resp) {
		t.Errorf("Response not as expected. Got: %#v Want: %#v", resp, want)
	}

}

func TestErrorResponseContainsStatusCodeAndMessage(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)

		fmt.Fprintf(w, "Response Body")

	})

	req, _ := client.newRequest("POST", "/", nil)

	_, err := client.do(req, nil)

	if e, ok := err.(*ErrorResponse); ok {
		if e.StatusCode != http.StatusBadRequest {
			t.Errorf("Got %d Want %d", e.StatusCode, http.StatusBadRequest)
		}

		want := "400 Bad Request"
		if e.Message != want {
			t.Errorf("Got %s, Want %s", e.Message, want)
		}
	} else {
		t.Errorf("Expected error should be type *ErrorResponse")
	}

}

func TestNewResponse(t *testing.T) {

	r := http.Response{
		Status:     "201 Created",
		StatusCode: http.StatusCreated,
	}

	resp := newResponse(&r)

	if got, want := resp.Status, "201 Created"; got != want {
		t.Errorf("Status Message Incorrect. Got: %s Want: %s", got, want)
	}

	if got, want := resp.StatusCode, http.StatusCreated; got != want {
		t.Errorf("Status Code Incorrect. Got: %s Want: %s", got, want)
	}

}
