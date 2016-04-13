package goes

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
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
		t.Errorf("An error ocurred %v", err.Error())

	}

	want := &Response{Response: resp.Response, StatusCode: http.StatusCreated, StatusMessage: "201 Created"}

	if !reflect.DeepEqual(want, resp) {
		t.Errorf("Response not as expected. Got: %#v Want: %#v", resp, want)
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

type MyDataType struct {
	Field1 int    `json:"my_field_1"`
	Field2 string `json:"my_field_2"`
}

func TestNewEvent(t *testing.T) {

	uuid, _ := NewUUID()
	eventType := "MyEventType"
	data := &MyDataType{Field1: 555, Field2: "Some string"}
	want := &Event{EventID: uuid, EventType: eventType, Data: data}

	got := client.NewEvent(uuid, eventType, data)

	if !reflect.DeepEqual(got, want) {
		t.Error("Error creating event. Got %+v, Want %+v", got, want)
	}

}

func TestGetMetaDataEmpty(t *testing.T) {
	setup()
	defer teardown()

	stream := "Some-Stream"

	es := createTestEvents(10, stream, server.URL, "EventTypeX")

	setupSimulator(es, nil)

	got, resp, err := client.GetStreamMetaData(stream)
	if err != nil {
		t.Errorf("Unexpected error: %s\n", err)
	}

	if got != nil {
		t.Errorf("Want %v\n, Got: %v\n", nil, got)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Got: %d Want %d\n", resp.StatusCode, http.StatusOK)
	}
}

func TestGetMetaData(t *testing.T) {
	setup()
	defer teardown()

	d := fmt.Sprintf("{ \"foo\" : %d }", rand.Intn(9999))
	raw := json.RawMessage(d)

	stream := "Some-Stream"

	es := createTestEvents(10, stream, server.URL, "EventTypeX")
	m := createTestEvent(stream, server.URL, "metadata", 10, &raw, nil)
	want, _ := createTestEventResponse(m, nil)

	//fmt.Printf("M: %+v", m)

	setupSimulator(es, m)

	got, _, _ := client.GetStreamMetaData(stream)
	if !reflect.DeepEqual(got.PrettyPrint(), want.PrettyPrint()) {
		t.Errorf("Got: %s\n Want %s\n ", got.PrettyPrint(), want.PrettyPrint())
	}
	//fmt.Printf("Got: %s\n", got.PrettyPrint())
	//fmt.Printf("Want: %s\n", want.PrettyPrint())

}

func TestAppendEventMetadata(t *testing.T) {

	setup()
	defer teardown()

	et := "SomeEventType"
	stream := "Some-Stream"

	fURL := fmt.Sprintf("/streams/%s/head/backward/100", stream)
	fullURL := fmt.Sprintf("%s%s", server.URL, fURL)
	mux.HandleFunc(fURL, func(w http.ResponseWriter, r *http.Request) {
		es := createTestEvents(1, stream, server.URL, et)
		f, _ := createTestFeed(es, fullURL)
		fmt.Fprint(w, f.PrettyPrint())
	})

	ml := fmt.Sprintf("{\"baz\": \"boo\"}")
	m := json.RawMessage(ml)
	evID, _ := NewUUID()
	meta := &MetaData{EventID: evID, EventType: et, Data: &m}

	url := fmt.Sprintf("/streams/%s/metadata", stream)

	mux.HandleFunc(url, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")

		var mg json.RawMessage
		g := MetaData{Data: &mg}
		_ = json.NewDecoder(r.Body).Decode(&g)
		i, _ := json.MarshalIndent(g, "", "	")
		fmt.Println(i)

		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, "")
	})

	resp, err := client.UpdateMetaData(stream, meta)
	if err != nil {
		t.Errorf("UpdateMetadata returned error: %v", err)
	}

	if resp.StatusMessage != "201 Created" {
		t.Errorf("Status Message incorrect. Got: %s, want %s", resp.StatusMessage, "201 Created")
	}

	if resp.StatusCode != http.StatusCreated {
		t.Errorf("Status Message incorrect. Got: %d, want %d", resp.StatusCode, 201)
	}

}
func TestAppendEventsSingle(t *testing.T) {

	setup()
	defer teardown()

	data := &MyDataType{Field1: 445, Field2: "Some string"}
	et := "SomeEventType"

	ev := client.NewEvent("", et, data)

	stream := "Some-Stream"

	url := fmt.Sprintf("/streams/%s", stream)

	mux.HandleFunc(url, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")

		b, _ := ioutil.ReadAll(r.Body)

		se := []Event{}
		err := json.NewDecoder(bytes.NewReader(b)).Decode(&se)
		if err != nil {
			t.Error("Unexpected error decoding event data")
		}

		gtev := se[0]
		if !reflect.DeepEqual(gtev.PrettyPrint(), ev.PrettyPrint()) {
			t.Errorf("Data not parsed correctly. Got %#v, want %#v", gtev, ev)
		}

		mt := "application/vnd.eventstore.events+json"
		mediaType := r.Header.Get("Content-Type")
		if mediaType != mt {
			t.Errorf("EventType not set correctly on header. Got %s, want %s", mediaType, mt)
		}

		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, "")
	})

	resp, err := client.AppendToStream(stream, nil, ev)
	if err != nil {
		t.Errorf("Events.PostEvent returned error: %v", err)
	}

	if resp.StatusMessage != "201 Created" {
		t.Errorf("Status Message incorrect. Got: %s, want %s", resp.StatusMessage, "201 Created")
	}

	if resp.StatusCode != http.StatusCreated {
		t.Errorf("Status Message incorrect. Got: %d, want %d", resp.StatusCode, 201)
	}

}
func TestAppendEventsMultiple(t *testing.T) {

	setup()
	defer teardown()

	et := "SomeEventType"
	d1 := &MyDataType{Field1: 445, Field2: "Some string"}
	d2 := &MyDataType{Field1: 446, Field2: "Some other string"}
	ev1 := client.NewEvent("", et, d1)
	ev2 := client.NewEvent("", et, d2)

	stream := "Some-Stream"
	url := fmt.Sprintf("/streams/%s", stream)

	mux.HandleFunc(url, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")

		b, _ := ioutil.ReadAll(r.Body)

		se := []Event{}
		err := json.NewDecoder(bytes.NewReader(b)).Decode(&se)
		if err != nil {
			t.Error("Unexpected error decoding event data")
		}

		gt1 := se[0]
		if !reflect.DeepEqual(gt1.PrettyPrint(), ev1.PrettyPrint()) {
			t.Errorf("Data not parsed correctly. Got %#v, want %#v", gt1, ev1)
		}
		gt2 := se[1]
		if !reflect.DeepEqual(gt2.PrettyPrint(), ev2.PrettyPrint()) {
			t.Errorf("Data not parsed correctly. Got %#v, want %#v", gt2, ev2)
		}

		mt := "application/vnd.eventstore.events+json"
		mediaType := r.Header.Get("Content-Type")
		if mediaType != mt {
			t.Errorf("EventType not set correctly on header. Got %s, want %s", mediaType, mt)
		}

		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, "")
	})

	resp, err := client.AppendToStream(stream, nil, ev1, ev2)
	if err != nil {
		t.Errorf("Events.PostEvent returned error: %v", err)
	}

	if resp.StatusMessage != "201 Created" {
		t.Errorf("Status Message incorrect. Got: %s, want %s", resp.StatusMessage, "201 Created")
	}

	if resp.StatusCode != http.StatusCreated {
		t.Errorf("Status Message incorrect. Got: %d, want %d", resp.StatusCode, 201)
	}

}

func TestAppendEventsWithExpectedVersion(t *testing.T) {

	setup()
	defer teardown()

	data := &MyDataType{Field1: 445, Field2: "Some string"}
	et := "SomeEventType"
	ev := client.NewEvent("", et, data)

	stream := "Some-Stream"
	url := fmt.Sprintf("/streams/%s", stream)

	expectedVersion := &StreamVersion{Number: 5}

	mux.HandleFunc(url, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")

		want := strconv.Itoa(expectedVersion.Number)
		got := r.Header.Get("ES-ExpectedVersion")
		if got != want {
			t.Errorf("Expected Version not set correctly on header. Got %s, want %s", got, want)
		}

		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "")
	})

	resp, err := client.AppendToStream(stream, expectedVersion, ev)
	if err == nil {
		t.Error("Expecting an error")
	}

	if resp.StatusMessage != "400 Bad Request" {
		t.Errorf("Status Message incorrect. Got: %s, want %s", resp.StatusMessage, "400 Bad Request")
	}

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("Status Message incorrect. Got: %d, want %d", resp.StatusCode, 400)
	}

}

func TestGetAtomPage(t *testing.T) {
	setup()
	defer teardown()

	stream := "some-stream"
	path := fmt.Sprintf("/streams/%s/head/backward/20", stream)
	url := fmt.Sprintf("%s%s", server.URL, path)

	es := createTestEvents(2, stream, server.URL, "EventTypeX")
	f, _ := createTestFeed(es, url)
	want := f.PrettyPrint()

	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")

		fmt.Fprint(w, want)
		if r.Header.Get("Accept") != "application/atom+xml" {
			t.Errorf("Accepts header incorrect. Got %s want %s", r.Header.Get("Accept"), "application/atom+xml")
		}
	})

	feed, resp, _ := client.readFeed(url)
	if !reflect.DeepEqual(resp.StatusCode, http.StatusOK) {
		t.Errorf("Incorrect response code returned. Got %d Want %d", resp.StatusCode, http.StatusOK)
	}

	got := feed.PrettyPrint()

	if !reflect.DeepEqual(got, want) {
		t.Errorf("Feed returned was incorrect. \nGot: %+v\n\n Want %+v\n", got, want)
	}

}

func TestUnmarshalFeed(t *testing.T) {
	setup()
	defer teardown()

	stream := "unmarshal-feed"
	count := 2

	es := createTestEvents(count, stream, server.URL, "EventTypeX")
	url := fmt.Sprintf("%s/streams/%s/head/backward/%d", server.URL, stream, count)

	wf, _ := createTestFeed(es, url)
	want := wf.PrettyPrint()

	gf, err := unmarshalFeed(strings.NewReader(want))
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	got := gf.PrettyPrint()

	if !reflect.DeepEqual(got, want) {
		t.Errorf("Error unmarshalling Feed: \nGot \n%+v \n want %+v\n", got, want)
	}

}

func TestGetEvent(t *testing.T) {
	setup()
	defer teardown()

	stream := "GetEventStream"
	es := createTestEvents(1, stream, server.URL, "SomeEventType")
	ti := Time(time.Now())

	want, _ := createTestEventResponse(es[0], &ti)

	er, _ := createTestEventAtomResponse(es[0], &ti)
	str := er.PrettyPrint()

	mux.HandleFunc("/streams/some-stream/299", func(w http.ResponseWriter, r *http.Request) {

		got := r.Header.Get("Accept")
		want := "application/vnd.eventstore.atom+json"

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got: %s Want: %s", got, want)
		}

		fmt.Fprint(w, str)
	})

	got, _, err := client.GetEvent("/streams/some-stream/299")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if !reflect.DeepEqual(got.PrettyPrint(), want.PrettyPrint()) {
		t.Errorf("Got\n %+v\n Want\n %+v", got.PrettyPrint(), want.PrettyPrint())
	}
}

func TestGetEventURLs(t *testing.T) {

	es := createTestEvents(2, "some-stream", "http://localhost:2113", "EventTypeX")
	f, _ := createTestFeed(es, "http://localhost:2113/streams/some-stream/head/backward/2")

	got, err := getEventURLs(f)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	want := []string{
		"http://localhost:2113/streams/some-stream/1",
		"http://localhost:2113/streams/some-stream/0",
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("Got:\n %+v\n Want:\n %+v\n", got, want)
	}
}

func TestRunServer(t *testing.T) {

	setup()
	defer teardown()

	stream := "astream"
	es := createTestEvents(100, stream, server.URL, "EventTypeA", "EventTypeB")

	setupSimulator(es, nil)

	_, _, err := client.ReadFeedBackward(stream, nil, nil)
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}

}

func TestReadFeedBackwardError(t *testing.T) {
	setup()
	defer teardown()

	stream := "ABigStream"
	errWant := errors.New("Stream Does Not Exist")
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {

		http.Error(w, errWant.Error(), http.StatusNotFound)

	})

	_, resp, err := client.ReadFeedBackward(stream, nil, nil)
	if err == nil {
		t.Errorf("Got %v Want %v", err, errWant)
	}

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("Got %v Want %v", resp.StatusCode, http.StatusNotFound)
	}
}

func TestReadFeedBackwardFromVersionAll(t *testing.T) {
	setup()
	defer teardown()

	stream := "ABigStream"
	ne := 1000
	es := createTestEvents(ne, stream, server.URL, "EventTypeX")

	setupSimulator(es, nil)

	ver := &StreamVersion{Number: 100}

	evs, _, err := client.ReadFeedBackward(stream, ver, nil)
	if err != nil {
		t.Errorf("Unexpected Error %s", err.Error())
	}

	nex := ver.Number + 1

	if len(evs) != nex {
		t.Errorf("Got: %d Want: %d", len(evs), nex)
		return
	}

	if evs[0].Event.EventNumber != ver.Number {
		t.Errorf("Got %d Want %d", evs[0].Event.EventNumber, ver.Number)
	}

	if evs[len(evs)-1].Event.EventNumber != 0 {
		t.Errorf("Got %d Want %d", evs[len(evs)-1].Event.EventNumber, 0)
	}

	for k, v := range evs {
		ex := (nex - 1) - k
		if v.Event.EventNumber != ex {
			t.Errorf("Got %d Want %d", v.Event.EventNumber, ex)
		}
		//fmt.Printf("%d\n %s", k, v.PrettyPrint())
	}

}

func TestReadFeedBackwardAll(t *testing.T) {
	setup()
	defer teardown()

	stream := "ABigStream"
	ne := 1000
	es := createTestEvents(ne, stream, server.URL, "EventTypeX")

	setupSimulator(es, nil)

	evs, _, err := client.ReadFeedBackward(stream, nil, nil)
	if err != nil {
		t.Errorf("Unexpected Error %s", err.Error())
	}

	if len(evs) != ne {
		t.Errorf("Got: %d Want: %d", len(evs), ne)
		return
	}

	if evs[0].Event.EventNumber != ne-1 {
		t.Errorf("Got %d Want %d", evs[0].Event.EventNumber, ne-1)
	}

	if evs[len(evs)-1].Event.EventNumber != 0 {
		t.Errorf("Got %d Want %d", evs[len(evs)-1].Event.EventNumber, 0)
	}

	for k, v := range evs {
		ex := (ne - 1) - k
		if v.Event.EventNumber != ex {
			t.Errorf("Got %d Want %d", v.Event.EventNumber, ex)
		}
		//fmt.Printf("%d\n %s", k, v.PrettyPrint())
	}

}

func TestReadFeedForwardError(t *testing.T) {
	setup()
	defer teardown()

	stream := "ABigStream"
	errWant := errors.New("Stream Does Not Exist")
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {

		http.Error(w, errWant.Error(), http.StatusNotFound)

	})

	_, resp, err := client.ReadFeedForward(stream, nil, nil)
	if err == nil {
		t.Errorf("Got %v Want %v", err, errWant)
	}

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("Got %v Want %v", resp.StatusCode, http.StatusNotFound)
	}
}
func TestReadFeedBackwardFromVersionWithTake(t *testing.T) {
	setup()
	defer teardown()

	stream := "ABigStream"
	ne := 1000
	es := createTestEvents(ne, stream, server.URL, "EventTypeX")

	setupSimulator(es, nil)

	ver := &StreamVersion{Number: 667}
	take := &Take{Number: 14}

	evs, _, err := client.ReadFeedBackward(stream, ver, take)
	if err != nil {
		t.Errorf("Unexpected Error %s", err.Error())
	}

	nex := take.Number

	if len(evs) != nex {
		t.Errorf("Got: %d Want: %d", len(evs), nex)
	}

	lstn := ver.Number - (take.Number - 1)

	if evs[0].Event.EventNumber != ver.Number {
		t.Errorf("Got %d Want %d", evs[0].Event.EventNumber, ver.Number)
	}

	if evs[len(evs)-1].Event.EventNumber != lstn {
		t.Errorf("Got %d Want %d", evs[len(evs)-1].Event.EventNumber, lstn)
	}

	for k, v := range evs {
		ex := ver.Number - k
		if v.Event.EventNumber != ex {
			t.Errorf("Got %d Want %d", v.Event.EventNumber, ex)
		}
		//fmt.Printf("%d\n %s", k, v.PrettyPrint())
	}

}

func TestReadFeedBackwardFromVersionWithTakeOutOfRangeUnder(t *testing.T) {
	setup()
	defer teardown()

	stream := "ABigStream"
	ne := 1000
	es := createTestEvents(ne, stream, server.URL, "EventTypeX")

	setupSimulator(es, nil)

	ver := &StreamVersion{Number: 49}
	take := &Take{Number: 59}

	evs, _, err := client.ReadFeedBackward(stream, ver, take)
	if err != nil {
		t.Errorf("Unexpected Error %s", err.Error())
	}

	nex := ver.Number + 1

	if len(evs) != nex {
		t.Errorf("Got: %d Want: %d", len(evs), nex)
	}

	lstn := 0

	if evs[0].Event.EventNumber != ver.Number {
		t.Errorf("Got %d Want %d", evs[0].Event.EventNumber, ver.Number)
	}

	if evs[len(evs)-1].Event.EventNumber != lstn {
		t.Errorf("Got %d Want %d", evs[len(evs)-1].Event.EventNumber, lstn)
	}

	for k, v := range evs {
		ex := ver.Number - k
		if v.Event.EventNumber != ex {
			t.Errorf("Got %d Want %d", v.Event.EventNumber, ex)
		}
		//fmt.Printf("%d\n %s", k, v.PrettyPrint())
	}

}

//Try to get versions past head of stream that do not yet exist
//this use case is used to poll head of stream waiting for new events
func TestReadFeedForwardTail(t *testing.T) {
	setup()
	defer teardown()

	stream := "ABigStream"
	ne := 1000
	es := createTestEvents(ne, stream, server.URL, "EventTypeX")

	setupSimulator(es, nil)

	ver := &StreamVersion{Number: 1000}

	evs, _, err := client.ReadFeedForward(stream, ver, nil)
	if err != nil {
		t.Errorf("Unexpected Error %s", err.Error())
	}

	nex := 0

	if len(evs) != nex {
		t.Errorf("Got: %d Want: %d", len(evs), nex)
	}

}

func TestGetFeedURLInvalidVersion(t *testing.T) {
	setup()
	defer teardown()

	stream := "ABigStream"

	ver := &StreamVersion{Number: -1}

	_, err := getFeedURL(stream, "forward", ver, nil)
	if got, ok := err.(invalidVersionError); !ok {
		want := invalidVersionError(ver.Number)
		t.Errorf("Got %#v\n Want %#v\n", got, want)
	}
}

func TestReadFeedForwardAll(t *testing.T) {
	setup()
	defer teardown()

	stream := "ABigStream"
	ne := 1000
	es := createTestEvents(ne, stream, server.URL, "EventTypeX")

	setupSimulator(es, nil)

	evs, _, err := client.ReadFeedForward(stream, nil, nil)
	if err != nil {
		t.Errorf("Unexpected Error %s", err.Error())
	}

	nex := ne

	if len(evs) != nex {
		t.Errorf("Got: %d Want: %d", len(evs), nex)
		return
	}

	lstn := ne - 1
	fstn := 0

	if evs[0].Event.EventNumber != fstn {
		t.Errorf("Got %d Want %d", evs[0].Event.EventNumber, fstn)
	}

	if evs[len(evs)-1].Event.EventNumber != lstn {
		t.Errorf("Got %d Want %d", evs[len(evs)-1].Event.EventNumber, lstn)
	}

	for k, v := range evs {
		ex := k
		if v.Event.EventNumber != ex {
			t.Errorf("Got %d Want %d", v.Event.EventNumber, ex)
		}
		//fmt.Printf("%d\n", v.EventNumber)
	}

}

func TestGetFeedURLForwardLowTake(t *testing.T) {
	want := "/streams/some-stream/0/forward/10"
	got, _ := getFeedURL("some-stream", "forward", nil, &Take{Number: 10})
	if got != want {
		t.Errorf("Got %s Want %s", got, want)
	}
}

func TestGetFeedURLBackwardLowTake(t *testing.T) {
	want := "/streams/some-stream/head/backward/15"
	got, _ := getFeedURL("some-stream", "backward", nil, &Take{Number: 15})
	if got != want {
		t.Errorf("Got %s Want %s", got, want)
	}
}

func TestGetFeedURLInvalidDirection(t *testing.T) {
	want := errors.New("Invalid Direction")
	_, got := getFeedURL("some-stream", "nonesense", nil, nil)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Got %s Want %s", got, want)
	}
}
func TestGetFeedURLBackwardNilAll(t *testing.T) {
	want := "/streams/some-stream/head/backward/100"
	got, _ := getFeedURL("some-stream", "", nil, nil)
	if got != want {
		t.Errorf("Got %s Want %s", got, want)
	}
}

func TestGetFeedURLForwardNilAll(t *testing.T) {
	want := "/streams/some-stream/0/forward/100"
	got, _ := getFeedURL("some-stream", "forward", nil, nil)
	if got != want {
		t.Errorf("Got %s Want %s", got, want)
	}
}

func TestGetFeedURLForwardVersioned(t *testing.T) {
	want := "/streams/some-stream/15/forward/100"
	got, _ := getFeedURL("some-stream", "forward", &StreamVersion{Number: 15}, nil)
	if got != want {
		t.Errorf("Got %s Want %s", got, want)
	}
}
