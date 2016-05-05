package goes

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"reflect"
	"strconv"
	"testing"
	"time"
)

type MyDataType struct {
	Field1 int    `json:"my_field_1"`
	Field2 string `json:"my_field_2"`
}

type MyMetaDataType struct {
	MetaField1 int    `json:"my_meta_field_1"`
	MetaField2 string `json:"my_meta_field_2"`
}

func TestNewEvent(t *testing.T) {
	uuid, _ := NewUUID()
	eventType := "MyEventType"
	data := &MyDataType{Field1: 555, Field2: "Some string"}
	meta := &MyMetaDataType{MetaField1: 1010, MetaField2: "Some meta string"}

	want := &Event{EventID: uuid, EventType: eventType, Data: data, MetaData: meta}

	got := client.ToEventData(uuid, eventType, data, meta)

	if !reflect.DeepEqual(got, want) {
		t.Error("Error creating event. Got %+v, Want %+v", got, want)
	}
}

func TestAppendEventsSingle(t *testing.T) {

	setup()
	defer teardown()

	data := &MyDataType{Field1: 445, Field2: "Some string"}
	et := "SomeEventType"

	ev := client.ToEventData("", et, data, nil)

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
	ev1 := client.ToEventData("", et, d1, nil)
	ev2 := client.ToEventData("", et, d2, nil)

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
	ev := client.ToEventData("", et, data, nil)

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

func createTestEvent(stream, server, eventType string, eventNumber int, data *json.RawMessage, meta *json.RawMessage) *Event {

	e := Event{}
	e.EventStreamID = stream
	e.EventNumber = eventNumber
	e.EventType = eventType

	uuid, _ := NewUUID()
	e.EventID = uuid

	e.Data = data

	u := fmt.Sprintf("%s/streams/%s", server, stream)
	eu := fmt.Sprintf("%s/%d/", u, eventNumber)
	l1 := Link{URI: eu, Relation: "edit"}
	l2 := Link{URI: eu, Relation: "alternate"}
	ls := []Link{l1, l2}
	e.Links = ls

	if meta != nil {
		e.MetaData = meta
	} else {
		m := "\"\""
		mraw := json.RawMessage(m)
		e.MetaData = &mraw
	}
	return &e

}

func createTestEvents(numEvents int, stream string, server string, eventTypes ...string) []*Event {

	se := []*Event{}

	for i := 0; i < numEvents; i++ {
		r := rand.Intn(len(eventTypes))
		eventType := eventTypes[r]

		d := fmt.Sprintf("{ \"foo\" : %d }", rand.Intn(9999))
		raw := json.RawMessage(d)

		uuid, _ := NewUUID()
		m := fmt.Sprintf("{\"bar\": \"%s\"}", uuid)
		mraw := json.RawMessage(m)

		e := createTestEvent(stream, server, eventType, i, &raw, &mraw)

		se = append(se, e)

	}
	return se
}
