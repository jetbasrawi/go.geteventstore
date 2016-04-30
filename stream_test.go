// Copyright 2016 Jet Basrawi. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package goes

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

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

func Test_GetMetaReturnsNilWhenStreamMetaDataIsEmpty(t *testing.T) {
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

	setupSimulator(es, m)

	got, _, _ := client.GetStreamMetaData(stream)
	if !reflect.DeepEqual(got.PrettyPrint(), want.PrettyPrint()) {
		t.Errorf("Got: %s\n Want %s\n ", got.PrettyPrint(), want.PrettyPrint())
	}
}

func TestAppendStreamMetadata(t *testing.T) {

	setup()
	defer teardown()

	eventType := "MetaData"
	stream := "Some-Stream"

	// Before updating the metadata, the method needs to get the MetaData url
	// According to the docs, the eventstore team reserve the right to change
	// the metadata url.
	fURL := fmt.Sprintf("/streams/%s/head/backward/100", stream)
	fullURL := fmt.Sprintf("%s%s", server.URL, fURL)
	mux.HandleFunc(fURL, func(w http.ResponseWriter, r *http.Request) {
		es := createTestEvents(1, stream, server.URL, eventType)
		f, _ := createTestFeed(es, fullURL)
		fmt.Fprint(w, f.PrettyPrint())
	})

	meta := fmt.Sprintf("{\"baz\":\"boo\"}")
	want := json.RawMessage(meta)

	url := fmt.Sprintf("/streams/%s/metadata", stream)

	mux.HandleFunc(url, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")

		var got json.RawMessage
		ev := &Event{Data: &got}
		err := json.NewDecoder(r.Body).Decode(ev)
		if err != nil {
			t.Errorf("Unexpected error %#v", err)
		}

		if !reflect.DeepEqual(string(got), string(want)) {
			t.Errorf("Got\n %#v\n Want\n %#v\n", string(got), string(want))
		}

		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, "")
	})

	resp, err := client.UpdateStreamMetaData(stream, &want)
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
