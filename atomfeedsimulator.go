// The simhandler simulates the EventStore ATOM feeds for streams.

package goes

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/jetbasrawi/goes/internal/atom"
)

type esRequest struct {
	Host      string
	Stream    string
	Direction string
	Version   int
	PageSize  int
}

// AtomFeedSimulator is the type that stores configuration and state for
// the feed simulator
type AtomFeedSimulator struct {
	sync.Mutex
	Events       []*Event
	BaseURL      *url.URL
	MetaData     *Event
	feedRegex    *regexp.Regexp
	eventRegex   *regexp.Regexp
	metaRegex    *regexp.Regexp
	trickleAfter int
}

// NewAtomFeedSimulator consructs a new AtomFeedSimulator
//
// events is a slice of *Event that will be returned by the handler. The events are equivalent to the
// total number of events in a stream and these can be read and paged as you would read and page a stream
// in GetEventStore. The number of events must be greater than 0.
// baseURL is the base url of the test server.
// streamMeta is the stream metadata that should be returned if a request for metadata is made to the server.
// trickleAfter is used to simulate polling and the arrival of new events while polling. The simulator will
// return any events after the version specified by the trickleAfter argument. For example, if ten events are
// passed in and trickleAfter is set to 5, the first five events will be returned in a feed page and then a
// subsequent poll to the head of the stream will return no events. Set the LongPoll header and the simulator
// will return the next five events at some random interval between 0 and the value of the LongPoll header.
func NewAtomFeedSimulator(events []*Event, baseURL *url.URL, streamMeta *Event, trickleAfter int) (*AtomFeedSimulator, error) {

	if len(events) <= 0 {
		return nil, errors.New("Must provide one or more events.")
	}

	fs := &AtomFeedSimulator{
		Events:       events,
		BaseURL:      baseURL,
		MetaData:     streamMeta,
		trickleAfter: trickleAfter,
	}

	fr, err := regexp.Compile("(?:streams\\/[^\\/]+\\/(?:head|\\d+)\\/(?:forward|backward)\\/\\d+)|(?:streams\\/[^\\/]+$)")
	if err != nil {
		return nil, err
	}
	fs.feedRegex = fr

	er, err := regexp.Compile("streams\\/[^\\/]+\\/\\d+\\/?$")
	if err != nil {
		return nil, err
	}
	fs.eventRegex = er

	mr, err := regexp.Compile("streams\\/[^\\/]+\\/metadata")
	if err != nil {
		return nil, err
	}
	fs.metaRegex = mr

	return fs, nil
}

// ServeHTTP serves atom feed responses
func (h *AtomFeedSimulator) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	reqURL := r.URL
	if !reqURL.IsAbs() {
		reqURL = h.BaseURL.ResolveReference(reqURL)
	}

	// Feed Request
	if h.feedRegex.MatchString(reqURL.String()) {

		index := h.trickleAfter
		if index < 0 {
			index = 0
		}

		f, err := CreateTestFeed(h.Events[:index], reqURL.String())
		if err != nil {
			if serr, ok := err.(invalidVersionError); ok {
				http.Error(w, serr.Error(), http.StatusBadRequest)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}

		if len(f.Entry) <= 0 && r.Header.Get("ES-LongPoll") != "" {
			longPoll, err := strconv.Atoi(r.Header.Get("ES-LongPoll"))
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			h.Lock()
			h.trickleAfter++
			if h.trickleAfter > len(h.Events) {
				h.trickleAfter--
			}
			index := h.trickleAfter
			if index < 0 {
				index = 0
			}

			f, err = CreateTestFeed(h.Events[:index], reqURL.String())
			h.Unlock()
			if err != nil {
				if serr, ok := err.(invalidVersionError); ok {
					http.Error(w, serr.Error(), http.StatusBadRequest)
				} else {
					http.Error(w, err.Error(), http.StatusInternalServerError)
				}
				return
			}

			waitDuration := longPoll
			if len(f.Entry) > 0 {
				waitDuration = rand.Intn(longPoll)
			}
			time.Sleep(time.Duration(waitDuration) * time.Second)
		}
		fmt.Fprint(w, f.PrettyPrint())
	}

	//Event request
	if h.eventRegex.MatchString(reqURL.String()) {
		e, err := resolveEvent(h.Events, reqURL.String())
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		er, err := CreateTestEventAtomResponse(e, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprint(w, er.PrettyPrint())
	}

	//Metadata request
	if h.metaRegex.MatchString(reqURL.String()) {
		if h.MetaData == nil {
			fmt.Fprint(w, "{}")
			return
		}
		m, err := CreateTestEventAtomResponse(h.MetaData, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprint(w, m.PrettyPrint())
	}
}

// CreateTestFeed creates an atom feed object from the events passed in and the
// url provided.
//
// The function will take the events that correspond to the version number and
// page size in the url and return a feed object that contains those events.
// If the url defines a set larger than the events passed in the returned events
// will only contain the events available.
func CreateTestFeed(es []*Event, feedURL string) (*atom.Feed, error) {
	if len(es) <= 0 {
		spew.Dump(es)
		fmt.Println(feedURL)
	}

	r, err := parseURL(feedURL)
	if err != nil {
		return nil, err
	}

	var prevVersion int
	var nextVersion int
	var lastVersion int

	s, _, isLast, isHead := getSliceSection(es, r.Version, r.PageSize, r.Direction)
	sr := reverseEventSlice(s)

	lastVersion = es[0].EventNumber

	if len(s) > 0 {
		nextVersion = s[0].EventNumber - 1
		prevVersion = sr[0].EventNumber + 1
	} else {
		nextVersion = es[len(es)-1].EventNumber
		prevVersion = -1
	}

	f := &atom.Feed{}

	f.Title = fmt.Sprintf("Event stream '%s'", r.Stream)
	f.Updated = atom.Time(time.Now())
	f.Author = &atom.Person{Name: "EventStore"}

	u := fmt.Sprintf("%s/streams/%s", r.Host, r.Stream)
	l := []atom.Link{}
	l = append(l, atom.Link{Href: u, Rel: "self"})
	l = append(l, atom.Link{Href: fmt.Sprintf("%s/head/backward/%d", u, r.PageSize), Rel: "first"})

	if !isLast { // On every page except last page
		l = append(l, atom.Link{Href: fmt.Sprintf("%s/%d/forward/%d", u, lastVersion, r.PageSize), Rel: "last"})
		l = append(l, atom.Link{Href: fmt.Sprintf("%s/%d/backward/%d", u, nextVersion, r.PageSize), Rel: "next"})
	}

	if prevVersion >= 0 {
		l = append(l, atom.Link{Href: fmt.Sprintf("%s/%d/forward/%d", u, prevVersion, r.PageSize), Rel: "previous"})
	}
	l = append(l, atom.Link{Href: fmt.Sprintf("%s/metadata", u), Rel: "metadata"})
	f.Link = l

	if isHead {
		f.HeadOfStream = true
	}

	f.StreamID = r.Stream

	for _, v := range sr {
		e := &atom.Entry{}
		e.Title = fmt.Sprintf("%d@%s", v.EventNumber, r.Stream)
		e.ID = v.EventStreamID
		e.Updated = atom.Time(time.Now())
		e.Author = &atom.Person{Name: "EventStore"}
		e.Summary = &atom.Text{Body: v.EventType}
		e.Link = append(e.Link, atom.Link{Rel: "edit", Href: v.Links[0].URI})
		e.Link = append(e.Link, atom.Link{Rel: "alternate", Href: v.Links[0].URI})
		f.Entry = append(f.Entry, e)
	}

	return f, nil
}

// CreateTestEventFromData returns test events derived from the user specified data
//
// Should be used where you require the simulator to return events of your own type
// with your own content.
func CreateTestEventFromData(stream, server string, eventNumber int, data interface{}, meta interface{}) *Event {
	e := Event{}
	e.EventStreamID = stream
	e.EventNumber = eventNumber
	e.EventType = reflect.TypeOf(data).Elem().Name()

	uuid := NewUUID()
	e.EventID = uuid

	b, _ := json.Marshal(data)
	var d json.RawMessage
	d = json.RawMessage(b)
	e.Data = &d

	u := fmt.Sprintf("%s/streams/%s", server, stream)
	eu := fmt.Sprintf("%s/%d/", u, eventNumber)
	l1 := Link{URI: eu, Relation: "edit"}
	l2 := Link{URI: eu, Relation: "alternate"}
	ls := []Link{l1, l2}
	e.Links = ls

	if meta != nil {
		mb, _ := json.Marshal(meta)
		var m json.RawMessage
		m = json.RawMessage(mb)
		e.MetaData = &m
	} else {
		m := "\"\""
		mraw := json.RawMessage(m)
		e.MetaData = &mraw
	}
	return &e
}

// CreateTestEvent will generate a test event.
//
// The event data and meta will be a *json.RawMessage.
// The type of the event returned will be derived from the eventType argument.
// The event will have a single field named Foo which will contain random content
// which is simply a uuid string.
// The meta returned will contain a single field named Bar which will also contain
// a uuid string.
func CreateTestEvent(stream, server, eventType string, eventNumber int, data *json.RawMessage, meta *json.RawMessage) *Event {
	e := Event{}
	e.EventStreamID = stream
	e.EventNumber = eventNumber
	e.EventType = eventType

	uuid := NewUUID()
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

// CreateTestEvents will return a slice of random test events.
//
// The types of the events will be randomly selected from the event type names passed in to the
// variadic argument eventTypes
func CreateTestEvents(numEvents int, stream string, server string, eventTypes ...string) []*Event {
	se := []*Event{}
	for i := 0; i < numEvents; i++ {
		r := rand.Intn(len(eventTypes))
		eventType := eventTypes[r]

		uuid := NewUUID()
		d := fmt.Sprintf("{ \"foo\" : \"%s\" }", uuid)
		raw := json.RawMessage(d)

		m := fmt.Sprintf("{\"bar\": \"%s\"}", uuid)
		mraw := json.RawMessage(m)

		e := CreateTestEvent(stream, server, eventType, i, &raw, &mraw)

		se = append(se, e)
	}
	return se
}

// CreateTestEventResponse will return an *EventResponse containing the event provided in the
// argument e.
//
// The Updated field of the EventResponse will be set to the value of ht TimeString tm if it is
// provided otherwise it will be set to time.Now
func CreateTestEventResponse(e *Event, tm *TimeStr) *EventResponse {

	timeStr := Time(time.Now())
	if tm != nil {
		timeStr = *tm
	}

	r := &EventResponse{
		Title:   fmt.Sprintf("%d@%s", e.EventNumber, e.EventStreamID),
		ID:      e.Links[0].URI,
		Updated: timeStr,
		Summary: e.EventType,
		Event:   e,
	}

	return r
}

// CreateTestEventResponses will return a slice of *EventResponse containing the events provided in the
// argument events.
//
// The Updated field of the EventResponse will be set to the value of ht TimeString tm if it is
// provided otherwise it will be set to time.Now
func CreateTestEventResponses(events []*Event, tm *TimeStr) []*EventResponse {
	ret := make([]*EventResponse, len(events))
	for k, v := range events {
		ret[k] = CreateTestEventResponse(v, tm)
	}
	return ret
}

// CreateTestEventAtomResponse returns an *eventAtomResponse derived from the *Event argument e.
//
// The updated time of the response will be set to the value of the *TimeStr argument tm. If tm is
// nil then the updated time will be set to now.
func CreateTestEventAtomResponse(e *Event, tm *TimeStr) (*eventAtomResponse, error) {

	b, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	raw := json.RawMessage(b)

	timeStr := Time(time.Now())
	if tm != nil {
		timeStr = *tm
	}

	r := &eventAtomResponse{
		Title:   fmt.Sprintf("%d@%s", e.EventNumber, e.EventStreamID),
		ID:      e.Links[0].URI,
		Updated: timeStr,
		Summary: e.EventType,
		Content: &raw,
	}

	return r, nil
}

func getSliceSection(es []*Event, ver int, pageSize int, direction string) (events []*Event, isFirst bool, isLast bool, isHead bool) {

	if len(es) < 1 {
		return []*Event{}, false, false, true
	}

	if ver < 0 {
		return nil, false, false, false
	}

	var start, end int

	switch direction {
	case "forward":
		if ver == 0 {
			start = 0
		} else {
			start = ver
			if ver > es[len(es)-1].EventNumber {
				return []*Event{}, true, false, true // Out of range over
			} else if ver < es[0].EventNumber {
				return []*Event{}, false, true, false //Out of range under
			}
		}
		//if start + pageSize exceeds the last item, set end to be last item
		end = int(math.Min(float64(start+pageSize), float64(len(es))))

	case "backward", "":
		if ver == 0 {
			end = len(es)
		} else {
			end = ver + 1
		}
		//if end - pagesize is less than first item return first item
		start = int(math.Max(float64(end-(pageSize)), 0.0))
	}

	if start <= 0 {
		isLast = true
	}
	if end >= len(es)-1 {
		isFirst = true
	}
	if end > len(es)-1 {
		isHead = true
	}

	if isLast {
		events = es[:end]
	} else if isFirst {
		events = es[start:]
	} else {
		events = es[start:end]
	}

	return
}

// Extracts relevant parameters from URL and returns them in an esRequest
func parseURL(u string) (*esRequest, error) {

	r := esRequest{}

	ru, err := url.Parse(u)
	if err != nil {
		return nil, err
	}
	r.Host = ru.Scheme + "://" + ru.Host

	split := strings.Split(strings.TrimLeft(ru.Path, "/"), "/")
	r.Stream = split[1]

	if len(split) > 2 {
		i, err := strconv.ParseInt(split[2], 0, 0)
		if err == nil {
			if i < 0 {
				return nil, invalidVersionError(i)
			}
			r.Version = int(i)
		}
		r.Direction = split[3]
		p, err := strconv.ParseInt(split[4], 0, 0)
		if err != nil {
			return nil, err
		}
		r.PageSize = int(p)
	} else {
		r.Direction = "backward"
		r.PageSize = 20
	}

	return &r, nil

}

func reverseEventSlice(s []*Event) []*Event {
	r := []*Event{}
	for i := len(s) - 1; i >= 0; i-- {
		r = append(r, s[i])
	}
	return r
}

func resolveEvent(events []*Event, url string) (*Event, error) {

	r, err := regexp.Compile("\\d+$")
	if err != nil {
		return nil, err
	}

	str := r.FindString(strings.TrimRight(url, "/"))
	i, err := strconv.ParseInt(str, 0, 0)
	if err != nil {
		return nil, err
	}
	return events[i], nil
}
