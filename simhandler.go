package goes

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jetbasrawi/goes/internal/atom"
)

type invalidVersionError int

func (i invalidVersionError) Error() string {
	return fmt.Sprintf("%d is not a valid event number", i)
}

type esRequest struct {
	Host      string
	Stream    string
	Direction string
	Version   *StreamVersion
	PageSize  int
}

type ESHandler struct {
	Events   []*Event
	BaseURL  *url.URL
	MetaData *Event
}

func (h ESHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	ru := r.URL
	if !ru.IsAbs() {
		ru = h.BaseURL.ResolveReference(ru)
	}

	// Feed Request
	fr, err := regexp.Compile("(?:streams\\/[^\\/]+\\/(?:head|\\d+)\\/(?:forward|backward)\\/\\d+)|(?:streams\\/[^\\/]+$)")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if fr.MatchString(ru.String()) {
		//fmt.Printf("\nHandling URL: %s\n", r.URL.String())
		f, err := createTestFeed(h.Events, ru.String())
		if err != nil {
			if serr, ok := err.(invalidVersionError); ok {
				http.Error(w, serr.Error(), http.StatusBadRequest)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
		//fmt.Println(f.PrettyPrint())
		fmt.Fprint(w, f.PrettyPrint())
	}

	//Event request
	er, err := regexp.Compile("streams\\/[^\\/]+\\/\\d+\\/?$")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if er.MatchString(ru.String()) {
		e, err := resolveEvent(h.Events, ru.String())
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		er, err := createTestEventAtomResponse(e, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprint(w, er.PrettyPrint())
	}

	//Metadata request
	mr, err := regexp.Compile("streams\\/[^\\/]+\\/metadata")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if mr.MatchString(ru.String()) {
		if h.MetaData == nil {
			fmt.Fprint(w, "{}")
			return
		}
		m, err := createTestEventAtomResponse(h.MetaData, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprint(w, m.PrettyPrint())
	}
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

func createTestEventResponse(e *Event, tm *TimeStr) (*EventResponse, error) {

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

	return r, nil
}

func createTestEventAtomResponse(e *Event, tm *TimeStr) (*eventAtomResponse, error) {

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

func getSliceSection(es []*Event, ver *StreamVersion, pageSize int, direction string) ([]*Event, bool, bool) {

	if len(es) < 1 {
		//TODO
	}

	if ver != nil && ver.Number < 0 {

		return nil, false, false

	}

	var start, end int
	var f, l bool

	switch direction {
	case "forward":
		if ver == nil {
			start = 0
		} else {
			start = ver.Number
			if ver.Number > es[len(es)-1].EventNumber {
				return []*Event{}, true, false // Out of range over
			} else if ver.Number < es[0].EventNumber {
				return []*Event{}, false, true //Out of range under
			}
		}
		//if start + pageSize exceeds the last item, set end to be last item
		end = int(math.Min(float64(start+pageSize), float64(len(es))))

	case "backward", "":
		if ver == nil {
			end = len(es)
		} else {
			end = ver.Number + 1
		}
		//if end - pagesize is less than first item return first item
		start = int(math.Max(float64(end-(pageSize)), 0.0))

	}

	if start <= 0 {
		l = true
	}
	if end >= len(es)-1 {
		f = true
	}

	if l {
		return es[:end], f, l
	} else if f {
		return es[start:], f, l
	} else {
		return es[start:end], f, l
	}
}

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
			r.Version = &StreamVersion{Number: int(i)}
		}
		r.Direction = split[3]
		p, err := strconv.ParseInt(split[4], 0, 0)
		if err != nil {
			return nil, err
		} else {
			r.PageSize = int(p)
		}
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

func createTestFeed(es []*Event, feedURL string) (*atom.Feed, error) {

	r, err := parseURL(feedURL)
	if err != nil {
		return nil, err
	}

	var prevVersion int
	var nextVersion int
	var lastVersion int

	s, _, isLast := getSliceSection(es, r.Version, r.PageSize, r.Direction)
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
