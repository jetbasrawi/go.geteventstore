package main

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"golang.org/x/tools/blog/atom"
)

type ReadDirection int

const (
	baseUrl = "http://localhost:2113/streams/"

	Forward ReadDirection = iota
	Backward
)

//go:generate stringer -type=ReadDirection

func main() {

	readStreamForward("TopicSource-StackOverflow", nil, nil)
}

type StreamVersion struct {
	Number int
}

type EventCount struct {
	Number int
}

func readStreamForward(stream string, version *StreamVersion, count *EventCount) {

	var v int
	var c int

	if version != nil {
		v = version.Number
	} else {
		v = 0
	}

	if count != nil {
		c = count.Number
	} else {
		c = getCurrentVersion(stream).Number
	}

	url := fmt.Sprintf("%s%s/%d/forward/%d", baseUrl, stream, v, c)
	readPageForward(url)
}

func getCurrentVersion(stream string) *StreamVersion {

	url := fmt.Sprintf("%s%s", baseUrl, stream)
	feed, _ := getFeed(url)
	e := feed.Entry[0]

	s := strings.Split(e.ID, "/")
	v := s[len(s)-1]

	n, _ := strconv.Atoi(v)

	return &StreamVersion{Number: n}
}

func readFeedBackward(streamName string, version int) {

	url := fmt.Sprintf("%s%s/%d/backward/20", baseUrl, streamName, version)
	readPageBackward(url)
}

func getFeed(url string) (*atom.Feed, error) {

	resp, err := http.Get(url + "?format=atom")
	if err != nil {
		return nil, err
	}

	feed := &atom.Feed{}

	if err := xml.NewDecoder(resp.Body).Decode(&feed); err != nil {
		return nil, err
	}

	return feed, nil
}

func readPageForward(url string) {

	feed, _ := getFeed(url)

	for i := len(feed.Entry) - 1; i >= 0; i-- {
		entry := feed.Entry[i]
		fmt.Printf("%s\n", entry.Title)
	}

	{
		for _, v := range feed.Link {
			if v.Rel == "previous" {
				readPageForward(v.Href)
			}
		}
	}
}

func readPageBackward(url string) {

	feed, _ := getFeed(url)

	for _, v := range feed.Entry {

		fmt.Printf("%s\n", v.Title)

	}

	{
		for _, v := range feed.Link {
			if v.Rel == "next" {
				readPageBackward(v.Href)
			}
		}
	}

}
