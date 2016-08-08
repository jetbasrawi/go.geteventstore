// Copyright 2016 Jet Basrawi. All rights reserved.
//
// Use of this source code is governed by a permissive BSD 3 Clause License
// that can be found in the license file.

package goes_test

import (
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/jetbasrawi/go.geteventstore"
	"github.com/jetbasrawi/go.geteventstore.testfeed"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

var (

	// mux is the HTTP request multiplexer used with the test server
	mux *http.ServeMux

	// eventStoreClient is the EventStore client being tested
	client *goes.Client

	// server is a test HTTP server used to provide mock API responses
	server *httptest.Server

	_ = Suite(&GoesSuite{})
)

type GoesSuite struct{}

const (
	defaultBaseURL = "http://somedomain:2113/"
)

func (s *GoesSuite) SetUpTest(c *C) {
	setup()
}

func (s *GoesSuite) TearDownTest(c *C) {
	teardown()
}

func setup() {
	mux = http.NewServeMux()
	server = httptest.NewServer(mux)
	client, _ = goes.NewClient(nil, server.URL)
}

func setupSimulator(es []*mock.Event, m *mock.Event) {
	u, _ := url.Parse(server.URL)
	handler, err := mock.NewAtomFeedSimulator(es, u, m, -1)
	if err != nil {
		log.Fatal(err)
	}
	mux.Handle("/", handler)
}

func teardown() {
	server.Close()
}
