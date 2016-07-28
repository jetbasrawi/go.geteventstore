package goes

import (
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
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

	// eventStoreClient is the EventStore client being tested
	eventStoreClient Client

	// server is a test HTTP server used to provide mack API responses
	server *httptest.Server
)

func setup() {
	mux = http.NewServeMux()
	server = httptest.NewServer(mux)

	baseURL, _ := url.Parse(server.URL)
	eventStoreClient = &client{
		client:  http.DefaultClient,
		baseURL: baseURL,
		headers: make(map[string]string),
	}
}

func setupSimulator(es []*Event, m *Event) {
	u, _ := url.Parse(server.URL)
	handler, err := NewAtomFeedSimulator(es, u, m, len(es))
	if err != nil {
		log.Fatal(err)
	}
	mux.Handle("/", handler)
}

func teardown() {
	server.Close()
}
