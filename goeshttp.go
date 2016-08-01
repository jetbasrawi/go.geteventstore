// Copyright 2016 Jet Basrawi. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package goes

import (
	"fmt"
	"reflect"
)

type invalidVersionError int

func (i invalidVersionError) Error() string {
	return fmt.Sprintf("%d is not a valid event number", i)
}

// NoMoreEventsError is returned when there are no events to return
// from a request to a stream.
type NoMoreEventsError struct{}

func (e NoMoreEventsError) Error() string {
	return "There are no more events to load."
}

// NotFoundError is returned when a stream is not found.
type NotFoundError struct {
	ErrorResponse *ErrorResponse
}

func (e NotFoundError) Error() string {
	return "The stream does not exist."
}

// UnauthorizedError is returned when a request to the eventstore is
// not authorized
type UnauthorizedError struct {
	ErrorResponse *ErrorResponse
}

func (e UnauthorizedError) Error() string {
	return "You are not authorised to access the stream or the stream does not exist."
}

// TemporarilyUnavailableError is returned when the server returns ServiceUnavailable.
//
// This error may be returned if a request is made to the server during startup. When
// the server starts up initially and the client is completely unable to connect to the
// server a *url.Error will be returned. Once the server is up but not ready to serve
// requests a ServiceUnavailable error will be returned for a brief period.
type TemporarilyUnavailableError struct {
	ErrorResponse *ErrorResponse
}

func (e TemporarilyUnavailableError) Error() string {
	return "Server Is Not Ready"
}

// UnexpectedError is returned when a request to the eventstore returns an error that
// is not explicitly represented by a goes Error type such as UnauthorisedError or
// NotFoundError
type UnexpectedError struct {
	ErrorResponse *ErrorResponse
}

func (e UnexpectedError) Error() string {
	return "An unexpected error occurred."
}

// BadRequestError is returned when the server returns a bad request error
type BadRequestError struct {
	ErrorResponse *ErrorResponse
}

func (e BadRequestError) Error() string {
	return "Bad request."
}

// ConcurrencyError is returned when the server returns a bad request error
type ConcurrencyError struct {
	ErrorResponse *ErrorResponse
}

func (e ConcurrencyError) Error() string {
	return "Concurrency Error."
}

// typeOf is a helper to get the names of types.
func typeOf(i interface{}) string {
	if i == nil {
		return ""
	}
	return reflect.TypeOf(i).Elem().Name()
}
