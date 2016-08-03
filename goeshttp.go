// Copyright 2016 Jet Basrawi. All rights reserved.
//
// Use of this source code is governed by a permissive MIT
// license that can be found in the LICENSE file.

// Package goes provides an abstraction over GetEventStore's HTTP API.
//
// The package simplifies reading and writing events to the HTTP API.
//
// The package provides a Client which contains foundation methods for
// connecting to and interacting with the eventstore.
//
// The package also provides StreamReader and StreamWriter types which
// provide methods for reading and writing events and metadata.
package goes

import (
	"reflect"

	"github.com/jetbasrawi/go.geteventstore/internal/uuid"
)

// typeOf is a helper to get the names of types.
func typeOf(i interface{}) string {
	if i == nil {
		return ""
	}
	return reflect.TypeOf(i).Elem().Name()
}

// NewUUID returns a new V4 uuid as a string.
func NewUUID() string {
	return uuid.NewV4().String()
}
