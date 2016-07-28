// Copyright 2016 Jet Basrawi. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package goes

import (
	"reflect"

	. "gopkg.in/check.v1"
)

var _ = Suite(&EventSuite{})

type EventSuite struct{}

func (s *EventSuite) SetUpTest(c *C) {
	setup()
}
func (s *EventSuite) TearDownTest(c *C) {
	teardown()
}

type MyDataType struct {
	Field1 int    `json:"my_field_1"`
	Field2 string `json:"my_field_2"`
}

type MyMetaDataType struct {
	MetaField1 int    `json:"my_meta_field_1"`
	MetaField2 string `json:"my_meta_field_2"`
}

func (s *EventSuite) TestToEventData(c *C) {
	uuid := NewUUID()
	eventType := "MyEventType"
	data := &MyDataType{Field1: 555, Field2: "Some string"}
	meta := &MyMetaDataType{MetaField1: 1010, MetaField2: "Some meta string"}
	want := &Event{EventID: uuid, EventType: eventType, Data: data, MetaData: meta}

	got := ToEventData(uuid, eventType, data, meta)

	c.Assert(got, DeepEquals, want)
}

func (s *EventSuite) TestToEventDataCreatesEventIDIfNotProvided(c *C) {
	eventType := "MyEventType"
	data := &MyDataType{Field1: 555, Field2: "Some string"}
	meta := &MyMetaDataType{MetaField1: 1010, MetaField2: "Some meta string"}

	got := ToEventData("", eventType, data, meta)

	c.Assert(got.EventID, Not(Equals), "")
}

func (s *EventSuite) TestToEventDataUsesTypeNameAsEventTypeIfNotProvided(c *C) {
	uuid := NewUUID()
	data := &MyDataType{Field1: 555, Field2: "Some string"}
	meta := &MyMetaDataType{MetaField1: 1010, MetaField2: "Some meta string"}

	got := ToEventData(uuid, "", data, meta)

	c.Assert(got.EventType, DeepEquals, reflect.TypeOf(data).Elem().Name())
}
