package main

import (
	"fmt"

	goes "github.com/jetbasrawi/goes"
)

const (
	serverURL = "http://localhost:2113"
)

type MyDataType struct {
	MyField1 int    `json:"my_field_1"`
	MyField2 string `json:"my_field_2"`
}

func main() {

	client, _ := goes.NewClient(nil, serverURL)

	for i := 0; i < 100000; i++ {

		data := &MyDataType{MyField1: i, MyField2: "This is my data"}

		event := client.NewEvent("", "SomeEventType", data)

		resp, err := client.PostEvent(fmt.Sprintf("Stream_%d", i), event)
		if err != nil {
			fmt.Printf("\nError\n%v\n", err)
		}

		fmt.Printf("\nResponse\n%+v\n", resp)

	}

}
