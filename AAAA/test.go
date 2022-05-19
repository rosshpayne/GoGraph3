package main

import (
	"fmt"
)

func main() {

	defer func() func() {
		fmt.Println("enter A")
		return func() {
			fmt.Println("Leave A")
		}
	}()()

	defer func() {
		fmt.Println(" Lave C ")
	}()

	defer func() func() {
		fmt.Println("enter B")
		return func() {
			fmt.Println("Leave B")
		}
	}()()

	fmt.Println("===== main =====")
}
