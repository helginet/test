package main

import (
	"fmt"
	"net"
	"os"
	"time"
)

func main() {
	_, err := net.DialTimeout("tcp", "127.0.0.1:9000", 5 * time.Second)
	if err != nil {
	/**
	* if I use firewall, I'm getting next:
	*
	* Error: dial tcp 127.0.0.1:9000: i/o timeout
	* exit status 1
	*
	*/
		fmt.Printf("Error: %s\n", err)
		os.Exit(1)
	}
	fmt.Println("Connected successfully")
}