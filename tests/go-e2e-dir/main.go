package main

import (
	"fmt"
	"os"

	"C"
)
import "time"

func main() {
	dir, err := os.ReadDir("/app")
	if err != nil {
		fmt.Printf("Reading dir error: %s\n", err)
		os.Exit(-1)
	}
	fmt.Printf("DirEntries: %s\n", dir)
	if len(dir) != 2 {
		os.Exit(-1)
	}
	if dir[0].Name() != "test.txt" || dir[1].Name() != "app.py"{
		os.Exit(-1)
	}
	// let close requests be sent for test
	time.Sleep(1 * time.Second)
	os.Exit(0)
}
