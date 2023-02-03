package main

import (
	"C"
	"fmt"
	"os"
	"syscall"
)

const TEXT = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."

func TestRead() {
	// Tests: SYS_read, SYS_openat
	dat, err := os.ReadFile("/app/test.txt")
	if err != nil {
		panic(err)
	}
	if string(dat) != TEXT {
		err := fmt.Errorf("Expected %s, got %s", TEXT, string(dat))
		panic(err)
	}
}

func TestWrite() {
	// Tests: SYS_write
	fileName := createTempFile()
	fmt.Println("±±±±±±±±±±±±±±±±±±±±±±±±±±±±±±±±± Created temp file ±±±±±±±±±±±±±±±±±±±±±±±±±±±±±±±±±")
	dat, err := os.ReadFile(fileName)
	fmt.Println("±±±±±±±±±±±±±±±±±±±±±±±±±±±±±±±±± Read temp file ±±±±±±±±±±±±±±±±±±±±±±±±±±±±±±±±±")
	if err != nil {
		panic(err)
	}
	if string(dat) != TEXT {
		err := fmt.Errorf("Expected %s, got %s", TEXT, string(dat))
		panic(err)
	}
}

func TestLseek() {
	// Tests: SYS_lseek
	fileName := createTempFile()
	file, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	buf := make([]byte, len(TEXT))
	file.Seek(0, 1)
	_, err = file.Read(buf)
	if err != nil {
		panic(err)
	}
	if string(buf) != TEXT {
		err := fmt.Errorf("Expected %s, got %s", TEXT, string(buf))
		panic(err)
	}
}

func TestFaccessat() {
	// Tests: SYS_faccessat
	// Access calls Faccess with _AT_FDCWD and flags set to 0
	fileName := createTempFile()
	err := syscall.Access(fileName, syscall.O_RDWR)
	if err != nil {
		panic(err)
	}
}

func createTempFile() string {
	file, err := os.CreateTemp("/tmp", "test")
	if err != nil {
		panic(err)
	}
	file.WriteString(TEXT)
	fileName := file.Name()
	file.Close()
	return fileName
}

func main() {
    fmt.Println("~~~~~~~~~~~~~~~~~~~~~~~~ Testing Read ~~~~~~~~~~~~~~~~~~~~~~~~~~~")
	TestRead()
    fmt.Println("~~~~~~~~~~~~~~~~~~~~~~~~ Testing Write ~~~~~~~~~~~~~~~~~~~~~~~~~~~")
	TestWrite()
    fmt.Println("~~~~~~~~~~~~~~~~~~~~~~~~ Testing LSeek ~~~~~~~~~~~~~~~~~~~~~~~~~~~")
	TestLseek()
    fmt.Println("~~~~~~~~~~~~~~~~~~~~~~~~ Testing FAccessAt ~~~~~~~~~~~~~~~~~~~~~~~~~~~")
	TestFaccessat()
}
