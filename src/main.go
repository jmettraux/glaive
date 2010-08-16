package main

import (
	"fmt"
	"net"
	"os"
	"bytes"
)

func die(msg string, err os.Error) {
	fmt.Fprintf(os.Stderr, "fatal error : %s%v\n", msg, err)
	os.Exit(1)
}
func error(msg string, err os.Error) {
	fmt.Fprintf(os.Stderr, "error : %s%v\n", msg, err)
}

func readUntilCrLf(con *net.TCPConn) (line []byte, err os.Error) {

	buf := make([]byte, 1)
	var data []byte
	crSeen := false

	for {
		_, err := con.Read(buf)
		if err != nil {
			if err == os.EOF {
				break
			} else {
				return nil, err
			}
		}
		if crSeen {
			if buf[0] == 10 {
				break
			} else {
				crSeen = false
				data = bytes.Add(data, buf)
			}
		} else {
			if buf[0] == 13 {
				crSeen = true
			} else {
				data = bytes.Add(data, buf)
			}
		}
	}

	return data, nil
}

func serve(con *net.TCPConn) {

	defer con.Close()

	line, _ := readUntilCrLf(con)

	fmt.Printf(string(line))
}

func listen() {

	addr, err := net.ResolveTCPAddr("127.0.0.1:5555")
	if err != nil {
		die("failed to resolve TCP address", err)
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		die("failed to listen on TCP address", err)
	}

	for {
		con, err := listener.AcceptTCP()
		if err != nil {
			error("problem with new connection", err)
		} else {
			go serve(con)
		}
	}
}

func main() {
	listen()
}
