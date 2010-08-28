package main

import (
	"fmt"
	"net"
	"os"
	"bytes"
)

type message struct {
	message string
	channel chan *message
}

var bus = make(chan *message, 77)

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

    serviceChannel := make(chan *message)

	fmt.Fprintf(os.Stdout, "serving %s\n", con.RemoteAddr().String())

	line, _ := readUntilCrLf(con)

	//fmt.Printf(string(line))
	bus <- &message{ string(line), serviceChannel }
	//bus <- &message{ message: "a" }

    answer := <-serviceChannel

    con.Write([]byte(answer.message))
}

func listen() {

	addr, err := net.ResolveTCPAddr("127.0.0.1:5555")
	if err != nil {
		panic("failed to resolve TCP address")
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		panic("failed to listen on TCP address")
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

func dictionary() {
	for {
		m := <-bus
		//fmt.Printf("BUS : %s", m.message)
        m.channel <- &message{ m.message, nil }
	}
}

func main() {

	go dictionary()
	listen()
}
