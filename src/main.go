package main

import (
	"fmt"
	"net"
	"os"
	"bytes"
	"strings"
    "json"
)

type message struct {
	message string
	channel chan *message
}

var reservationChannel = make(chan *message, 77)

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

func writeLine(con *net.TCPConn, line string) {
	con.Write([]byte(line))
	con.Write([]byte("\r\n"))
}

func doGet(con *net.TCPConn, args []string) {
	writeLine(con, args[0])
}
func doPut(con *net.TCPConn, args []string) {
    data, _ := readUntilCrLf(con)
    j := new(map[string]interface{})
    fmt.Printf("%#v\n", *j)
    json.Unmarshal(data, j)
    fmt.Printf("%#v\n", *j)
    for k, _ := range *j {
      fmt.Printf("key : %#v %T\n", k, k)
      v, _ := (*j)[k]
      fmt.Printf("val : %#v", v)
    }
    //id, _ := *j["_id"]
    //fmt.Printf("_id : %v", id)
	writeLine(con, "0")
}

var commands = map[string]func(*net.TCPConn, []string){ "get": doGet, "put": doPut }

func serve(con *net.TCPConn) {

	fmt.Fprintf(os.Stdout, "serving %s\n", con.RemoteAddr().String())

	for {

		line, err := readUntilCrLf(con)

		if err != nil {
			// TODO : pass error message
			con.Write([]byte("\"internal error\"\r\n"))
			continue
		}

		tokens := strings.Split(string(line), " ", -1)

		command := tokens[0]

		if command == "quit" {
			writeLine(con, "\"bye.\"")
			con.Close()
			break
		}

		f, ok := commands[command]
		if ok {
			f(con, tokens[1:])
		} else {
			writeLine(
				con, fmt.Sprintf("\"unknown command '%s'\"", command))
		}
	}
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

func manageReservations() {
	for {
		m := <-reservationChannel
		m.channel <- &message{m.message, nil}
	}
}

func main() {

	go manageReservations()
	listen()
}
