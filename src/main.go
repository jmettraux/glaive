package main

import (
	"fmt"
	"net"
	"os"
	"bytes"
	"strings"
	"json"
)

type document map[string]interface{}

type reservation struct {
	typ string
	id  string
}

type request struct {
	typ     string
	id      string
	channel chan *reservation
}

func (r reservation) put(d *document) (doc *document, failure bool) {
	return nil, true
}
func (r reservation) delete(rev int64) (doc *document, failure bool) {
	return nil, true
}

var reservationChannel = make(chan *request, 77)

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

//func writeLine(con *net.TCPConn, line string) {
//	con.Write([]byte(line))
//	con.Write([]byte("\r\n"))
//}
func writeJson(con *net.TCPConn, data *interface{}) {
	bytes, _ := json.Marshal(data)
	con.Write(bytes)
	con.Write([]byte("\r\n"))
}
func writeJsonString(con *net.TCPConn, s string) {
	con.Write([]byte(fmt.Sprintf("\"%s\"\r\n", s)))
}

//
// the commands

func doGet(con *net.TCPConn, args []string) {
	writeJsonString(con, args[0])
}
func doPut(con *net.TCPConn, args []string) {
	data, _ := readUntilCrLf(con)
	doc := new(document)
	json.Unmarshal(data, doc)
	//id, found := (*doc)["_id"]
	rev, found := (*doc)["_rev"]
	if !found {
		rev = 0
	}
	writeJson(con, &rev)
}

var commands = map[string]func(*net.TCPConn, []string){"get": doGet, "put": doPut}

//
// serving

func serve(con *net.TCPConn) {

	defer con.Close()

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
			//writeLine(con, "\"bye.\"")
			writeJsonString(con, "bye.")
			break
		}

		f, ok := commands[command]
		if ok {
			f(con, tokens[1:])
		} else {
			writeJsonString(con, fmt.Sprintf("unknown command '%s'", command))
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
		<-reservationChannel
		//m.channel <- &request{m.message, nil}
	}
}

func main() {

	go manageReservations()
	listen()
}
