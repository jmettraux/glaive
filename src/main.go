package main

import (
	"fmt"
	"net"
	"os"
	"bytes"
	"strings"
	"json"
	"flag"
	"io/ioutil"
)

//
// command-line args

var host = flag.String("h", "127.0.0.1", "host")
var port = flag.Int("p", 5555, "port")
var dir = flag.String("d", "data", "glaive data dir")
var verbose = flag.Bool("v", false, "verbose")

//
// document

type document map[string]interface{}

func (d document) typ() string {
	v, _ := d["type"].(string)
	return v
}
func (d document) rev() int64 {
	v, _ := d["_rev"].(float64)
	return int64(v)
}
func (d document) id() string {
	v, _ := d["_id"].(string)
	return v
}

// TODO : at some point, introduce a cache

func pathFor(typ string, id string) string {
	subdir := id
	if len(id) > 1 {
		subdir = id[len(id)-2:]
	}
	return strings.Join([]string{*dir, typ, subdir}, "/")
}

func fileFor(typ string, id string) string {
	return strings.Join([]string{pathFor(typ, id), id + ".json"}, "/")
}

func fetch(typ string, id string) *document {
	data, err := ioutil.ReadFile(fileFor(typ, id))
	if err != nil {
		return nil
	}
	doc := new(document)
	json.Unmarshal(data, doc)
	return doc
}

//
// right

type right struct {
	typ     string
	id      string
	channel chan *right
}

func (r right) key() string {
	return r.typ + "//" + r.id
}

func (r right) put(d *document) interface{} {

	typ, id, rev := d.typ(), d.id(), d.rev()
	doc := fetch(typ, id)

	if doc == nil && rev != 0 {
		return -1
	}
	if doc != nil && doc.rev() != rev {
		return doc
	}
	err := os.MkdirAll(pathFor(typ, id), 0755)
	if err != nil {
		return -1
	}

	(*d)["_rev"] = rev + 1
	j, _ := json.Marshal(d)
	ioutil.WriteFile(fileFor(typ, id), j, 0755)

	return rev + 1
}

func (r right) delete(rev int64) *interface{} {
	return nil
}

//
// misc

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
func writeJson(con *net.TCPConn, data interface{}) {
	bytes, _ := json.Marshal(data)
	con.Write(bytes)
	con.Write([]byte("\r\n"))
}
func writeJsonString(con *net.TCPConn, s string) {
	con.Write([]byte(fmt.Sprintf("\"%s\"\r\n", s)))
}

//
// reserve and release

func reserve(typ string, id string) *right {
	feedback := make(chan *right)
	reserveChannel <- &right{typ, id, feedback}
	return <-feedback
}

func release(typ string, id string) {
	releaseChannel <- &right{typ, id, nil}
}

//
// the commands

func doGet(con *net.TCPConn, args []string) {
	doc := fetch(args[0], args[1])
	writeJson(con, doc)
}

func doPut(con *net.TCPConn, args []string) {
	data, _ := readUntilCrLf(con)
	doc := new(document)
	json.Unmarshal(data, doc)
	right := reserve(doc.typ(), doc.id()) // blocking
	result := right.put(doc)
	writeJson(con, result)
}

func doPurge(con *net.TCPConn, args []string) {
	err := os.RemoveAll(*dir)
	if err != nil {
		writeJson(con, fmt.Sprintf("something went wrong : %v", err))
	} else {
		writeJson(con, "ok")
	}
}

var commands = map[string]func(*net.TCPConn, []string){"put": doPut, "get": doGet, "purge": doPurge}

//
// reservations

var reserveChannel = make(chan *right)
var releaseChannel = make(chan *right)

func manageReservations() {
	var reserved = make(map[string]*right)
	var waiting = make(map[string]chan *right)
	for {
		select {
		case reservation := <-reserveChannel:

			key := reservation.key()
			_, ok := reserved[key]
			if ok {
				//
				// someone is already on it, let's wait
				//
				waiting[key] = reservation.channel, true
			} else {
				//
				// document is free, let's hand the reservation
				//
				r := &right{reservation.typ, reservation.id, nil}
				reserved[key] = r, true
				reservation.channel <- r
			}

		case release := <-releaseChannel:

			key := release.key()
			reserved[key] = nil, false
			channel, ok := waiting[key]
			if ok {
				//
				// there is someone waiting for a right on the document
				//
				waiting[key] = nil, false
				channel <- &right{release.typ, release.id, nil}
			}
		}
	}
}

//
// serving

func serve(con *net.TCPConn) {

	defer con.Close()

    if *verbose {
	  fmt.Fprintf(os.Stdout, "serving %s\n", con.RemoteAddr().String())
    }

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

	hostAndPort := fmt.Sprintf("%s:%d", *host, *port)

    if *verbose {
	  fmt.Printf("listening on %s\n", hostAndPort)
    }

	addr, err := net.ResolveTCPAddr(hostAndPort)
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

func main() {

	flag.Parse()

	go manageReservations()
	listen()
}
