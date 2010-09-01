//
// Copyright (c) 2010, John Mettraux, jmettraux@gmail.com
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
//
// Made in Japan.
//

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
	"strconv"
	"container/list"
	"sort"
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
		return "failed to create dir(s)"
	}

	(*d)["_rev"] = rev + 1
	j, _ := json.Marshal(d)

	err = ioutil.WriteFile(fileFor(typ, id), j, 0755)
	if err != nil {
		return "failed to save file"
	}

	return rev + 1
}

func (r right) delete(rev int64) interface{} {

	doc := fetch(r.typ, r.id)

	if doc == nil {
		return -1
	}
	if doc.rev() != rev {
		return doc
	}
	err := os.Remove(fileFor(r.typ, r.id))
	if err != nil {
		return fmt.Sprintf("failed to remove %s/%s/%d", r.typ, r.id, rev)
	}
	return rev
}

//
// misc

func p(i interface{}) { fmt.Printf("%#v\n", i) }

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

func writeJson(con *net.TCPConn, data interface{}) {
	bytes, _ := json.Marshal(data)
	con.Write(bytes)
	con.Write([]byte("\r\n"))
}

//
// a sortable []string

type stringSlice []string

func (s stringSlice) Len() int {
	return len(s)
}

func (s stringSlice) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s stringSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
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

func doPurge(con *net.TCPConn, args []string) {

	err := os.RemoveAll(*dir)
	if err != nil {
		writeJson(con, fmt.Sprintf("something went wrong : %v", err))
	} else {
		writeJson(con, 0)
	}
}

func doGet(con *net.TCPConn, args []string) {

	if len(args) < 2 {
		writeJson(con, "usage : get {type} {id}")
		return
	}

	doc := fetch(args[0], args[1])

	writeJson(con, doc)
}

func listFiles(dirname string) []string {

	infos, err := ioutil.ReadDir(dirname)

	if err != nil {
		return []string{}
	}

	result := make([]string, len(infos))

	for index, info := range infos {
		result[index] = info.Name
	}

	return result
}

func listIds(args []string) stringSlice {

	if len(args) > 2 {
		return args[1:]
	}

	sds := listFiles(strings.Join([]string{*dir, args[0]}, "/"))

	l := list.New()

	for _, sd := range sds {

		is := listFiles(strings.Join([]string{*dir, args[0], sd}, "/"))

		for _, id := range is {
			l.PushBack(id)
		}
	}

	result := make(stringSlice, l.Len())
	e := l.Front()

	for i := 0; i < l.Len(); i++ {

		id, _ := e.Value.(string)
		result[i] = id[0 : len(id)-5]

		e = e.Next()
	}

	sort.Sort(result)

	return result
}

func doGetMany(con *net.TCPConn, args []string) {

	if len(args) < 1 {
		writeJson(con, "usage : get_many {type} {key}* [opts]")
		return
	}

	ids := listIds(args)

	docs := make([]interface{}, len(ids))
	i := 0

	for _, id := range ids {
		doc := fetch(args[0], id)
		if doc != nil {
			docs[i] = doc
			i = i + 1
		}
	}

	writeJson(con, docs)
}

func doIds(con *net.TCPConn, args []string) {

	if len(args) != 1 {
		writeJson(con, "usage : ids {type}")
	} else {
		writeJson(con, listIds(args))
	}
}

func doPut(con *net.TCPConn, args []string) {

	data, _ := readUntilCrLf(con)
	doc := new(document)
	err := json.Unmarshal(data, doc)

	if err != nil {
		writeJson(con, "failed to parse document, is it really a JSON Object ?")
		return
	}

	typ, id := doc.typ(), doc.id()

	if len(typ) < 1 || len(id) < 1 {
		writeJson(con, "document is missing a \"type\" and/or \"_id\" attribute")
		return
	}

	right := reserve(typ, id) // blocking
	result := right.put(doc)
	release(typ, id)

	writeJson(con, result)
}

func doDelete(con *net.TCPConn, args []string) {

	rev, err := strconv.Atoi64(args[2])
	if err != nil {
		writeJson(con, fmt.Sprintf("revision '%v' is not an integer", args[2]))
		return
	}
	typ, id := args[0], args[1]

	right := reserve(typ, id) // blocking
	result := right.delete(rev)
	release(typ, id)

	writeJson(con, result)
}

var commands = map[string]func(*net.TCPConn, []string){
	"put":      doPut,
	"get":      doGet,
	"get_many": doGetMany,
	"ids":      doIds,
	"purge":    doPurge,
	"delete":   doDelete}

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

				// TODO : waiting expiration...

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
			writeJson(con, "bye.")
			break
		}

		f, ok := commands[command]
		if ok {
			f(con, tokens[1:])
		} else {
			writeJson(con, fmt.Sprintf("unknown command '%s'", command))
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
