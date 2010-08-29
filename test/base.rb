
require 'socket'
require 'test/unit'
require 'yajl'
require 'rufus-json'

def start_glaive(opts={})

  no_kill = opts.delete(:no_kill)

  cpid = Process.fork do
    exec './glaive -d data_test'
  end

  unless no_kill
    at_exit { Process.kill(9, cpid) }
  end

  cpid
end

def connect

  con = TCPSocket.new('127.0.0.1', 5555)

  class << con

    def parse_reply
      Rufus::Json.decode(gets)
    end
    def emit(head, body=nil)
      head = Array(head)
      write(head.join(' '))
      write("\r\n")
      if body
        write(body)
        write("\r\n")
      end
      parse_reply
    end

    def put(doc)
      emit('put', Rufus::Json.encode(doc))
    end

    def get(type, id)
      emit([ 'get', type, id ])
    end
  end

  con
end

