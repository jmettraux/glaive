
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

    def get_many(type, key=nil)
      a = [ 'get_many', type ]
      a << key if key
      emit(a)
    end

    def purge
      emit('purge')
    end

    def delete(type, id, rev)
      emit([ 'delete', type, id, rev ])
    end
  end

  con
end

module GtBase

  def setup
    unless $pid
      $pid = start_glaive
      sleep 0.077
    end
    @con = connect
    @con.purge
  end

  def teardown
    begin
      @con.close
    rescue
    ensure
      @con = nil
    end
  end
end

