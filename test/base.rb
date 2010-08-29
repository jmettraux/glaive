
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
  TCPSocket.new('127.0.0.1', 5555)
end

