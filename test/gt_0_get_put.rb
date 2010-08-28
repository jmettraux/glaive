
require File.join(File.dirname(__FILE__), 'base.rb')

class GetPutTest < Test::Unit::TestCase

  def setup
    $pid ||= start_glaive
    sleep 0.077
    @con = connect
  end

  def teardown
    begin
      @con.close
    rescue
    ensure
      @con = nil
    end
  end

  def test_quit
    @con.write("quit\r\n")
    assert_equal "\"bye.\"\r\n", @con.gets
    assert_nil @con.gets
  end

  def test_unknown_command
    @con.write("nanka\r\n")
    assert_equal "\"unknown command 'nanka'\"\r\n", @con.gets
  end

  def test_get
    @con.write("get car\r\n")
    assert_equal "car\r\n", @con.gets
  end
end

