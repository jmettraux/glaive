
require File.join(File.dirname(__FILE__), 'base.rb')

class GetPutTest < Test::Unit::TestCase

  def setup
    $pid ||= start_glaive
    sleep 0.077
    @con = connect
    @con.write("purge\r\n")
    @con.gets
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
    assert_equal nil, @con.get('car', 'benz')
  end

  def test_put
    assert_equal 1, @con.put({ 'type' => 'car', '_id' => 'bmw' })
  end

  def test_put_new_rev
    assert_equal -1, @con.put({ 'type' => 'car', '_id' => 'bmw', '_rev' => 1 })
  end

  #def test_put_reput
  #  @con.write("put\r\n")
  #  @con.write(Rufus::Json.encode({
  #    'type' => 'car', '_id' => 'bmw'
  #  }))
  #  @con.write("\r\n")
  #end
end

