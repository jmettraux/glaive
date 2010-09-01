
require File.join(File.dirname(__FILE__), 'base.rb')


class GtBasicTest < Test::Unit::TestCase

  include GtBase

  def test_quit
    @con.write("quit\r\n")
    assert_equal "\"bye.\"\r\n", @con.gets
    assert_nil @con.gets
  end

  def test_unknown_command
    assert_equal "unknown command 'nanka'", @con.emit("nanka")
  end

  def test_get
    assert_equal nil, @con.get('car', 'benz')
  end

  def test_delete_missing
    assert_equal -1, @con.delete('car', 'benz', 1)
  end

  def test_delete_wrong_rev
    assert_equal 1, @con.put({ 'type' => 'car', '_id' => 'bmw' })
    assert_equal -1, @con.delete('car', 'benz', 2)
  end

  def test_delete
    assert_equal 1, @con.put({ 'type' => 'car', '_id' => 'bmw' })
    assert_equal 1, @con.delete('car', 'bmw', 1)
  end

  def test_utf_8
    assert_equal(
      1,
      @con.put({ 'type' => 'car', '_id' => 'トヨタ' }))
    assert_equal(
      { 'type' => 'car', '_id' => 'トヨタ', '_rev' => 1 },
      @con.get('car', 'トヨタ'))
  end

  def test_ids

    @con.put({ 'type' => 'cars', '_id' => 'bmw' })
    @con.put({ 'type' => 'cars', '_id' => 'volkswagen' })
    @con.put({ 'type' => 'cars', '_id' => 'saab' })
    @con.put({ 'type' => 'cars', '_id' => 'alfaromeo' })

    assert_equal(
      %w[ alfaromeo bmw saab volkswagen ],
      @con.ids('cars'))
  end
end

