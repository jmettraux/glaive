
require File.join(File.dirname(__FILE__), 'base.rb')


class GtGetManyTest < Test::Unit::TestCase

  include GtBase

  def test_get_many

    @con.put({ 'type' => 'bikes', '_id' => '2010', 'brand' => 'giant' })
    @con.put({ 'type' => 'bikes', '_id' => '2011', 'brand' => 'arrowjp' })

    assert_equal(
      [
        { 'type' => 'bikes', '_id' => '2010', 'brand' => 'giant', '_rev' => 1 },
        { 'type' => 'bikes', '_id' => '2011', 'brand' => 'arrowjp', '_rev' => 1 }
      ],
      @con.get_many('bikes'))
  end

  def test_get_many_when_none

    assert_equal([], @con.get_many('boats'))
  end

  def test_get_many_when_gone

    @con.put({ 'type' => 'boats', '_id' => 'emma', 'brand' => 'liberty' })
    @con.delete('boats', 'emma', 1)

    assert_equal([], @con.get_many('boats'))
  end
end

