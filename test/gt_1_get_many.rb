
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

  def test_get_many_multiple_ids

    load_frogs

    assert_equal(
      3,
      @con.get_many('frogs', [ 'frog1', 'frog3', 'frog11' ]).size)
    assert_equal(
      %w[ frog2 frog4 frog7 ],
      @con.get_many('frogs', %w[ frog2 frog4 frog7 ]).collect { |h| h['_id'] })
  end

  def test_get_many_descending

    load_frogs

    assert_equal(
      %w[ frog9 frog8 frog7 frog6 frog5 frog4 frog3 frog2 frog10 frog1 frog0 ],
      @con.get_many('frogs', :descending => true).collect { |f| f['_id'] })
  end

  def test_get_many_skip

    load_frogs

    assert_equal(
      [],
      @con.get_many('frogs', :skip => 9))
  end

  protected

  def load_frogs

    11.times do |i|
      @con.put({ 'type' => 'frogs', '_id' => "frog#{i}" })
    end
  end
end

