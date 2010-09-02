# encoding: utf-8

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

  def test_get_many__descending

    load_frogs

    assert_equal(
      %w[ frog9 frog8 frog7 frog6 frog5 frog4 frog3 frog2 frog10 frog1 frog0 ],
      @con.get_many('frogs', :descending => true).collect { |f| f['_id'] })
  end

  def test_get_many__offset

    load_frogs

    assert_equal(
      [ { '_rev' => 1, '_id' => 'frog8', 'type' => 'frogs' },
        { '_rev' => 1, '_id' => 'frog9', 'type' => 'frogs' } ],
      @con.get_many('frogs', :offset => 9))
  end

  def test_get_many__limit

    load_frogs

    assert_equal(
      [ { '_rev' => 1, '_id' => 'frog0', 'type' => 'frogs' },
        { '_rev' => 1, '_id' => 'frog1', 'type' => 'frogs' } ],
      @con.get_many('frogs', :limit => 2))
  end

  def test_get_many__offset_and_limit

    load_frogs

    assert_equal(
      [ { '_rev' => 1, '_id' => 'frog2', 'type' => 'frogs' },
        { '_rev' => 1, '_id' => 'frog3', 'type' => 'frogs' } ],
      @con.get_many('frogs', :offset => 3, :limit => 2))

    assert_equal(
      [ { '_rev' => 1, '_id' => 'frog1', 'type' => 'frogs' },
        { '_rev' => 1, '_id' => 'frog10', 'type' => 'frogs' } ],
      @con.get_many('frogs', :offset => 1, :limit => 2))
    assert_equal(
      [ { '_rev' => 1, '_id' => 'frog1', 'type' => 'frogs' },
        { '_rev' => 1, '_id' => 'frog10', 'type' => 'frogs' } ],
      @con.get_many('frogs', :skip => 1, :limit => 2))
  end

  def test_get_many__offset_limit_and_descending

    load_frogs

    assert_equal(
      [ { '_rev' => 1, '_id' => 'frog6', 'type' => 'frogs' },
        { '_rev' => 1, '_id' => 'frog5', 'type' => 'frogs' } ],
      @con.get_many('frogs', :offset => 3, :limit => 2, :descending => true))
  end

  def test_get_many_regex

    @con.put({ 'type' => 'people', '_id' => 'john' })
    @con.put({ 'type' => 'people', '_id' => 'jami' })
    @con.put({ 'type' => 'people', '_id' => 'james' })
    @con.put({ 'type' => 'people', '_id' => 'jeff' })
    @con.put({ 'type' => 'people', '_id' => 'jeremy' })
    @con.put({ 'type' => 'people', '_id' => 'jethro' })

    assert_equal(
      %w[ jeff jeremy jethro ],
      @con.get_many('people', '/^je/').collect { |pe| pe['_id'] })
  end

  def test_get_many_regex_utf8

    @con.put({ 'type' => 'places', '_id' => '横浜' })
    @con.put({ 'type' => 'places', '_id' => '品川' })
    @con.put({ 'type' => 'places', '_id' => 'london' })

    assert_equal(
      %w[ 品川 ],
      @con.get_many('places', '川').collect { |pe| pe['_id'] })

    assert_equal(
      %w[ 品川 ],
      @con.get_many('places', '/川$/').collect { |pe| pe['_id'] })
  end

  protected

  def load_frogs

    11.times do |i|
      @con.put({ 'type' => 'frogs', '_id' => "frog#{i}" })
    end
  end
end

