
require File.join(File.dirname(__FILE__), 'base.rb')


class GtBasicTest < Test::Unit::TestCase

  include GtBase

  def test_put
    assert_equal 1, @con.put({ 'type' => 'car', '_id' => 'bmw' })
  end

  def test_put_non_hash
    assert_equal(
      "failed to parse document, is it really a JSON Object ?",
      @con.put('nada'))
  end

  def test_put_missing_id
    assert_equal(
      "document is missing a \"type\" and/or \"_id\" attribute",
      @con.put({ 'type' => 'car' }))
  end

  def test_put_missing_type
    assert_equal(
      "document is missing a \"type\" and/or \"_id\" attribute",
      @con.put({ '_id' => 'surf' }))
  end

  def test_put_missing_type_and_id
    assert_equal(
      "document is missing a \"type\" and/or \"_id\" attribute",
      @con.put({ 'rank' => 'rise from it' }))
  end

  def test_put_new_rev
    assert_equal -1, @con.put({ 'type' => 'car', '_id' => 'bmw', '_rev' => 1 })
  end

  def test_put_reput
    assert_equal(
      1,
      @con.put({ 'type' => 'car', '_id' => 'bmw' }))
    assert_equal(
      { 'type' => 'car', '_id' => 'bmw', '_rev' => 1 },
      @con.put({ 'type' => 'car', '_id' => 'bmw' }))
  end
end

