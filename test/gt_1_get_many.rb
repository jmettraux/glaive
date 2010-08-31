
require File.join(File.dirname(__FILE__), 'base.rb')


class GtGetManyTest < Test::Unit::TestCase

  include GtBase

  def test_get_many

    @con.put({ 'type' => 'bike', '_id' => '2010', 'brand' => 'giant' })
    @con.put({ 'type' => 'bike', '_id' => '2011', 'brand' => 'arrowjp' })

    assert_equal(
      [
        { 'type' => 'bike', '_id' => '2010', 'brand' => 'giant' },
        { 'type' => 'bike', '_id' => '2011', 'brand' => 'arrowjp' }
      ],
      @con.get_many('bike'))
  end
end

