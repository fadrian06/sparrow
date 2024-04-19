<?php

namespace Tests;

use PHPUnit\Framework\TestCase;
use Sparrow;

final class QueryBuilderExamplesTest extends TestCase {
  /** @var Sparrow */
  private $sparrow;

  function setUp()
  {
    $this->sparrow = new Sparrow;
    $this->sparrow->from('user');
  }

  function testCanBuildASelectAllQuery() {
    $result = $this->sparrow->select()->sql();
    $expected = 'SELECT * FROM user';

    self::assertSame($expected, $result);
  }

  function testCanBuildASingleWhereCondition() {
    $result = $this->sparrow->where('id', 123)->select()->sql();
    $expected = 'SELECT * FROM user WHERE id=123';

    self::assertSame($expected, $result);
  }

  function testCanBuildMultipleWhereConditions() {
    $result = $this->sparrow
      ->where('id', 123)
      ->where('name', 'bob')
      ->select()
      ->sql();

    $expected = "SELECT * FROM user WHERE id=123 AND name='bob'";

    self::assertSame($expected, $result);
  }

  function testCanBuildAWhereConditionFromAnArray() {
    $where = array('id' => 123, 'name' => 'bob');
    $result = $this->sparrow->where($where)->select()->sql();
    $expected = "SELECT * FROM user WHERE id=123 AND name='bob'";

    self::assertSame($expected, $result);
  }

  function testCanBuildAWhereConditionFromAString() {
    $result = $this->sparrow->where('id = 99')->select()->sql();
    $expected = 'SELECT * FROM user WHERE id = 99';

    self::assertSame($expected, $result);
  }

  function testCanBuildAWhereConditionWithACustomOperator() {
    $result = $this->sparrow->where('id >', 123)->select()->sql();
    $expected = 'SELECT * FROM user WHERE id>123';

    self::assertSame($expected, $result);
  }

  function testCanBuildAnOrWhereCondition() {
    $result = $this->sparrow
      ->where('id <', 10)
      ->where('|id >', 20)
      ->select()
      ->sql();

    $expected = 'SELECT * FROM user WHERE id<10 OR id>20';

    self::assertSame($expected, $result);
  }

  function testCanBuildAWhereConditionWithLikeOperator() {
    $result = $this->sparrow
      ->where('name %', '%bob%')
      ->select()
      ->sql();

    $expected = "SELECT * FROM user WHERE name LIKE '%bob%'";

    self::assertSame($expected, $result);
  }

  function testCanBuildAWhereConditionWithANotLikeOperator() {
    $result = $this->sparrow
      ->where('name !%', '%bob%')
      ->select()
      ->sql();

    $expected = "SELECT * FROM user WHERE name NOT LIKE '%bob%'";

    self::assertSame($expected, $result);
  }

  function testCanBuildAWhereConditionWithInOperator() {
    $result = $this->sparrow
      ->where('id @', array(10, 20, 30))
      ->select()
      ->sql();

    $expected = 'SELECT * FROM user WHERE id IN (10,20,30)';

    self::assertSame($expected, $result);
  }

  function testCanBuildAWhereConditionWithNotInOperator() {
    $result = $this->sparrow
      ->where('id !@', array(10, 20, 30))
      ->select()
      ->sql();

    $expected = 'SELECT * FROM user WHERE id NOT IN (10,20,30)';

    self::assertSame($expected, $result);
  }

  function testCanBuildASelectQueryWithSpecifiedFields() {
    $result = $this->sparrow->select(array('id', 'name'))->sql();
    $expected = 'SELECT id,name FROM user';

    self::assertSame($expected, $result);
  }

  function testCanBuildASelectQueryWithALimitAndOffset() {
    $result = $this->sparrow->limit(10)->offset(20)->select()->sql();
    $expected = 'SELECT * FROM user LIMIT 10 OFFSET 20';

    self::assertSame($expected, $result);
  }

  function testCanBuildASelectQueryWithALimitAndOffsetFromTheSelectMethod() {
    $result = $this->sparrow->select('*', 50, 10)->sql();
    $expected = 'SELECT * FROM user LIMIT 50 OFFSET 10';

    self::assertSame($expected, $result);
  }

  function testCanBuildASelectQueryWithADistinctField() {
    $result = $this->sparrow->distinct()->select('name')->sql();
    $expected = 'SELECT DISTINCT name FROM user';

    self::assertSame($expected, $result);
  }

  function testCanBuildASimpleTableJoinQuery() {
    $result = $this->sparrow
      ->join('role', array('role.id' => 'user.role_id'))
      ->select()
      ->sql();

    $expected = 'SELECT * FROM user  INNER JOIN role ON role.id=user.role_id';

    self::assertSame($expected, $result);
  }

  function testCanBuildATableJoinQueryWithMultipleConditionsAndCustomOperators() {
    $result = $this->sparrow
      ->join('role', array('role.id' => 'user.role_id', 'role.id >' => 10))
      ->select()
      ->sql();

    $expected = 'SELECT * FROM user  INNER JOIN role ON role.id=user.role_id AND role.id>10';

    self::assertSame($expected, $result);
  }

  function testCanBuildADescSortedSelectQuery() {
    $result = $this->sparrow
      ->sortDesc('id')
      ->select()
      ->sql();

    $expected = 'SELECT * FROM user ORDER BY id DESC';

    self::assertSame($expected, $result);
  }

  function testCanBuildAnAscSortedMultipleFieldsSelectQuery() {
    $result = $this->sparrow
      ->sortAsc(array('rank', 'name'))
      ->select()
      ->sql();

    $expected = 'SELECT * FROM user ORDER BY rank ASC, name ASC';

    self::assertSame($expected, $result);
  }

  function testCanBuildASimpleGroupBySelectQuery() {
    $result = $this->sparrow
      ->groupBy('points')
      ->select(array('id', 'count(*)'))
      ->sql();

    $expected = 'SELECT id,count(*) FROM user GROUP BY points';

    self::assertSame($expected, $result);
  }

  function testCanBuildASimpleInsertQuery() {
    $data = array('id' => 123, 'name' => 'bob');
    $result = $this->sparrow
      ->insert($data)
      ->sql();

    $expected = "INSERT INTO user (id,name) VALUES (123,'bob')";

    self::assertSame($expected, $result);
  }

  function testCanBuildASimpleUpdateQuery() {
    $data = array('name' => 'bob', 'email' => 'bob@aol.com');
    $where = array('id' => 123);

    $result = $this->sparrow
      ->where($where)
      ->update($data)
      ->sql();

    $expected = "UPDATE user SET name='bob',email='bob@aol.com' WHERE id=123";

    self::assertSame($expected, $result);
  }

  function testCanBuildASimpleDeleteQuery() {
    $result = $this->sparrow
      ->where('id', 123)
      ->delete()
      ->sql();

    $expected = "DELETE FROM user WHERE id=123";

    self::assertSame($expected, $result);
  }
}
