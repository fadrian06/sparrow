<?php

namespace Tests;

use PHPUnit\Framework\TestCase;
use Sparrow;

final class QueriesExecutionExamplesTest extends TestCase {
  /** @var Sparrow */
  private $sparrow;

  function setUp(): void {
    $this->sparrow = new Sparrow;
    $this->sparrow->setDb('sqlite://' . __DIR__ . '/Northwind.db');
    $this->sparrow->from('OrderDetails');
  }

  function testCanFetchMultipleRecords() {
    $details = $this->sparrow->where('OrderDetailID >', 100)->many();

    $expected = array(
      'OrderDetailID' => 101,
      'OrderID' => 10285,
      'ProductID' => 40,
      'Quantity' => 40
    );

    self::assertCount(418, $details);
    self::assertSame($expected, $details[0]);
  }

  function testCanFetchOneRecord() {
    $details = $this->sparrow->where('OrderDetailID', 123)->one();

    $expected = array(
      'OrderDetailID' => 123,
      'OrderID' => 10293,
      'ProductID' => 75,
      'Quantity' => 6
    );

    self::assertSame($expected, $details);
  }

  function testCanFetchASingleRecordValue() {
    $orderID = $this->sparrow->where('OrderDetailID', 123)->value('OrderID');

    self::assertSame(10293, $orderID);
  }

  function testCanFetchOneRecordWithSpecifiedFields() {
    $details = $this->sparrow
      ->where('OrderDetailID', 123)
      ->select(array('OrderDetailID', 'OrderID'))
      ->one();

    $expected = array('OrderDetailID' => 123, 'OrderID' => 10293);

    self::assertSame($expected, $details);
  }

  function testCanDeleteOneRecord() {
    $this->sparrow->sql(file_get_contents(__DIR__ . '/UsersTable.sql'))->execute();

    $userID = hash('sha256', rand());
    $data = array('UserID' => $userID, 'UserName' => 'Franyer');

    $this->sparrow->from('Users')->insert($data)->execute();

    $userInserted = $this->sparrow
      ->from('Users')
      ->where('UserID', $userID)
      ->one();

    self::assertSame($data, $userInserted);

    $this->sparrow
      ->reset()
      ->from('Users')
      ->delete(array('UserID' => $userID))
      ->execute();

    $lastSQL = $this->sparrow->sql();
    $expected = "DELETE FROM Users WHERE UserID='$userID'";

    self::assertSame($lastSQL, $expected);

    $mustBeEmpty = $this->sparrow->where('UserID', $userID)->one();

    self::assertSame(array(), $mustBeEmpty);
  }

  function testCanExecuteCustomQueries() {
    $categories = $this->sparrow->sql('SELECT * FROM Categories')->many();

    $expected = array(
      'CategoryID' => 1,
      'CategoryName' => 'Beverages',
      'Description' => 'Soft drinks, coffees, teas, beers, and ales'
    );

    self::assertCount(8, $categories);
    self::assertSame($expected, $categories[0]);

    $category = $this->sparrow
      ->sql('SELECT * FROM Categories WHERE CategoryID = 1')
      ->one();

    self::assertSame($expected, $category);

    $userID = hash('sha256', rand());
    $data = array('UserID' => $userID, 'UserName' => 'Franyer');

    $this->sparrow
      ->from('Users')
      ->insert($data)
      ->execute();

    $this->sparrow
      ->sql("UPDATE Users SET UserName = 'Adrián' WHERE UserID = '$userID'")
      ->execute();

    $userUpdated = $this->sparrow->sql("SELECT UserName FROM Users WHERE UserID = '$userID'")->one();

    self::assertSame('Adrián', $userUpdated['UserName']);

    $this->sparrow->sql("DELETE FROM Users WHERE UserID = '$userID'")->execute();
  }

  function testCanEscapeSpecialCharacters() {
    $name = "O'Dell";

    $result = sprintf(
      'SELECT * FROM user WHERE name = %s',
      $this->sparrow->quote($name)
    );

    $expected = "SELECT * FROM user WHERE name = 'O\'Dell'";

    self::assertSame($expected, $result);
  }

  function testCanFillQueryProperties() {
    $sql = 'SELECT * FROM Categories';

    $this->sparrow->sql($sql)->many();
    $this->sparrow->sql('CREATE TABLE test (id PRIMARY KEY AUTOINCREMENT)');

    self::assertSame($sql, $this->sparrow->last_query);
    self::assertSame(8, $this->sparrow->num_rows);
  }
}
