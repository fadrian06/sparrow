<?php

namespace Tests;

use mysqli;
use PDO;
use PHPUnit\Framework\TestCase;
use Sparrow;

final class ConnectionExamplesTest extends TestCase {
  /** @var Sparrow */
  private $sparrow;
  private $host;
  private $user;
  private $password;
  private $dbName;
  private $port;

  function setUp(): void {
    $this->sparrow = new Sparrow;
    $this->sparrow->from('user');
    $this->host = $_ENV['MYSQL_HOST'];
    $this->user = $_ENV['MYSQL_USER'];
    $this->password = $_ENV['MYSQL_PASSWORD'];
    $this->dbName = $_ENV['MYSQL_DBNAME'];
    $this->port = $_ENV['MYSQL_PORT'];
  }

  function testCanConnectToMysqlDatabaseFromConnectionString() {
    $this->sparrow->setDb("mysql://$this->user:$this->password@$this->host:$this->port/$this->dbName");

    self::assertInstanceOf('mysqli', $this->sparrow->getDb());
  }

  function testCanConnectToMysqlDatabaseFromConnectionArray() {
    $this->sparrow->setDb(array(
      'type' => 'mysql',
      'hostname' => $this->host,
      'database' => $this->dbName,
      'username' => $this->user,
      'password' => $this->password,
      'port' => $this->port
    ));

    self::assertInstanceOf('mysqli', $this->sparrow->getDb());
  }

  function testCanConnectToMysqlDatabaseFromAConnectionObject() {
    $mysql = new mysqli(
      $this->host,
      $this->user,
      $this->password,
      $this->dbName,
      $this->port
    );

    $this->sparrow->setDb($mysql);

    self::assertInstanceOf('mysqli', $this->sparrow->getDb());
  }

  function testCanConnectToMysqlDatabaseFromAPdoConnectionString() {
    $this->sparrow->setDb("pdomysql://$this->user:$this->password@$this->host:$this->password/$this->dbName");

    self::assertInstanceOf('PDO', $this->sparrow->getDb());
  }

  function testCanConnectToMysqlDatabaseFromAPdoObject() {
    $pdo = new PDO(
      "mysql:host=$this->host;port=$this->port;dbname=$this->dbName",
      $this->user,
      $this->password
    );

    $this->sparrow->setDb($pdo);

    self::assertInstanceOf('PDO', $this->sparrow->getDb());
  }

  function testCanConnectToSqliteDatabaseFromConnectionString() {
    $this->sparrow->setDb('sqlite://' . __DIR__ . '/Northwind.db');

    self::assertInstanceOf('SQLite3', $this->sparrow->getDb());
  }

  function testCanConnectToSqliteDatabaseFromPdoConnectionString() {
    $this->sparrow->setDb('pdosqlite://' . __DIR__ . '/Northwind.db');

    self::assertInstanceOf('PDO', $this->sparrow->getDb());
  }
}
