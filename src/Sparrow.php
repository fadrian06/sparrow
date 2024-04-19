<?php

use Sparrow\Factory;

/**
 * Sparrow: A simple database toolkit
 *
 * @copyright Copyright (c) 2011, Mike Cao <mike@mikecao.com>
 * @license   MIT, http://www.opensource.org/licenses/mit-license.php
 * @template T of class-string
 */
class Sparrow {
  /** @var string */
  protected $table = '';

  /** @var string */
  protected $where = '';

  /** @var string */
  protected $joins = '';

  /** @var string */
  protected $order = '';

  /** @var string */
  protected $groups = '';

  /** @var string */
  protected $having = '';

  /** @var string */
  protected $distinct = '';

  /** @var ?int */
  protected $limit = null;

  /** @var ?int */
  protected $offset = null;

  /** @var string */
  protected $sql = '';

  /** @var null | mysqli | SQLite3 | PDO | resource */
  protected $db = null;

  /** @var 'pdo' | 'mysqli' | 'mysql' | 'pgsql' | 'sqlite' | 'sqlite3' */
  protected $db_type;

  /** @var Memcache | Memcached | string | array<string, mixed> */
  protected $cache;

  /** @var 'memcached' | 'memcache' | 'xcache' | 'apc' | 'file' | 'array' */
  protected $cache_type;

  /**
   * @var array{
   *   total_time: int,
   *   num_queries: int,
   *   num_rows: int,
   *   num_changes: int,
   *   avg_query_time: float,
   *   queries: array<int, array{time: int, rows: int, changes: int}>
   * }
   */
  protected $stats = array(
    'total_time' => 0,
    'num_queries' => 0,
    'num_rows' => 0,
    'num_changes' => 0,
    'avg_query_time' => 0.0,
    'queries' => array()
  );

  /** @var float */
  protected $query_time;

  /** @var class-string<T> */
  protected $class = '';

  /** @var ?string */
  public $last_query = null;

  /** @var ?int */
  public $num_rows = null;

  /** @var ?int */
  public $insert_id = null;

  /** @var ?int */
  public $affected_rows = null;

  /** @var bool */
  public $is_cached = false;

  /** @var bool */
  public $stats_enabled = false;

  /** @var bool */
  public $show_sql = false;

  /** @var ?string */
  public $key_prefix = null;

  static function factory() {
    return new Factory;
  }

  //////////////////
  // Core Methods //
  //////////////////
  /**
   * Joins string tokens into a SQL statement
   *
   * @param string $sql SQL statement
   * @param string $input Input string to append
   * @return string New SQL statement
   */
  protected static function build($sql, $input) {
    return $input ? "$sql $input" : $sql;
  }

  /**
   * Parses a connection string into an object
   *
   * @param string $connection Connection string `driver://user:password@host:port/dbname` or `driver://dbname`
   * @return array{
   *   type: 'mysqli' | 'mysql' | 'pgsql' | 'sqlite' | 'sqlite3' | 'pdomysql' | 'pdopgsql' | 'pdosqlite',
   *   hostname: string | null,
   *   database: string | null,
   *   username: string | null,
   *   password: string | null,
   *   port: int | null
   * } Connection information
   * @throws Exception For invalid connection string
   */
  protected static function parseConnection($connection) {
    $connection = str_replace('\\', '/', $connection);
    $url = parse_url($connection);

    if (!$url) {
      throw new Exception('Invalid connection string.');
    }

    $config = array(
      'type' => isset($url['scheme']) ? $url['scheme'] : $url['path'],
      'hostname' => isset($url['host']) ? $url['host'] : null,
      'database' => isset($url['path']) ? substr($url['path'], 1) : null,
      'username' => isset($url['user']) ? $url['user'] : null,
      'password' => isset($url['pass']) ? $url['pass'] : null,
      'port' => isset($url['port']) ? (int) $url['port'] : null
    );

    static $dbTypes = array(
      'mysql',
      'mysqli',
      'sqlite',
      'sqlite3',
      'pgsql',
      'pdomysql',
      'pdosqlite',
      'pdopgsql'
    );

    if (!in_array($config['type'], $dbTypes)) {
      throw new Exception("Invalid type {$config['type']}.");
    }

    return $config;
  }

  /** Gets the query statistics */
  public function getStats() {
    $this->stats['total_time'] = 0;
    $this->stats['num_queries'] = 0;
    $this->stats['num_rows'] = 0;
    $this->stats['num_changes'] = 0;

    foreach ($this->stats['queries'] as $query) {
      $this->stats['total_time'] += $query['time'];
      $this->stats['num_queries'] += 1;
      $this->stats['num_rows'] += $query['rows'];
      $this->stats['num_changes'] += $query['changes'];
    }

    $numQueries = $this->stats['num_queries'] ? $this->stats['num_queries'] : 1;
    $this->stats['avg_query_time'] = $this->stats['total_time'] / $numQueries;

    return $this->stats;
  }

  /**
   * Checks whether the table property has been set
   *
   * @throws Exception If table is not defined
   */
  protected function checkTable() {
    if (!$this->table) {
      throw new Exception('Table is not defined.');
    }

    return $this;
  }

  /**
   * Resets class properties
   *
   * @return $this Self reference
   */
  public function reset() {
    $this->where = '';
    $this->joins = '';
    $this->order = '';
    $this->groups = '';
    $this->having = '';
    $this->distinct = '';
    $this->limit = '';
    $this->offset = '';
    $this->sql = '';

    return $this;
  }

  /////////////////////////
  // SQL Builder Methods //
  /////////////////////////
  /**
   * Parses a condition statement
   *
   * @param string | array<string, string> $field Database field
   * @param null | string | array<int, string> $value Condition value
   * @param string $join Joining word
   * @param bool $escape Escape values setting
   * @return string Condition as a string
   * @throws Exception For invalid where condition
   */
  protected function parseCondition($field, $value = null, $join = '', $escape = true) {
    if (is_string($field)) {
      if ($value === '') {
        return "$join " . trim($field);
      }

      $operator = '=';

      if (strpos($field, ' ') !== false) {
        list($field, $operator) = explode(' ', $field);
      }

      switch ($operator) {
        case '%':
          $condition = ' LIKE ';
          break;

        case '!%':
          $condition = ' NOT LIKE ';
          break;

        case '@':
          $condition = ' IN ';
          break;

        case '!@':
          $condition = ' NOT IN ';
          break;

        default:
          $condition = $operator;
      }

      if (!$join) {
        $join = $field[0] === '|' ? ' OR' : ' AND';
      }

      if (is_array($value)) {
        if (strpos($operator, '@') === false) {
          $condition = ' IN ';
        }

        $value = '(' . implode(',', array_map(array($this, 'quote'), $value)) . ')';
      } elseif ($escape && !is_numeric($value)) {
        $value = $this->quote($value);
      }

      $field = str_replace('|', '', $field);

      return "$join {$field}{$condition}{$value}";
    }

    if (is_array($field)) {
      $condition = '';

      foreach ($field as $fieldName => $fieldValue) {
        $condition .= $this->parseCondition($fieldName, $fieldValue, $join, $escape);
        $join = '';
      }

      return $condition;
    }

    throw new Exception('Invalid where condition.');
  }

  /**
   * Sets the table
   *
   * @param string $table Table name
   * @param bool $reset Reset class properties
   * @return $this Self reference
   */
  public function from($table, $reset = true) {
    $this->table = $table;

    if ($reset) {
      $this->reset();
    }

    return $this;
  }

  /**
   * Adds a table join
   *
   * @param string $table Table to join to
   * @param array<string, string> $fields Fields to join on
   * @param 'INNER' | 'LEFT OUTER' | 'RIGHT OUTER' | 'FULL OUTER' $type Type of join
   * @return $this Self reference
   * @throws Exception For invalid join type
   */
  public function join($table, array $fields, $type = 'INNER') {
    static $joins = array(
      'INNER',
      'LEFT OUTER',
      'RIGHT OUTER',
      'FULL OUTER'
    );

    if (!in_array($type, $joins, true)) {
      throw new Exception('Invalid join type.');
    }

    $condition = $this->parseCondition($fields, null, ' ON', false);
    $this->joins .= " $type JOIN $table{$condition}";

    return $this;
  }

  /**
   * Adds a left table join
   *
   * @param string $table Table to join to
   * @param array<string, string> $fields Fields to join on
   * @return $this Self reference
   */
  public function leftJoin($table, array $fields) {
    return $this->join($table, $fields, 'LEFT OUTER');
  }

  /**
   * Adds a right table join
   *
   * @param string $table Table to join to
   * @param array<string, string> $fields Fields to join on
   * @return $this Self reference
   */
  public function rightJoin($table, array $fields) {
    return $this->join($table, $fields, 'RIGHT OUTER');
  }

  /**
   * Adds a full table join
   *
   * @param string $table Table to join to
   * @param array<string, string> $fields Fields to join on
   * @return $this Self reference
   */
  public function fullJoin($table, array $fields) {
    return $this->join($table, $fields, 'FULL OUTER');
  }

  /**
   * Adds where conditions
   *
   * @param string | array<string, string> $field A field name or an array of fields and values.
   * @param null | string | array<int, string> $value A field value to compare to
   * @return $this Self reference
   */
  public function where($field, $value = null) {
    $join = !$this->where ? 'WHERE' : '';
    $this->where .= $this->parseCondition($field, $value, $join);

    return $this;
  }

  /**
   * Adds fields to order by
   *
   * @param string | array<int, string> $field Field name
   * @param 'ASC' | 'DESC' $direction Sort direction
   * @return $this Self reference
   * @throws Exception If direction is invalid
   */
  public function orderBy($field, $direction = 'ASC') {
    static $directions = array('ASC', 'DESC');

    if (!in_array($direction, $directions)) {
      throw new Exception('Invalid direction.');
    }

    $join = !$this->order ? 'ORDER BY' : ',';

    if (is_array($field)) {
      foreach ($field as $key => $value) {
        $field[$key] = "$value $direction";
      }
    } else {
      $field .= " $direction";
    }

    $fields = is_array($field) ? implode(', ', $field) : $field;
    $this->order .= "$join $fields";

    return $this;
  }

  /**
   * Adds an ascending sort for a field
   *
   * @param string | array<int, string> $field Field name
   * @return $this Self reference
   */
  public function sortAsc($field) {
    return $this->orderBy($field, 'ASC');
  }

  /**
   * Adds an descending sort for a field
   *
   * @param string | array<int, string> $field Field name
   * @return $this Self reference
   */
  public function sortDesc($field) {
    return $this->orderBy($field, 'DESC');
  }

  /**
   * Adds fields to group by
   *
   * @param string | array<int, string> $field Field name or array of field names
   * @return $this Self reference
   */
  public function groupBy($field) {
    $join = !$this->order ? 'GROUP BY' : ',';
    $fields = is_array($field) ? implode(',', $field) : $field;

    $this->groups .= "$join $fields";

    return $this;
  }

  /**
   * Adds having conditions
   *
   * @param string | array<string, string> $field A field name or an array of fields and values.
   * @param null | string | array<int, string> $value A field value to compare to
   * @return $this Self reference
   */
  public function having($field, $value = null) {
    $join = !$this->having ? 'HAVING' : '';
    $this->having .= $this->parseCondition($field, $value, $join);

    return $this;
  }

  /**
   * Adds a limit to the query.
   *
   * @param ?int $limit Number of rows to limit
   * @param ?int $offset Number of rows to offset
   * @return $this Self reference
   */
  public function limit($limit = null, $offset = null) {
    if ($limit !== null) {
      $this->limit = "LIMIT $limit";
    }

    if ($offset !== null) {
      $this->offset($offset);
    }

    return $this;
  }

  /**
   * Adds an offset to the query
   *
   * @param ?int $offset Number of rows to offset
   * @param ?int $limit Number of rows to limit
   * @return $this Self reference
   */
  public function offset($offset = null, $limit = null) {
    if ($offset !== null) {
      $this->offset = "OFFSET $offset";
    }

    if ($limit !== null) {
      $this->limit($limit);
    }

    return $this;
  }

  /**
   * Sets the distinct keyword for a query
   *
   * @return $this Self reference
   */
  public function distinct() {
    if (!$this->distinct) {
      $this->distinct = 'DISTINCT';
    }

    return $this;
  }

  /**
   * Sets a between where clause
   *
   * @param string $field Database field
   * @param int | string $value1 First value
   * @param int | string $value2 Second value
   * @return $this Self reference
   */
  public function between($field, $value1, $value2) {
    return $this->where(sprintf(
      '%s BETWEEN %s AND %s',
      $field,
      $this->quote($value1),
      $this->quote($value2)
    ));
  }

  /**
   * Builds a select query
   *
   * @param array | string | '*' $fields Array of field names to select
   * @param ?int $limit Limit condition
   * @param ?int $offset Offset condition
   * @return $this Self reference
   * @throws Exception If table is not defined
   */
  public function select($fields = '*', $limit = null, $offset = null) {
    $this->checkTable();

    $fields = (is_array($fields)) ? implode(',', $fields) : $fields;
    $this->limit($limit, $offset);

    $this->sql(array(
      'SELECT',
      $this->distinct,
      $fields,
      'FROM',
      $this->table,
      $this->joins,
      $this->where,
      $this->groups,
      $this->having,
      $this->order,
      $this->limit,
      $this->offset
    ));

    return $this;
  }

  /**
   * Builds an insert query
   *
   * @param array<string, int | float | string | bool> $data Array of key and values to insert
   * @return $this Self reference
   * @throws Exception If table is not defined
   */
  public function insert(array $data) {
    $this->checkTable();

    if (!$data) {
      return $this;
    }

    $keys = implode(',', array_keys($data));

    $values = implode(',', array_values(
      array_map(
        array($this, 'quote'),
        $data
      )
    ));

    $this->sql(array(
      'INSERT INTO',
      $this->table,
      "($keys)",
      'VALUES',
      "($values)"
    ));

    return $this;
  }

  /**
   * Builds an update query
   *
   * @param string | array $data Array of keys and values, or string literal
   * @return $this Self reference
   * @throws Exception If table is not defined
   */
  public function update($data) {
    $this->checkTable();

    if (!$data) {
      return $this;
    }

    $values = array();

    if (is_array($data)) {
      foreach ($data as $key => $value) {
        $values[] = (is_numeric($key)) ? $value : "$key={$this->quote($value)}";
      }
    } else {
      $values[] = $data;
    }

    $this->sql(array(
      'UPDATE',
      $this->table,
      'SET',
      implode(',', $values),
      $this->where
    ));

    return $this;
  }

  /**
   * Builds a delete query
   *
   * @param array<string, int | float | string | bool> $where Where conditions
   * @return $this Self reference
   * @throws If table is not defined
   */
  public function delete(array $where = array()) {
    $this->checkTable();

    if ($where) {
      $this->where($where);
    }

    return $this->sql(array(
      'DELETE FROM',
      $this->table,
      $this->where
    ));
  }

  /**
   * Gets or sets the SQL statement
   *
   * @param null | string | array<int, string> $sql SQL statement
   * @return ($sql is null ? string : $this) SQL statement
   */
  public function sql($sql = null) {
    if ($sql !== null) {
      $this->sql = trim(
        is_array($sql)
          ? array_reduce($sql, array($this, 'build'))
          : $sql
      );

      return $this;
    }

    return $this->sql;
  }

  /////////////////////////////
  // Database Access Methods //
  /////////////////////////////
  /**
   * Sets the database connection
   *
   * @param string | array{
   *   type: string,
   *   hostname: string | null,
   *   database: string | null,
   *   username: string | null,
   *   password: string | null,
   *   port: string | null
   * } | object | resource $db Database connection string, array or object
   * @throws Exception For connection error
   */
  public function setDb($db) {
    $this->db = null;

    if (is_string($db)) { // Connection string
      return $this->setDb($this->parseConnection($db));
    }

    if (is_array($db)) { // Connection information
      $quotes = array('"', "'");

      if (
        (
          in_array($db['hostname'][0], $quotes)
          && in_array($db['database'][-1], $quotes)
        ) || strlen($db['hostname']) <= 2
      ) {
        $db['hostname'] .= ":/{$db['database']}";
        $db['hostname'] = str_replace($quotes[0], '', $db['hostname']);
        $db['hostname'] = str_replace($quotes[1], '', $db['hostname']);
      }

      switch ($db['type']) {
        case 'mysqli':
        case 'mysql':
          $this->db = new mysqli(
            $db['hostname'],
            $db['username'],
            $db['password'],
            $db['database']
          );

          if ($this->db->connect_error) {
            throw new Exception("Connection error: $this->db->connect_error");
          }

          break;

        case 'pgsql':
          $str = sprintf(
            'host=%s dbname=%s user=%s password=%s',
            $db['hostname'],
            $db['database'],
            $db['username'],
            $db['password']
          );

          $this->db = pg_connect($str);

          break;

        case 'sqlite':
        case 'sqlite3':
          $this->db = new SQLite3($db['hostname'], SQLITE3_OPEN_CREATE | SQLITE3_OPEN_READWRITE, '');

          break;

        case 'pdomysql':
          $dsn = sprintf(
            'mysql:host=%s;port=%d;dbname=%s',
            $db['hostname'],
            isset($db['port']) ? $db['port'] : 3306,
            $db['database']
          );

          $this->db = new PDO($dsn, $db['username'], $db['password']);
          $db['type'] = 'pdo';

          break;

        case 'pdopgsql':
          $dsn = sprintf(
            'pgsql:host=%s;port=%d;dbname=%s;user=%s;password=%s',
            $db['hostname'],
            isset($db['port']) ? $db['port'] : 5432,
            $db['database'],
            $db['username'],
            $db['password']
          );

          $this->db = new PDO($dsn);
          $db['type'] = 'pdo';

          break;

        case 'pdosqlite':
          $this->db = new PDO("sqlite:{$db['hostname']}");
          $db['type'] = 'pdo';

          break;
      }

      if (!$this->db) {
        throw new Exception('Undefined database.');
      }

      $this->db_type = $db['type'];

      return;
    }

    $type = $this->getDbType($db);

    static $db_types = array(
      'pdo',
      'mysqli',
      'mysql',
      'pgsql',
      'sqlite',
      'sqlite3'
    );

    if (!in_array($type, $db_types)) {
      throw new Exception('Invalid database type.');
    }

    $this->db = $db;
    $this->db_type = $type;
  }

  /**
   * Gets the database connection
   *
   * @return object | resource Database connection
   */
  public function getDb() {
    return $this->db;
  }

  /**
   * Gets the database type.
   *
   * @param null | object | resource $db Database object or resource
   * @return null | 'mysql' | 'sqlite' | 'pgsql' | class-string Database type
   */
  public function getDbType($db) {
    if (is_object($db)) {
      return strtolower(get_class($db));
    }

    if (is_resource($db)) {
      switch (get_resource_type($db)) {
        case 'mysql link':
          return 'mysql';

        case 'sqlite database':
          return 'sqlite';

        case 'pgsql link':
          return 'pgsql';
      }
    }

    return null;
  }

  /**
   * Executes a sql statement
   *
   * @param string $key Cache key
   * @param int $expire Expiration time in seconds
   * @return mixed Query results object
   * @throws Exception When database is not defined
   */
  public function execute($key = '', $expire = 0) {
    if (!$this->db) {
      throw new Exception('Database is not defined.');
    }

    if ($key !== '') {
      $result = $this->fetch($key);

      if ($this->is_cached) {
        return $result;
      }
    }

    $result = null;

    $this->is_cached = false;
    $this->num_rows = 0;
    $this->affected_rows = 0;
    $this->insert_id = -1;
    $this->last_query = $this->sql;

    if ($this->stats_enabled) {
      if (!$this->stats) {
        $this->stats = array('queries' => array());
      }

      $this->query_time = microtime(true);
    }

    if ($this->sql) {
      $error = null;

      switch ($this->db_type) {
        case 'pdo':
          try {
            $result = $this->db->prepare($this->sql);

            if (!$result) {
              $error = $this->db->errorInfo();
            } else {
              $result->execute();

              $this->num_rows = $result->rowCount();
              $this->affected_rows = $result->rowCount();
              $this->insert_id = $this->db->lastInsertId();
            }
          } catch (PDOException $ex) {
            $error = $ex->getMessage();
          }

          break;

        case 'mysql':
        case 'mysqli':
          $result = $this->db->query($this->sql);

          if (!$result) {
            $error = $this->db->error;
          } else {
            if (is_object($result)) {
              $this->num_rows = $result->num_rows;
            } else {
              $this->affected_rows = $this->db->affected_rows;
            }
            $this->insert_id = $this->db->insert_id;
          }

          break;

        case 'pgsql':
          $result = pg_query($this->db, $this->sql);

          if (!$result) {
            $error = pg_last_error($this->db);
          } else {
            $this->num_rows = pg_num_rows($result);
            $this->affected_rows = pg_affected_rows($result);
            $this->insert_id = pg_last_oid($result);
          }

          break;

        case 'sqlite':
        case 'sqlite3':
          $result = $this->db->query($this->sql);

          if ($result === false) {
            $error = $this->db->lastErrorMsg();
          } else {
            $this->num_rows = 0;
            $this->affected_rows = ($result) ? $this->db->changes() : 0;
            $this->insert_id = $this->db->lastInsertRowId();
          }

          break;
      }

      if ($error !== null) {
        if ($this->show_sql) {
          $error .= "\nSQL: $this->sql";
        }

        throw new Exception("Database error: $error");
      }
    }

    if ($this->stats_enabled) {
      $time = microtime(true) - $this->query_time;

      $this->stats['queries'][] = array(
        'query' => $this->sql,
        'time' => $time,
        'rows' => $this->num_rows,
        'changes' => $this->affected_rows
      );
    }

    return $result;
  }

  /**
   * Fetch multiple rows from a select query
   *
   * @param string $key Cache key
   * @param int $expire Expiration time in seconds
   * @return array<int, array<string, null | int | float | string | bool>> Rows
   */
  public function many($key = '', $expire = 0) {
    if (!$this->sql) {
      $this->select();
    }

    $data = array();

    $result = $this->execute($key, $expire);

    if ($this->is_cached) {
      $data = $result;

      if ($this->stats_enabled) {
        $this->stats['cached']["{$this->key_prefix}{$key}"] = $this->sql;
      }
    } else {
      switch ($this->db_type) {
        case 'pdo':
          $data = $result->fetchAll(PDO::FETCH_ASSOC);
          $this->num_rows = sizeof($data);

          break;

        case 'mysql':
        case 'mysqli':
          if (function_exists('mysqli_fetch_all')) {
            $data = $result->fetch_all(MYSQLI_ASSOC);
          } else {
            while ($row = $result->fetch_assoc()) {
              $data[] = $row;
            }
          }
          $result->close();

          break;

        case 'pgsql':
          $data = pg_fetch_all($result);
          pg_free_result($result);

          break;

        case 'sqlite':
        case 'sqlite3':
          if ($result) {
            while ($row = $result->fetchArray(SQLITE3_ASSOC)) {
              $data[] = $row;
            }

            $result->finalize();
            $this->num_rows = sizeof($data);
          }

          break;
      }
    }

    if (!$this->is_cached && $key !== '') {
      $this->store($key, $data, $expire);
    }

    return $data;
  }

  /**
   * Fetch a single row from a select query.
   *
   * @param string $key Cache key
   * @param int $expire Expiration time in seconds
   * @return array<string, int | float | string | bool | null> Row
   */
  public function one($key = '', $expire = 0) {
    if (!$this->sql) {
      $this->limit(1)->select();
    }

    $data = $this->many($key, $expire);
    $row = $data ? $data[0] : array();

    return $row;
  }

  /**
   * Fetch a value from a field
   *
   * @param string $name Database field name
   * @param string $key Cache key
   * @param int $expire Expiration time in seconds
   * @return null | int | float | string | bool Row value
   */
  public function value($name, $key = '', $expire = 0) {
    $row = $this->one($key, $expire);
    $value = $row ? $row[$name] : null;

    return $value;
  }

  /**
   * Gets the min value for a specified field
   *
   * @param string $field Field name
   * @param int $expire Expiration time in seconds
   * @param string $key Cache key
   * @return int | string | null
   */
  public function min($field, $key = '', $expire = 0) {
    return $this->select("MIN($field) min_value")->value('min_value', $key, $expire);
  }

  /**
   * Gets the max value for a specified field
   *
   * @param string $field Field name
   * @param int $expire Expiration time in seconds
   * @param string $key Cache key
   * @return int | string | null
   */
  public function max($field, $key = '', $expire = 0) {
    return $this->select("MAX($field) max_value")->value('max_value', $key, $expire);
  }

  /**
   * Gets the sum value for a specified field
   *
   * @param string $field Field name
   * @param int $expire Expiration time in seconds
   * @param string $key Cache key
   * @return int
   */
  public function sum($field, $key = '', $expire = 0) {
    return $this->select("SUM($field) sum_value")->value('sum_value', $key, $expire);
  }

  /**
   * Gets the average value for a specified field
   *
   * @param string $field Field name
   * @param int $expire Expiration time in seconds
   * @param string $key Cache key
   * @return float
   */
  public function avg($field, $key = '', $expire = 0) {
    return $this->select("AVG($field) avg_value")->value('avg_value', $key, $expire);
  }

  /**
   * Gets a count of records for a table
   *
   * @param string | '*' $field Field name
   * @param string $key Cache key
   * @param int $expire Expiration time in seconds
   * @return int
   */
  public function count($field = '*', $key = '', $expire = 0) {
    return $this->select("COUNT($field) num_rows")->value('num_rows', $key, $expire);
  }

  /**
   * Wraps quotes around a string and escapes the content for a string parameter
   *
   * @param mixed $value mixed value
   * @return string Quoted value
   */
  public function quote($value) {
    if ($value === null) {
      return 'NULL';
    }

    if (is_string($value)) {
      if ($this->db !== null) {
        switch ($this->db_type) {
          case 'pdo':
            return $this->db->quote($value);

          case 'mysql':
          case 'mysqli':
            return "'" . $this->db->real_escape_string($value) . "'";

          case 'pgsql':
            return "'" . pg_escape_string($this->db, $value) . "'";

          case 'sqlite':
          case 'sqlite3':
            return "'" . addslashes($value) . "'";
        }
      }

      $value = str_replace(
        array('\\', "\0", "\n", "\r", "'", '"', "\x1a"),
        array('\\\\', '\\0', '\\n', '\\r', "\\'", '\\"', '\\Z'),
        $value
      );

      return "'$value'";
    }

    return $value;
  }

  ///////////////////
  // Cache Methods //
  ///////////////////
  /**
   * Sets the cache connection
   *
   * @param string | object $cache Cache connection string or object
   * @throws Exception For invalid cache type
   */
  public function setCache($cache) {
    $this->cache = null;

    if (is_string($cache)) { // Connection string
      if ($cache[0] === '.' || $cache[0] === '/') {
        $this->cache = $cache;
        $this->cache_type = 'file';
      } else {
        $this->setCache($this->parseConnection($cache));
      }
    } elseif (is_array($cache)) { // Connection information
      switch ($cache['type']) {
        case 'memcache':
          $this->cache = new Memcache;
          $this->cache->connect(
            $cache['hostname'],
            $cache['port']
          );
          break;

        case 'memcached':
          $this->cache = new Memcached;
          $this->cache->addServer(
            $cache['hostname'],
            $cache['port']
          );
          break;

        default:
          $this->cache = $cache['type'];
      }

      $this->cache_type = $cache['type'];
    } elseif (is_object($cache)) { // Cache object
      $type = strtolower(get_class($cache));
      static $cache_types = array('memcached', 'memcache', 'xcache');

      if (!in_array($type, $cache_types)) {
        throw new Exception('Invalid cache type.');
      }

      $this->cache = $cache;
      $this->cache_type = $type;
    }
  }

  /**
   * Gets the cache instance
   *
   * @return object Cache instance
   */
  public function getCache() {
    return $this->cache;
  }

  /**
   * Stores a value in the cache
   *
   * @param string $key Cache key
   * @param mixed $value Value to store
   * @param int $expire Expiration time in seconds
   */
  public function store($key, $value, $expire = 0) {
    $key = $this->key_prefix . $key;

    switch ($this->cache_type) {
      case 'memcached':
        $this->cache->set($key, $value, $expire);
        break;

      case 'memcache':
        $this->cache->set($key, $value, 0, $expire);
        break;

      case 'apc':
        apc_store($key, $value, $expire);
        break;

      case 'xcache':
        xcache_set($key, $value, $expire);
        break;

      case 'file':
        $file = "$this->cache/" . md5($key);

        $data = array(
          'value' => $value,
          'expire' => ($expire > 0) ? (time() + $expire) : 0
        );

        file_put_contents($file, serialize($data));
        break;

      default:
        $this->cache[$key] = $value;
    }
  }

  /**
   * Fetches a value from the cache
   *
   * @param string $key Cache key
   * @return mixed Cached value
   */
  public function fetch($key) {
    $key = $this->key_prefix . $key;

    switch ($this->cache_type) {
      case 'memcached':
        $value = $this->cache->get($key);
        $this->is_cached = ($this->cache->getResultCode() === Memcached::RES_SUCCESS);

        return $value;

      case 'memcache':
        $value = $this->cache->get($key);
        $this->is_cached = ($value !== false);

        return $value;

      case 'apc':
        return apc_fetch($key, $this->is_cached);

      case 'xcache':
        $this->is_cached = xcache_isset($key);
        return xcache_get($key);

      case 'file':
        $file = $this->cache . '/' . md5($key);

        if ($this->is_cached = file_exists($file)) {
          $data = unserialize(file_get_contents($file));
          if ($data['expire'] === 0 || time() < $data['expire']) {
            return $data['value'];
          } else {
            $this->is_cached = false;
          }
        }

        break;

      default:
        return $this->cache[$key];
    }

    return null;
  }

  /**
   * Clear a value from the cache.
   *
   * @param string $key Cache key
   * @return mixed
   */
  public function clear($key) {
    $key = $this->key_prefix . $key;

    switch ($this->cache_type) {
      case 'memcached':
        return $this->cache->delete($key);

      case 'memcache':
        return $this->cache->delete($key);

      case 'apc':
        return apc_delete($key);

      case 'xcache':
        return xcache_unset($key);

      case 'file':
        $file = $this->cache . '/' . md5($key);
        if (file_exists($file)) {
          return unlink($file);
        }
        return false;

      default:
        if (isset($this->cache[$key])) {
          unset($this->cache[$key]);
          return true;
        }
        return false;
    }
  }

  /**
   * Flushes out the cache.
   */
  public function flush() {
    switch ($this->cache_type) {
      case 'memcached':
        $this->cache->flush();
        break;

      case 'memcache':
        $this->cache->flush();
        break;

      case 'apc':
        apc_clear_cache();
        break;

        // TODO implement xcache flush
        // case 'xcache':
        //   xcache_clear_cache();
        //   break;

      case 'file':
        if ($handle = opendir($this->cache)) {
          while (false !== ($file = readdir($handle))) {
            if ($file !== '.' && $file !== '..') {
              unlink($this->cache . '/' . $file);
            }
          }
          closedir($handle);
        }
        break;

      default:
        $this->cache = array();
        break;
    }
  }

  ////////////////////
  // Object Methods //
  ////////////////////
  /**
   * Sets the class
   *
   * @param string|object $class Class name or instance
   * @return $this Self reference
   */
  public function using($class) {
    if (is_string($class)) {
      $this->class = $class;
    } elseif (is_object($class)) {
      $this->class = get_class($class);
    }

    $this->reset();

    return $this;
  }

  /**
   * Loads properties for an object.
   *
   * @param object $object Class instance
   * @param array $data Property data
   * @return static Populated object
   */
  public function load($object, array $data) {
    foreach ($data as $key => $value) {
      if (property_exists($object, $key)) {
        $object->$key = $value;
      }
    }

    return $object;
  }

  /**
   * Finds and populates an object.
   *
   * @param int|string|array Search value
   * @param string $key Cache key
   * @return static Populated object
   */
  public function find($value = null, $key = null) {
    if (!$this->class) {
      throw new Exception('Class is not defined.');
    }

    $properties = $this->getProperties();

    $this->from($properties->table, false);

    if ($value !== null) {
      if (is_int($value) && property_exists($properties, 'id_field')) {
        $this->where($properties->id_field, $value);
      } else if (is_string($value) && property_exists($properties, 'name_field')) {
        $this->where($properties->name_field, $value);
      } else if (is_array($value)) {
        $this->where($value);
      }
    }

    if (!$this->sql) {
      $this->select();
    }

    $data = $this->many($key);
    $objects = array();

    foreach ($data as $row) {
      $objects[] = $this->load(new $this->class, $row);
    }

    return (count($objects) === 1) ? $objects[0] : $objects;
  }

  /**
   * Saves an object to the database.
   *
   * @template T of object
   * @param T $object Class instance
   * @param array $fields Select database fields to save
   */
  public function save($object, array $fields = array()) {
    $this->using($object);

    $properties = $this->getProperties();

    $this->from($properties->table);

    $data = get_object_vars($object);
    $id = $object->{$properties->id_field};

    unset($data[$properties->id_field]);

    if ($id === null) {
      $this->insert($data)->execute();

      $object->{$properties->id_field} = $this->insert_id;
    } else {
      if ($fields) {
        $keys = array_flip($fields);
        $data = array_intersect_key($data, $keys);
      }

      $this->where($properties->id_field, $id)
        ->update($data)
        ->execute();
    }

    return $this->class;
  }

  /**
   * Removes an object from the database.
   *
   * @param object $object Class instance
   */
  public function remove($object) {
    $this->using($object);

    $properties = $this->getProperties();

    $this->from($properties->table);

    $id = $object->{$properties->id_field};

    if ($id !== null) {
      $this->where($properties->id_field, $id)
        ->delete()
        ->execute();
    }
  }

  /**
   * Gets class properties.
   *
   * @return object Class properties
   */
  public function getProperties() {
    static $properties = array();

    if (!$this->class) return array();

    if (!isset($properties[$this->class])) {
      static $defaults = array(
        'table' => null,
        'id_field' => null,
        'name_field' => null
      );

      $reflection = new ReflectionClass($this->class);
      $config = $reflection->getStaticProperties();

      $properties[$this->class] = (object)array_merge($defaults, $config);
    }

    return $properties[$this->class];
  }
}
