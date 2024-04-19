<?php

namespace Tests;

use PHPUnit\Framework\TestCase;
use Sparrow;

final class PropertiesTest extends TestCase {
  function testDefaultValuesDoesNotThrowAnyError() {
    $sparrow = new Sparrow;

    self::assertNull($sparrow->affected_rows);
    self::assertNull($sparrow->insert_id);
    self::assertFalse($sparrow->is_cached);
    self::assertNull($sparrow->key_prefix);
    self::assertNull($sparrow->last_query);
    self::assertNull($sparrow->num_rows);
    self::assertFalse($sparrow->show_sql);
    self::assertFalse($sparrow->stats_enabled);
  }
}
