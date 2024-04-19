<?php

namespace Tests;

use PHPUnit\Framework\TestCase;
use Sparrow;

final class FactoryTest extends TestCase {
  function testCanBuildCustomSparrowInstanceWithDefaultValues() {
    $sparrow = Sparrow::factory()->getResult();

    self::assertNull($sparrow->affected_rows);
    self::assertNull($sparrow->insert_id);
    self::assertFalse($sparrow->is_cached);
    self::assertNull($sparrow->key_prefix);
    self::assertNull($sparrow->last_query);
    self::assertNull($sparrow->num_rows);
    self::assertFalse($sparrow->show_sql);
    self::assertFalse($sparrow->stats_enabled);
  }

  function testCanBuildCustomSparrowInstanceWithCustomValues() {
    $sparrow = Sparrow::factory()
      ->enableCache()
      ->enableStats()
      ->setCacheKeyPrefix('MY_PREFIX')
      ->showSqlOnErrors()
      ->getResult();

    self::assertTrue($sparrow->is_cached);
    self::assertSame('MY_PREFIX', $sparrow->key_prefix);
    self::assertTrue($sparrow->show_sql);
    self::assertTrue($sparrow->stats_enabled);
  }
}
