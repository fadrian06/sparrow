<?php

namespace Sparrow;

use Sparrow;

final class Factory {
  /** @var ?Sparrow */
  private $sparrow = null;

  function getResult() {
    if (!$this->sparrow) {
      $this->sparrow = new Sparrow;
    }

    return $this->sparrow;
  }

  function enableCache() {
    $this->getResult()->is_cached = true;

    return $this;
  }

  function disableCache() {
    $this->getResult()->is_cached = false;

    return $this;
  }

  /** @param ?string $prefix */
  function setCacheKeyPrefix($prefix) {
    $this->getResult()->key_prefix = $prefix;

    return $this;
  }

  function showSqlOnErrors($show = true) {
    $this->getResult()->show_sql = $show;

    return $this;
  }

  function enableStats() {
    $this->getResult()->stats_enabled = true;

    return $this;
  }

  function disableStats() {
    $this->getResult()->stats_enabled = false;

    return $this;
  }
}
