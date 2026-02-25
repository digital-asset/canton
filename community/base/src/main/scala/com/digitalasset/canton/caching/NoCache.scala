// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.caching

import java.util.concurrent.ConcurrentHashMap
import scala.collection.concurrent
import scala.jdk.CollectionConverters.ConcurrentMapHasAsScala

final class NoCache[Key, Value] private[caching] extends ConcurrentCache[Key, Value] {
  override def put(key: Key, value: Value): Unit = ()

  override def putAll(mappings: Map[Key, Value]): Unit = ()

  override def getIfPresent(key: Key): Option[Value] = None

  override def getOrAcquire(key: Key, acquire: Key => Value): Value = acquire(key)

  override def invalidateAll(items: Iterable[Key]): Unit = ()

  override def invalidateAll(): Unit = ()

  override def updateViaMap(updater: concurrent.Map[Key, Value] => Unit): Unit = updater(
    new ConcurrentHashMap[Key, Value]().asScala
  )
}
