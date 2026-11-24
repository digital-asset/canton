// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.interning

import com.digitalasset.canton.discard.Implicits.DiscardOps

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

class CoveringCacheBasedStringInterningDomain[T](
    override val to: String => T,
    override val from: T => String,
    override val prefix: String,
) extends StringInterningDomainInt[T] {
  private val toExternal: TrieMap[Int, T] = new TrieMap()
  private val toInternal: TrieMap[T, Int] = new TrieMap()

  override val initializationRequired: Boolean = true

  override def add(id: Int, t: T): Unit = {
    toExternal.put(id, t).discard
    toInternal.put(t, id).discard
  }

  override def resetTo(id: Int): Unit =
    toExternal.iterator
      .filter { case (key, _) =>
        key > id
      }
      .toVector
      .foreach { case (removeId, removeString) =>
        toExternal.remove(removeId).discard
        toInternal.remove(removeString).discard
      }

  override def unsafe: StringInterningAccessor[String] = new StringInterningAccessor[String] {
    override def internalize(t: String): Int = toInternal(to(t))
    override def tryInternalize(t: String): Option[Int] = toInternal.get(to(t))
    override def externalize(id: Int): String = from(toExternal(id))
    override def tryExternalize(id: Int): Option[String] = toExternal.get(id).map(from)
  }

  override def internalize(t: T): Int = toInternal(t)
  override def tryInternalize(t: T): Option[Int] = toInternal.get(t)
  override def externalize(id: Int): T = toExternal(id)
  override def tryExternalize(id: Int): Option[T] = toExternal.get(id)
}

class CoveringCacheBasedStringInternalizingBuilderDomain[T](
    domain: StringInterningDomainInt[T],
    newRawStrings: mutable.HashSet[String],
    readyForLookup: () => Boolean,
) extends StringInternalizingBuilderDomainInt[T] {
  override def add(t: T): Unit = {
    assert(!readyForLookup())
    domain.tryInternalize(t) match {
      case Some(_) =>
      // as included in the covering cache nothing to do

      case None =>
        val rawString = domain.toPrefixed(t)
        if (!newRawStrings.contains(rawString)) {
          newRawStrings.add(rawString).discard
        }
    }
  }

  override def resolvedRawString(id: Int, rawString: String): Unit = {
    assert(!readyForLookup())
    domain.addPrefixed(id, rawString)
  }

  override def lookup(t: T): Int = {
    assert(readyForLookup())
    domain.internalize(t)
  }

  override def lookupIfExists(t: T): Option[Int] = domain.tryInternalize(t)
}

class CoveringCacheBasedStringExternalizingBuilderDomain[T](
    domain: StringInterningDomainInt[T]
) extends StringExternalizingBuilderDomainInt[T] {
  override def resolvedRawString(id: Int, rawString: String): Unit = ()

  override def lookup(id: Int): T = domain.externalize(id)

  override def add(t: Int): Unit = ()
}
