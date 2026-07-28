// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util.signalling

import com.digitalasset.nonempty.NonEmpty

/** Who gets notified that an event has happened */
sealed trait Notification[+K] extends Product with Serializable {
  def isBroadcast: Boolean
}

object Notification {

  case object NoTarget extends Notification[Nothing] {
    override def isBroadcast: Boolean = false
  }
  def noTarget[K]: Notification[K] = NoTarget

  final case class Keys[K](keys: NonEmpty[Set[K]]) extends Notification[K] {
    override def isBroadcast: Boolean = false
    override def toString: String = s"Keys(${keys.mkString(",")})"
  }

  case object All extends Notification[Nothing] {
    override def isBroadcast: Boolean = true
  }
  def all[K]: Notification[K] = All

  def union[K](first: Notification[K], second: Notification[K]): Notification[K] =
    (first, second) match {
      case (Keys(firstKeys), Keys(secondKeys)) => Keys(firstKeys ++ secondKeys)
      case (All, _) | (_, All) => All
      case (Notification.NoTarget, those) => those
      case (these, Notification.NoTarget) => these
    }
}
