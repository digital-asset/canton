// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util.signalling

import cats.Functor
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

/** Component to signal to a subscriber identified via type `K` that a notification has arrived for
  * this subscriber. Notifications carry a signal of type `S`.
  *
  * Later signals may overwrite earlier signals that have not yet been delivered. For example, if
  * two notifications with signals `s1` and `s2` happen in this order for a subscriber, then the
  * subscriber may see only `s2`, but never `s1`, e.g., when the signal `s2` arrives before `s1` has
  * been delivered to the subscriber.
  */
trait EventSignaller[K, S] extends AutoCloseable {
  def notify(notification: Notification[K], signal: S): Unit
  def readSignals(key: K, prettyKey: String)(implicit
      traceContext: TraceContext
  ): Source[NotificationSignal[S], NotUsed]
}

final case class NotificationSignal[+S](signal: S) {
  def map[A](f: S => A): NotificationSignal[A] = NotificationSignal(f(signal))
  def traverse[F[_], A](f: S => F[A])(implicit F: Functor[F]): F[NotificationSignal[A]] =
    F.map(f(signal))(NotificationSignal(_))
}
object NotificationSignal {
  val unit: NotificationSignal[Unit] = NotificationSignal(())
}
