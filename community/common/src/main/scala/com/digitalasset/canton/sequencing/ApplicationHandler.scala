// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.Monoid
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.ExecutionContext

/** An application handler processes boxed envelopes and returns a [[HandlerResult]] */
trait ApplicationHandler[-Box[+_], -Env] extends (BoxedEnvelope[Box, Env] => HandlerResult) {

  /** Human-readable name of the application handler for logging and debugging */
  def name: String

  /** Called by the [[com.digitalasset.canton.sequencing.client.SequencerClient]] before the start of a subscription. */
  def resubscriptionStartsAt(start: ResubscriptionStart)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** Replaces the application handler's processing with `f` and
    * leaves the [[resubscriptionStartsAt]] logic and the name the same.
    */
  def replace[Box2[+_], Env2](
      f: BoxedEnvelope[Box2, Env2] => HandlerResult
  ): ApplicationHandler[Box2, Env2] = new ApplicationHandler[Box2, Env2] {

    override def name: String = ApplicationHandler.this.name

    override def resubscriptionStartsAt(start: ResubscriptionStart)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Unit] =
      ApplicationHandler.this.resubscriptionStartsAt(start)

    override def apply(boxedEnvelope: BoxedEnvelope[Box2, Env2]): HandlerResult =
      f(boxedEnvelope)
  }

  /** Run the `other` ApplicationHandler after `this`. */
  def combineWith[Box2[+X] <: Box[X], Env2 <: Env](other: ApplicationHandler[Box2, Env2])(implicit
      ec: ExecutionContext
  ): ApplicationHandler[Box2, Env2] = new ApplicationHandler[Box2, Env2] {

    override def name: String =
      s"${ApplicationHandler.this.name}+${other.name}"

    override def resubscriptionStartsAt(
        start: ResubscriptionStart
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
      for {
        _ <- ApplicationHandler.this.resubscriptionStartsAt(start)
        _ <- other.resubscriptionStartsAt(start)
      } yield ()

    override def apply(boxedEnvelope: BoxedEnvelope[Box2, Env2]): HandlerResult = {
      for {
        r1 <- ApplicationHandler.this.apply(boxedEnvelope: BoxedEnvelope[Box, Env])
        r2 <- other.apply(boxedEnvelope)
      } yield Monoid[AsyncResult].combine(r1, r2)
    }
  }
}

object ApplicationHandler {

  /** Creates an application handler that runs `f` on the boxed envelopes
    * and ignores the [[ApplicationHandler.resubscriptionStartsAt]] notifications
    */
  def create[Box[+_], Env](name: String)(
      f: BoxedEnvelope[Box, Env] => HandlerResult
  ): ApplicationHandler[Box, Env] = {
    val handlerName = name
    new ApplicationHandler[Box, Env] {

      override val name: String = handlerName

      override def resubscriptionStartsAt(start: ResubscriptionStart)(implicit
          traceContext: TraceContext
      ): FutureUnlessShutdown[Unit] =
        FutureUnlessShutdown.unit

      override def apply(boxedEnvelope: BoxedEnvelope[Box, Env]): HandlerResult = f(boxedEnvelope)
    }
  }

  /** Application handler that does nothing and always succeeds */
  @VisibleForTesting
  def success[Box[+_], Env](name: String = "success"): ApplicationHandler[Box, Env] =
    ApplicationHandler.create(name)(_ => HandlerResult.done)
}

/** Information passed by the [[com.digitalasset.canton.sequencing.client.SequencerClient]]
  * to the [[ApplicationHandler]] where the resubscription (= processing of events) starts.
  * The [[ApplicationHandler]] can then initialize itself appropriately.
  */
sealed trait ResubscriptionStart extends Product with Serializable with PrettyPrinting

object ResubscriptionStart {

  /** The subscription is created for the first time.
    * The application handler has never been called with an event.
    */
  case object FreshSubscription extends ResubscriptionStart {
    override def pretty: Pretty[FreshSubscription] = prettyOfObject[FreshSubscription]
  }
  type FreshSubscription = FreshSubscription.type

  /** The first processed event is at some timestamp after the `cleanPrehead`.
    * All events up to `cleanPrehead` inclusive have previously been processed completely.
    * The application handler has never been called with an event with a higher timestamp.
    */
  final case class CleanHeadResubscriptionStart(cleanPrehead: CantonTimestamp)
      extends ResubscriptionStart {

    override def pretty: Pretty[CleanHeadResubscriptionStart] = prettyOfClass(
      param("clean prehead", _.cleanPrehead)
    )
  }

  /** The first processed event will be `firstReplayed`.
    *
    * @param cleanPreheadO The timestamp of the last event known to be clean.
    *                      If set, this may be before, at, or after `firstReplayed`.
    *                      If it is before `firstReplayed`,
    *                      then `firstReplayed` is the timestamp of the first event after `cleanPreheadO`.
    */
  final case class ReplayResubscriptionStart(
      firstReplayed: CantonTimestamp,
      cleanPreheadO: Option[CantonTimestamp],
  ) extends ResubscriptionStart {
    override def pretty: Pretty[ReplayResubscriptionStart] = prettyOfClass(
      param("first replayed", _.firstReplayed),
      paramIfDefined("clean prehead", _.cleanPreheadO),
    )
  }

}
