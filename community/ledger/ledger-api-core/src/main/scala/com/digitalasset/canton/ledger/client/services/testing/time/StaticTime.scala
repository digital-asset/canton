// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.testing.time

import com.daml.ledger.api.v2.testing.time_service.TimeServiceGrpc.{TimeService, TimeServiceStub}
import com.daml.ledger.api.v2.testing.time_service.{GetTimeRequest, SetTimeRequest}
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.ledger.api.util.TimestampConversion.*
import com.digitalasset.canton.ledger.api.util.{TimeProvider, TimestampConversion}
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.logging.NamedLoggerFactory
import org.apache.pekko.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink, Source}
import org.apache.pekko.stream.{ClosedShape, KillSwitches, Materializer, UniqueKillSwitch}

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

class StaticTime(
    timeService: TimeService,
    clock: AtomicReference[Instant],
    killSwitch: UniqueKillSwitch,
) extends TimeProvider
    with AutoCloseable {

  def getCurrentTime: Instant = clock.get

  def timeRequest(instant: Instant): SetTimeRequest =
    SetTimeRequest(
      Some(TimestampConversion.fromInstant(getCurrentTime)),
      Some(TimestampConversion.fromInstant(instant)),
    )

  def setTime(instant: Instant)(implicit ec: ExecutionContext): Future[Unit] = {
    timeService.setTime(timeRequest(instant)).map { _ =>
      val _ = StaticTime.advanceClock(clock, instant)
    }
  }

  override def close(): Unit = killSwitch.shutdown()
}

object StaticTime {
  def advanceClock(clock: AtomicReference[Instant], instant: Instant): Instant = {
    clock.updateAndGet {
      case current if instant isAfter current => instant
      case current => current
    }
  }

  def updatedVia(
      timeService: TimeServiceStub,
      loggerFactory: NamedLoggerFactory,
      token: Option[String] = None,
      updateInterval: FiniteDuration = 1.second,
  )(implicit m: Materializer): Future[StaticTime] = {
    val clockRef = new AtomicReference[Instant](Instant.EPOCH)
    val killSwitchExternal = KillSwitches.single[Instant]
    val sinkExternal = Sink.head[Instant]

    val logger = loggerFactory.getLogger(getClass)
    val directEc = DirectExecutionContext(logger)

    RunnableGraph
      .fromGraph {
        GraphDSL.createGraph(killSwitchExternal, sinkExternal) {
          case (killSwitch, futureOfFirstElem) =>
            // We serve this in a future which completes when the first element has passed through.
            // Thus we make sure that the object we serve already received time data from the ledger.
            futureOfFirstElem.map(_ => new StaticTime(timeService, clockRef, killSwitch))(
              directEc
            )
        } { implicit b => (killSwitch, sinkHead) =>
          import GraphDSL.Implicits.*

          val instantSource = b.add(
            Source
              .tick(updateInterval, updateInterval, ())
              .mapAsync(1)(_ => LedgerClient.stub(timeService, token).getTime(GetTimeRequest()))
              .map(r => toInstant(r.getCurrentTime))
          )

          val updateClock = b.add(Flow[Instant].map { i =>
            advanceClock(clockRef, i).discard
            i
          })

          val broadcastTimes = b.add(Broadcast[Instant](2))

          val ignore = b.add(Sink.ignore)

          // format: OFF
          instantSource ~> killSwitch ~> updateClock ~> broadcastTimes.in
                                                        broadcastTimes.out(0) ~> sinkHead
                                                        broadcastTimes.out(1) ~> ignore
          // format: ON

          ClosedShape
        }
      }
      .run()
  }

}
