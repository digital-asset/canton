// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.grpc

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.pattern.pipe
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.testkit.{TestKit, TestProbe}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.collection.immutable
import scala.concurrent.ExecutionContext

final class DropRepeatedSpec
    extends TestKit(ActorSystem(classOf[DropRepeatedSpec].getSimpleName))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  private[this] implicit val materializer: Materializer = Materializer(system)
  private[this] implicit val executionContext: ExecutionContext = materializer.executionContext

  override def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)

  "DropRepeated" should {
    "drop repeated elements" in {
      val probe = TestProbe()
      val input = immutable.Seq(1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5)

      val _ = Source(input)
        .via(DropRepeated())
        .runWith(Sink.seq)
        .pipeTo(probe.ref)
        .failed
        .foreach(fail(_))

      probe.expectMsg(Vector(1, 2, 3, 4, 5))
    }

    "does not drop duplicate elements that are not repeated" in {
      val probe = TestProbe()
      val input = immutable.Seq(1, 1, 2, 2, 1, 1, 2, 2)

      val _ = Source(input)
        .via(DropRepeated())
        .runWith(Sink.seq)
        .pipeTo(probe.ref)
        .failed
        .foreach(fail(_))

      probe.expectMsg(Vector(1, 2, 1, 2))
    }
  }
}
