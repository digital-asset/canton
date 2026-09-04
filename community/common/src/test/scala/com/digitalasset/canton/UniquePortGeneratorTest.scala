// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import org.scalatest.flatspec.AnyFlatSpec

import java.net.{InetSocketAddress, ServerSocket}
import scala.util.{Failure, Success, Try, Using}

class UniquePortGeneratorTest extends AnyFlatSpec with BaseTest {

  private lazy val generator = UniquePortGenerator.generatorForUniquePortGeneratorTest

  behavior of "UniquePortGenerator"

  it should "hand out ports that can actually be bound" in {
    val port = generator.next

    // Must not throw: the generator has just probed this very port.
    Using(new ServerSocket()) { socket =>
      socket.setReuseAddress(true)
      socket.bind(new InetSocketAddress(port.unwrap))
    }.success.value
  }

  it should "skip a port that is already bound" in {
    val allocated = generator.next

    // As this test uses a dedicated UniquePortGenerator, there is no interference from other allocations.
    // Therefore, this returns the counter's next value.
    def succ(port: Int): Int =
      if (port == generator.portRangeEnd) generator.portRangeStart else port + 1

    // Occupying this port forces the generator to probe an unavailable port and move on.
    val occupied = succ(allocated.unwrap)

    Using.resource(new ServerSocket()) { socket =>
      socket.setReuseAddress(true)
      Try(socket.bind(new InetSocketAddress(occupied))) match {
        case Failure(_) =>
          cancel(s"Could not occupy port $occupied to set up the test")
        case Success(_) =>
          generator.next.unwrap shouldBe succ(occupied)
      }
    }
  }
}
