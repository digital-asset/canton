// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.acs.commitment.util

import com.digitalasset.canton.BaseTest
import jdk.jfr.{Category, Description, Event as JFREvent, Label}

import scala.annotation.meta.field
import scala.util.{Failure, Try}

/** Represents a Java Flight Record (JFR) event where only the phase name parameter can be set
  *
  * @param phase
  *   the phase name that is recorded in jfr file and that can be filtered using the 'Event Browser'
  *   of JDK Mission Control (JMC)
  */
@Description("Represents a logical phase in a test execution")
@Category(Array("Tests", "Phases"))
class JFRTestPhaseEvent(
    @(Label @field)("Phase Name")
    @(Description @field)("Name of the executed phase")
    val phase: String
) extends JFREvent

/** Used to facilitate the instrumentation of Java Flight Record (JFR) phases in integration tests
  *
  * Note: for using Java Flight Recorder you need to run your test with the JVM param
  * `-XX:StartFlightRecording`` that has some parameters for instance file name and profile name.
  * More details <a
  * href="https://docs.oracle.com/javacomponents/jmc-5-4/jfr-runtime-guide/run.htm">here</a>
  */
trait JFRTestPhase {
  this: BaseTest =>

  def jfrPhaseTry[T](phase: String)(code: => T): Try[T] = {
    val e = new JFRTestPhaseEvent(phase)

    if (e.isEnabled) e.begin()
    val result = Try(code).recoverWith { case ex: Throwable =>
      logger.warn(s"Failure in run inside jfr phase: $ex")
      Failure[T](ex)
    }

    e.commit()
    result
  }

  def jfrPhase[T](phase: String)(code: => T): T =
    jfrPhaseTry(phase)(code).fold(ex => throw ex, identity)
}
