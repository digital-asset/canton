// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.console.{
  ConsoleEnvironment,
  ConsoleEnvironmentTestHelpers,
  LocalParticipantReference,
}

/** Aliases used by our typical single domain and multi domain tests.
  * If a test attempts to use an aliases against an environment without
  * that node configured it will immediately throw.
  */
trait CommonTestAliases[CE <: ConsoleEnvironment] {
  this: ConsoleEnvironmentTestHelpers[CE] =>
  lazy val participant1: LocalParticipantReference = p("participant1")
  lazy val participant2: LocalParticipantReference = p("participant2")
  lazy val participant3: LocalParticipantReference = p("participant3")
  lazy val participant4: LocalParticipantReference = p("participant4")
  lazy val da: CE#DomainLocalRef = d("da")
  lazy val acme: CE#DomainLocalRef = d("acme")
}
