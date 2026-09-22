// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.interpretation

sealed abstract class ExecutionMode extends Product with Serializable
object ExecutionMode {
  // run using Update Machine
  case object UpdateMachine extends ExecutionMode
  // run using Cmd Machine and Transaction Conductor
  case object Conductor extends ExecutionMode
}
