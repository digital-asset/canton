// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

package object engine {

  type Enricher = refinement.Enricher
  val Enricher: refinement.Enricher.type = refinement.Enricher

}
