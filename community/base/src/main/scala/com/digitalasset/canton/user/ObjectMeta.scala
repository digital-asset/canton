// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.user

final case class ObjectMeta(
    resourceVersionO: Option[Long],
    annotations: Map[String, String],
)

object ObjectMeta {
  def empty: ObjectMeta = ObjectMeta(
    resourceVersionO = None,
    annotations = Map.empty,
  )
}

final case class ObjectMetaUpdate(
    resourceVersionO: Option[Long],
    annotationsUpdateO: Option[Map[String, String]],
)

object ObjectMetaUpdate {
  def empty: ObjectMetaUpdate = ObjectMetaUpdate(
    resourceVersionO = None,
    annotationsUpdateO = None,
  )
}
