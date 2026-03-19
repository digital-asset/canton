// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.h2

import com.digitalasset.canton.platform.store.backend.common.Field
import com.digitalasset.canton.platform.store.interning.StringInterning

import java.io.{ByteArrayInputStream, InputStream}

private[h2] final case class H2Bytea[From](extract: StringInterning => From => Array[Byte])
    extends Field[From, Array[Byte], InputStream] {
  override def convert: Array[Byte] => InputStream = new ByteArrayInputStream(_)
}

private[h2] final case class H2ByteaOptional[From](
    extract: StringInterning => From => Option[Array[Byte]]
) extends Field[From, Option[Array[Byte]], InputStream] {
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  override def convert: Option[Array[Byte]] => InputStream =
    _.map(new ByteArrayInputStream(_)).orNull
}
