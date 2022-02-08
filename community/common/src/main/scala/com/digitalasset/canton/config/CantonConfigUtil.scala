// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import pureconfig.generic.FieldCoproductHint

object CantonConfigUtil {

  /** By default pureconfig will expect our H2 config to use the value `h-2` for type.
    * This just changes this expectation to being lower cased so `h2` will work.
    */
  def lowerCaseStorageConfigType[SC <: StorageConfig]: FieldCoproductHint[SC] =
    new FieldCoproductHint[SC]("type") {
      // Keep the following case classes of ADTs as lowercase and not kebab-case
      override def fieldValue(name: String): String = name.toLowerCase
    }

}
