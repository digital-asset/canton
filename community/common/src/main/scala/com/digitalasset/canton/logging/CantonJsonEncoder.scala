// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging

import net.logstash.logback.encoder.LogstashEncoder;

/** canton specific json log encoding
  *
  * equivalent of:
  * <encoder class="net.logstash.logback.encoder.LogstashEncoder">
  *      <excludeMdcKeyName>err-context</excludeMdcKeyName>
  * </encoder>
  */
class CantonJsonEncoder extends LogstashEncoder {
  addExcludeMdcKeyName("err-context")
}
