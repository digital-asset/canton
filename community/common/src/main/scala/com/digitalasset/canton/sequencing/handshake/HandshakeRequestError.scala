// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.handshake

/** Failed to obtain a handshake response from the server.
  * @param message loggable message for why the handshake failed
  * @param retryable does the transport suggest retrying the handshake
  */
case class HandshakeRequestError(message: String, retryable: Boolean = false)
