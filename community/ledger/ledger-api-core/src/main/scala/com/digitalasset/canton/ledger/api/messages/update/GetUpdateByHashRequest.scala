// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.update

import com.digitalasset.canton.ledger.api.UpdateFormat
import com.google.protobuf.ByteString

final case class GetUpdateByHashRequest(
    hash: ByteString,
    updateFormat: UpdateFormat,
)
