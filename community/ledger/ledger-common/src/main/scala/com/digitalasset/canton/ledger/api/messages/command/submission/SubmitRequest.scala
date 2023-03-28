// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.command.submission

import com.digitalasset.canton.ledger.api.domain

final case class SubmitRequest(commands: domain.Commands)
