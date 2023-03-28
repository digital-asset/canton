// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.runner.common

import com.daml.resources.ProgramResource.SuppressedStartupException

private[common] final class ConfigParseException
    extends RuntimeException
    with SuppressedStartupException
