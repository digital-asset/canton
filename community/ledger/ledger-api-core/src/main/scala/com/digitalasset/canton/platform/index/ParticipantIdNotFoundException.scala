// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.index

class ParticipantIdNotFoundException
    extends RuntimeException(
      "No participant ID found in the index database"
    )
