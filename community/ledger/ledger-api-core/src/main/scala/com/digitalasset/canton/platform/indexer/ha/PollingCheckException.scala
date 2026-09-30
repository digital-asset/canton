// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.ha

class PollingCheckException(cause: Throwable)
    extends RuntimeException(s"check failed, killSwitch aborted", cause)
