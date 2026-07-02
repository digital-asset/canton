// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.nonempty.NonEmpty

class ShutdownFailedException(instances: NonEmpty[Seq[String]])
    extends RuntimeException(show"Unable to close ${instances.map(_.singleQuoted)}.")
