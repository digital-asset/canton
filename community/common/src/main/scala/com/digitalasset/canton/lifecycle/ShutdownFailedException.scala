// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.util.ShowUtil._

class ShutdownFailedException(instances: NonEmpty[Seq[String]])
    extends RuntimeException(show"Unable to close ${instances.map(_.singleQuoted)}.")
