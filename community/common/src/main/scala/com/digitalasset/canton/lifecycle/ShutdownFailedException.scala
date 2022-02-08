// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import cats.data.NonEmptyList
import com.digitalasset.canton.util.ShowUtil._

class ShutdownFailedException(instances: NonEmptyList[String])
    extends RuntimeException(
      s"Unable to close ${instances.map(_.singleQuoted).toList.mkString(", ")}."
    ) {}
