// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.scalactic

import com.digitalasset.canton.logging.pretty.CanPrettyPrint

class PrettyPrettifier extends DefaultPrettifier {
  override protected def prettify(o: Any, processed: collection.Set[Any]): String =
    if (processed.contains(o))
      throw new StackOverflowError("Cyclic relationship detected, let's fail early!")
    else
      o match {
        case canPrettyPrint: CanPrettyPrint =>
          canPrettyPrint.toString
        case _ => super.prettify(o, processed)
      }
}

object PrettyPrettifier {
  val prettifier = new PrettyPrettifier
}
