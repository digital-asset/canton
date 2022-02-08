// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

object StackTraceUtil {

  def formatStackTrace(filter: Thread => Boolean = _ => true): String = {
    import scala.jdk.CollectionConverters._
    Thread.getAllStackTraces.asScala.toMap
      .filter { case (tr, _) =>
        filter(tr)
      }
      .map { case (k, v) =>
        s"  ${k.toString} is-daemon=${k.isDaemon} state=${k.getState.toString}" + "\n    " + v
          .map(_.toString)
          .mkString("\n    ")
      }
      .mkString("\n\n")
  }

  def caller(offset: Int = 1): String = {
    val stack = Thread.currentThread().getStackTrace
    if (stack.lengthCompare(offset) > 0) {
      val cal = stack(offset)
      s"${cal.getFileName}:${cal.getLineNumber}"
    } else "unknown"
  }

}
