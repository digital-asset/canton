// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging

import ch.qos.logback.classic.{Level, Logger}

/** Set of utility methods to access logging information of this node */
object NodeLoggingUtil {

  /** Set the global canton logger level
    *
    * This method works based on the logback.xml and the CantonFilterEvaluator
    */
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def setLevel(loggerName: String = "com.digitalasset.canton", level: String): Unit = {

    // TODO(#18445): setting the log level for a particular logger doesn't work due to this one here
    //    the fix would include to set it explicitly for the logger here as a property
    //    and then to check in the CantonFilterEvaluator whether there exists are more detailed
    //    configuration
    if (Seq("com.digitalasset.canton", "com.daml").exists(loggerName.startsWith))
      System.setProperty("LOG_LEVEL_CANTON", level)

    val logger = getLogger(loggerName)
    if (level == "null")
      logger.setLevel(null)
    else
      logger.setLevel(Level.valueOf(level))
  }

  def getLogger(loggerName: String): Logger = {
    import org.slf4j.LoggerFactory
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    val logger: Logger = LoggerFactory.getLogger(loggerName).asInstanceOf[Logger]
    logger
  }
}
