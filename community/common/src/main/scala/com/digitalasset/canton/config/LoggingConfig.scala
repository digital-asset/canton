// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

/** Detailed logging configurations
  *
  * This section allows to configure additional data such as transaction details to be logged to the standard logback system
  *
  * @param api Configuration settings for the ApiRequestLogger
  * @param eventDetails If set to true, we will log substantial details of internal messages being processed. To be disabled in production!
  */
final case class LoggingConfig(
    api: ApiLoggingConfig = ApiLoggingConfig(),
    eventDetails: Boolean = false,
)
