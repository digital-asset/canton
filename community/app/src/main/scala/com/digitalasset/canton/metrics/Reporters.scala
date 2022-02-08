// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import io.prometheus.client.exporter.HTTPServer
import com.codahale.metrics.Reporter

object Reporters {

  class Prometheus(hostname: String, port: Int) extends Reporter {
    val server: HTTPServer = new HTTPServer.Builder().withHostname(hostname).withPort(port).build();
    override def close(): Unit = server.close()
  }

}
