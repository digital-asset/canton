// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.RequireTypes.String36

trait InitConfigBase {
  val autoInit: Boolean
  val instanceIdHint: Option[String36]
}

/** Configuration for the node's init process
  *
  * @param autoInit if true, the node will automatically initialize itself.
  *                 In particular, it will create a new namespace, and initialize its member id and its keys for signing and encryption.
  *                 If false, the user has to manually perform these steps.
  * @param instanceIdHint if an instance id hint is provided it will be used during initialization to set the instance-id. otherwise a UUID will be used.
  */
case class InitConfig(autoInit: Boolean = true, instanceIdHint: Option[String36] = None)
    extends InitConfigBase
